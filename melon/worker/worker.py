# -*- coding: utf-8 -*-

import time
import os
import subprocess
import sys
import signal
import copy
from collections import Counter

from .request import Request
from .connection import Connection
from .mixins import RoutesMixin, AppEventsMixin
from ..log import logger
from .. import constants


class Worker(RoutesMixin, AppEventsMixin):
    connection_class = Connection
    request_class = Request

    box_class = None

    debug = False
    got_first_request = False
    blueprints = None
    # 是否有效(父进程中代表程序有效，子进程中代表worker是否有效)
    enable = True
    # 子进程列表
    processes = None
    # 最多回应一次
    rsp_once = True
    # 网络连接超时(秒)
    conn_timeout = constants.CONN_TIMEOUT
    # 处理job超时(秒). 超过后worker会自杀. None 代表永不超时
    job_timeout = None
    # 停止子进程超时(秒). 使用 TERM / USR1 进行停止时，如果超时未停止会发送KILL信号
    stop_timeout = None

    host = None
    port = None

    def __init__(self, box_class):
        RoutesMixin.__init__(self)
        AppEventsMixin.__init__(self)
        self.blueprints = list()
        self.processes = list()
        self.box_class = box_class

    def register_blueprint(self, blueprint):
        blueprint.register_to_app(self)

    def run(self, host=None, port=None, debug=None, workers=None):
        self._validate_cmds()

        self.host = host
        self.port = port

        if debug is not None:
            self.debug = debug

        workers = workers if workers is not None else 1

        if os.getenv(constants.WORKER_ENV_KEY) != 'true':
            # 主进程
            logger.info('Connect to server on %s:%s, debug: %s, workers: %s',
                        host, port, self.debug, workers)

            # 只能在主线程里面设置signals
            self._handle_parent_proc_signals()
            self._fork_workers(workers)
        else:
            # 子进程
            self._try_serve_forever()

    def _validate_cmds(self):
        """
        确保 cmd 没有重复
        :return:
        """

        cmd_list = list(self.rule_map.keys())

        for bp in self.blueprints:
            cmd_list.extend(bp.rule_map.keys())

        duplicate_cmds = (Counter(cmd_list) - Counter(set(cmd_list))).keys()

        assert not duplicate_cmds, 'duplicate cmds: %s' % duplicate_cmds

    def _before_worker_run(self):
        self.events.create_worker()
        for bp in self.blueprints:
            bp.events.create_app_worker()

    def _try_serve_forever(self):
        self._handle_child_proc_signals()

        self._before_worker_run()

        try:
            self._serve_forever()
        except KeyboardInterrupt:
            pass
        except:
            logger.error('exc occur.', exc_info=True)

    def _fork_workers(self, workers):
        worker_env = copy.deepcopy(os.environ)
        worker_env.update({
            constants.WORKER_ENV_KEY: 'true'
        })

        def start_worker_process():
            args = [sys.executable] + sys.argv
            inner_p = subprocess.Popen(args, env=worker_env)
            return inner_p

        for it in xrange(0, workers):
            p = start_worker_process()
            self.processes.append(p)

        while 1:
            for idx, p in enumerate(self.processes):
                if p and p.poll() is not None:
                    # 说明退出了
                    self.processes[idx] = None

                    if self.enable:
                        # 如果还要继续服务
                        p = start_worker_process()
                        self.processes[idx] = p

            if not filter(lambda x: x, self.processes):
                # 没活着的了
                break

            # 时间短点，退出的快一些
            time.sleep(0.1)

    def _handle_parent_proc_signals(self):
        def exit_handler(signum, frame):
            self.enable = False

            # 如果是终端直接CTRL-C，子进程自然会在父进程之后收到INT信号，不需要再写代码发送
            # 如果直接kill -INT $parent_pid，子进程不会自动收到INT
            # 所以这里可能会导致重复发送的问题，重复发送会导致一些子进程异常，所以在子进程内部有做重复处理判断。
            for p in self.processes:
                if p:
                    p.send_signal(signum)

            # https://docs.python.org/2/library/signal.html#signal.alarm
            if self.stop_timeout is not None:
                signal.alarm(self.stop_timeout)

        def final_kill_handler(signum, frame):
            if not self.enable:
                # 只有满足了not enable，才发送term命令
                for p in self.processes:
                    if p:
                        p.send_signal(signal.SIGKILL)

        def safe_stop_handler(signum, frame):
            """
            等所有子进程结束，父进程也退出
            """
            self.enable = False

            for p in self.processes:
                if p:
                    p.send_signal(signal.SIGTERM)

            if self.stop_timeout is not None:
                signal.alarm(self.stop_timeout)

        def safe_reload_handler(signum, frame):
            """
            让所有子进程重新加载
            """
            for p in self.processes:
                if p:
                    p.send_signal(signal.SIGHUP)

        # INT, QUIT为强制结束
        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGQUIT, exit_handler)
        # TERM为安全结束
        signal.signal(signal.SIGTERM, safe_stop_handler)
        # HUP为热更新
        signal.signal(signal.SIGHUP, safe_reload_handler)
        # 最终判决，KILL掉子进程
        signal.signal(signal.SIGALRM, final_kill_handler)

    def _handle_child_proc_signals(self):
        def exit_handler(signum, frame):
            # 防止重复处理KeyboardInterrupt，导致抛出异常
            if self.enable:
                self.enable = False
                raise KeyboardInterrupt

        def safe_stop_handler(signum, frame):
            self.enable = False

        # 强制结束，抛出异常终止程序进行
        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGQUIT, exit_handler)
        # 安全停止
        signal.signal(signal.SIGTERM, safe_stop_handler)
        signal.signal(signal.SIGHUP, safe_stop_handler)

    def _serve_forever(self):
        conn = self.connection_class(self, self.host, self.port, self.conn_timeout)
        conn.run()
