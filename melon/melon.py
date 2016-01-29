# -*- coding: utf-8 -*-

import os
import copy
import weakref
import sys
import subprocess
import time
import signal
from collections import Counter
import Queue
# linux 默认就是epoll
from twisted.internet import reactor

from .log import logger
from .proxy import ClientConnectionFactory, WorkerConnectionFactory
from .worker import RoutesMixin, AppEventsMixin, Request
from . import constants


class Melon(RoutesMixin, AppEventsMixin):

    CONN_ID_MAX = 2 ** 63 - 1

    client_connection_factory_class = ClientConnectionFactory
    worker_connection_factory_class = WorkerConnectionFactory
    request_class = Request
    box_class = None
    blueprints = None

    group_conf = None
    group_router = None

    conn_id_counter = 0
    conn_dict = None

    host = None
    port = None
    debug = False
    backlog = constants.SERVER_BACKLOG

    # 是否有效(父进程中代表程序有效，子进程中代表worker是否有效)
    enable = True
    # 网络连接超时(秒)
    conn_timeout = constants.CONN_TIMEOUT
    # 处理job超时(秒). 超过后worker会自杀. None 代表永不超时
    job_timeout = None
    # 停止子进程超时(秒). 使用 TERM / USR1 进行停止时，如果超时未停止会发送KILL信号
    stop_timeout = None

    # 子进程列表
    processes = None
    msg_queue_dict = None

    def __init__(self, box_class, group_conf, group_router):
        """
        构造函数
        :param box_class: box类
        :param group_conf: 进程配置，格式如下:
            {
                $group_id: {
                    count: 10,
                }
            }
        :param group_router: 通过box路由group_id:
            def group_router(box):
                return group_id
        :return:
        """
        RoutesMixin.__init__(self)
        AppEventsMixin.__init__(self)

        self.box_class = box_class
        self.group_conf = group_conf
        self.group_router = group_router

        self.blueprints = list()
        self.msg_queue_dict = dict()
        self.conn_dict = weakref.WeakValueDictionary()

    def register_blueprint(self, blueprint):
        blueprint.register_to_app(self)

    def alloc_conn_id(self):
        """
        获取自增的连接ID
        :return:
        """

        # 使用longlong型
        if self.conn_id_counter >= self.CONN_ID_MAX:
            self.conn_id_counter = 0

        self.conn_id_counter += 1

        return self.conn_id_counter

    def run(self, host=None, port=None, debug=None):
        self._validate_cmds()

        self.host = host
        self.port = port

        if debug is not None:
            self.debug = debug

        # 只要没有这个环境变量，就是主进程
        if not os.getenv(constants.WORKER_GROUP_ENV_KEY):
            # 主进程
            logger.info('Running server on %s:%s, debug: %s',
                        host, port, self.debug)
            self._handle_parent_proc_signals()

            reactor.listenTCP(port, self.client_connection_factory_class(self),
                              backlog=self.backlog, interface=host)

            # 启动监听worker
            for group_id in self.group_conf:
                self.msg_queue_dict[group_id] = Queue.Queue()
                address = "worker_%s.sock"

                # 给内部worker通信用的
                reactor.listenUnix(address, self.worker_connection_factory_class(self, group_id))

            try:
                reactor.run(installSignalHandlers=False)
            except KeyboardInterrupt:
                pass
            except:
                logger.error('exc occur.', exc_info=True)
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

    def _fork_workers(self):
        def start_worker_process(group_id):
            # 要传入group_id
            worker_env = copy.deepcopy(os.environ)
            worker_env.update({
                constants.WORKER_GROUP_ENV_KEY: str(group_id)
            })
            args = [sys.executable] + sys.argv
            inner_p = subprocess.Popen(args, env=worker_env)
            inner_p.group_id = group_id
            return inner_p

        for group_id, group_info in self.group_conf.items():
            p = start_worker_process(group_id)
            self.processes.append(p)

        while 1:
            for idx, p in enumerate(self.processes):
                if p and p.poll() is not None:
                    group_id = p.group_id

                    # 说明退出了
                    self.processes[idx] = None

                    if self.enable:
                        # 如果还要继续服务
                        p = start_worker_process(group_id)
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

