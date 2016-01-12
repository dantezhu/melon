# -*- coding: utf-8 -*-

import time
from multiprocessing import Queue, Process
import weakref
from threading import Thread
# linux 默认就是epoll
from twisted.internet import reactor
import signal
from collections import Counter

from .log import logger
from .connection import ConnectionFactory
from .worker import Worker
from .mixins import RoutesMixin, AppEventsMixin
from .request import Request
from . import constants


class Melon(RoutesMixin, AppEventsMixin):

    connection_factory_class = ConnectionFactory
    request_class = Request

    # 是否有效(父进程中代表程序有效，子进程中代表worker是否有效)
    enable = True

    # 停止子进程超时(秒). 使用 TERM / USR1 进行停止时，如果超时未停止会发送KILL信号
    stop_timeout = None

    box_class = None

    debug = False
    got_first_request = False
    backlog = constants.SERVER_BACKLOG

    group_conf = None
    group_router = None

    parent_input_dict = None
    parent_output_dict = None
    conn_dict = None

    # 子进程列表
    processes = None

    server = None
    blueprints = None

    def __init__(self, box_class, group_conf, group_router):
        """
        构造函数
        :param box_class: box类
        :param group_conf: 进程配置，格式如下:
            {
                $group_id: {
                    count: 10,
                    input_max_size: 1000,  # parent端的input
                    output_max_size: 1000, # parent端的output
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
        self.processes = list()
        # 0 不代表无穷大，看代码是 SEM_VALUE_MAX = 32767L
        self.parent_input_dict = dict()
        self.parent_output_dict = dict()
        self.conn_dict = weakref.WeakValueDictionary()
        self._init_groups()

    def register_blueprint(self, blueprint):
        blueprint.register_to_app(self)

    def run(self, host=None, port=None, debug=None, handle_signals=None):
        self._validate_cmds()

        if host is None:
            host = constants.SERVER_HOST
        if port is None:
            port = constants.SERVER_PORT
        if debug is not None:
            self.debug = debug
        handle_signals = handle_signals if handle_signals is not None else True

        def run_wrapper():
            logger.info('Running server on %s:%s, debug: %s',
                        host, port, self.debug)

            if handle_signals:
                self._handle_parent_proc_signals()

            self._spawn_poll_worker_result_thread()
            self._spawn_serve_forever(host, port)

            self._fork_workers()

        run_wrapper()

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

    def _init_groups(self):
        """
        初始化group数据
        :return:
        """
        for group_id, conf in self.group_conf.items():
            self.parent_input_dict[group_id] = Queue(conf.get('input_max_size', 0))
            self.parent_output_dict[group_id] = Queue(conf.get('output_max_size', 0))

    def _spawn_serve_forever(self, host, port):
        """
        启动网络
        :return:
        """

        def serve_forever(host, port):
            reactor.listenTCP(port, self.connection_factory_class(self),
                              backlog=self.backlog, interface=host)

            try:
                reactor.run(installSignalHandlers=False)
            except KeyboardInterrupt:
                pass
            except:
                logger.error('exc occur.', exc_info=True)

        thread = Thread(target=serve_forever, args=(host, port))
        thread.daemon = True
        thread.start()

    def _spawn_poll_worker_result_thread(self):
        """
        启动获取worker数据的线程
        """
        for group_id in self.group_conf:
            thread = Thread(target=self._poll_worker_result, args=(group_id,))
            thread.daemon = True
            thread.start()

    def _fork_workers(self):
        def start_worker_process(target):
            inner_p = Process(target=target)
            inner_p.daemon = True
            inner_p.start()
            # 用_popen，更方便
            return inner_p._popen

        self.processes = []

        for group_id, conf in self.group_conf.items():

            child_input = self.parent_output_dict[group_id]
            child_output = self.parent_input_dict[group_id]
            worker = Worker(self, group_id, child_input, child_output)

            for it in xrange(0, conf.get('count', 1)):
                p = start_worker_process(worker.run)
                self.processes.append([p, worker])

        while 1:
            for proc_info in self.processes:
                p = proc_info[0]
                worker = proc_info[1]

                if p and not p.is_alive():
                    # 先赋值为None
                    proc_info[0] = None

                    if self.enable:
                        p = start_worker_process(worker.run)
                        proc_info[0] = p

            if not filter(lambda x: x[0], self.processes):
                # 没活着的了
                break

            time.sleep(0.1)

    def _poll_worker_result(self, group_id):
        """
        从队列里面获取worker的返回
        """
        while 1:
            try:
                msg = self.parent_input_dict[group_id].get()
            except KeyboardInterrupt:
                break
            except:
                logger.error('exc occur.', exc_info=True)
                break

            # 参考 http://twistedsphinx.funsize.net/projects/core/howto/threading.html
            reactor.callFromThread(self._handle_worker_response, msg)

    def _handle_worker_response(self, msg):
        conn = self.conn_dict.get(msg.get('conn_id'))
        data = msg.get('data')

        if conn and conn.transport:
            try:
                if data:
                    conn.transport.write(data)
                else:
                    # data 为NULL代表关闭链接的意思
                    conn.transport.loseConnection()
            except:
                logger.error('exc occur. msg: %r', msg, exc_info=True)

    def _handle_parent_proc_signals(self):
        def exit_handler(signum, frame):
            self.enable = False

            # 如果是终端直接CTRL-C，子进程自然会在父进程之后收到INT信号，不需要再写代码发送
            # 如果直接kill -INT $parent_pid，子进程不会自动收到INT
            # 所以这里可能会导致重复发送的问题，重复发送会导致一些子进程异常，所以在子进程内部有做重复处理判断。
            for p, worker in self.processes:
                if p:
                    p.send_signal(signum)

            # https://docs.python.org/2/library/signal.html#signal.alarm
            if self.stop_timeout is not None:
                signal.alarm(self.stop_timeout)

        def final_kill_handler(signum, frame):
            if not self.enable:
                # 只有满足了not enable，才发送term命令
                for p, worker in self.processes:
                    if p:
                        p.send_signal(signal.SIGKILL)

        def safe_stop_handler(signum, frame):
            """
            等所有子进程结束，父进程也退出
            """
            self.enable = False

            for p, worker in self.processes:
                if p:
                    p.send_signal(signal.SIGTERM)

            if self.stop_timeout is not None:
                signal.alarm(self.stop_timeout)

        # INT, QUIT为强制结束
        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGQUIT, exit_handler)
        # TERM为安全结束
        signal.signal(signal.SIGTERM, safe_stop_handler)
        # 最终判决，KILL掉子进程
        signal.signal(signal.SIGALRM, final_kill_handler)

