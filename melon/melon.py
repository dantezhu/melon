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
from . import autoreload
from . import constants


class Melon(RoutesMixin, AppEventsMixin):

    connection_factory_class = ConnectionFactory
    request_class = Request

    box_class = None
    stream_checker = None

    debug = False
    got_first_request = False
    backlog = constants.SERVER_BACKLOG

    parent_input = None
    parent_output = None
    conn_dict = None

    server = None
    blueprints = None

    worker = None

    def __init__(self, box_class, input_queue_maxsize=None, output_queue_maxsize=None):
        RoutesMixin.__init__(self)
        AppEventsMixin.__init__(self)

        self.box_class = box_class
        self.stream_checker = self.box_class().check

        self.blueprints = list()
        # 0 不代表无穷大，看代码是 SEM_VALUE_MAX = 32767L
        self.parent_input = Queue(input_queue_maxsize or 0)
        self.parent_output = Queue(output_queue_maxsize or 0)
        self.conn_dict = weakref.WeakValueDictionary()
        self.worker = Worker(self)

    def register_blueprint(self, blueprint):
        blueprint.register_to_app(self)

    def run(self, host=None, port=None, debug=None, use_reloader=None, workers=None, handle_signals=None):
        self._validate_cmds()

        if host is None:
            host = constants.SERVER_HOST
        if port is None:
            port = constants.SERVER_PORT
        if debug is not None:
            self.debug = debug
        use_reloader = use_reloader if use_reloader is not None else self.debug
        workers = workers if workers is not None else 1
        handle_signals = handle_signals if handle_signals is not None else not use_reloader

        def run_wrapper():
            logger.info('Running server on %s:%s, debug: %s, use_reloader: %s',
                        host, port, self.debug, use_reloader)

            self._spawn_poll_worker_result_thread()
            self._spawn_fork_workers(workers)
            if handle_signals:
                self._handle_parent_proc_signals()

            reactor.listenTCP(port, self.connection_factory_class(self),
                              backlog=self.backlog, interface=host)

            try:
                reactor.run(installSignalHandlers=False)
            except KeyboardInterrupt:
                pass
            except:
                logger.error('exc occur.', exc_info=True)

        if use_reloader:
            autoreload.main(run_wrapper)
        else:
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

    def _spawn_poll_worker_result_thread(self):
        """
        启动获取worker数据的线程
        """
        thread = Thread(target=self._poll_worker_result)
        thread.daemon = True
        thread.start()

    def _spawn_fork_workers(self, workers):
        """
        通过线程启动多个worker
        """
        thread = Thread(target=self._fork_workers, args=(workers,))
        thread.daemon = True
        thread.start()

    def _fork_workers(self, workers):
        def start_worker_process():
            inner_p = Process(target=self.worker.run)
            inner_p.daemon = True
            inner_p.start()
            return inner_p

        p_list = []

        for it in xrange(0, workers):
            p = start_worker_process()
            p_list.append(p)

        while 1:
            for idx, p in enumerate(p_list):
                if not p.is_alive():
                    old_pid = p.pid
                    p = start_worker_process()
                    p_list[idx] = p

                    logger.error('process[%s] is dead. start new process[%s].', old_pid, p.pid)

            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break
            except:
                logger.error('exc occur.', exc_info=True)
                break

    def _poll_worker_result(self):
        """
        从队列里面获取worker的返回
        """
        while 1:
            try:
                msg = self.parent_input.get()
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
        def custom_signal_handler(signum, frame):
            """
            在centos6下，callFromThread(stop)无效，因为处理不够及时
            """
            try:
                reactor.stop()
            except:
                pass

        signal.signal(signal.SIGTERM, custom_signal_handler)
        signal.signal(signal.SIGINT, custom_signal_handler)
