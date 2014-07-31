# -*- coding: utf-8 -*-

import time
from multiprocessing import Queue, Process
import weakref
from threading import Thread
# linux 默认就是epoll
from twisted.internet import reactor

from .log import logger
from .connection import ConnectionFactory
from .worker import Worker
from .callbacks_mixin import RoutesMixin
from .request import Request
from . import autoreload


class Melon(RoutesMixin):

    parent_input = None
    parent_output = None
    conn_dict = None

    server = None
    blueprints = None
    debug = False

    def __init__(self, box_class, connection_factory_class=None, request_class=None,
                 input_queue_maxsize=None, output_queue_maxsize=None):
        super(Melon, self).__init__()
        self.box_class = box_class
        self.connection_factory_class = connection_factory_class or ConnectionFactory

        self.blueprints = list()
        self.parent_input = Queue(input_queue_maxsize)
        self.parent_output = Queue(output_queue_maxsize)
        self.conn_dict = weakref.WeakValueDictionary()
        self.request_class = request_class or Request

    def register_blueprint(self, blueprint):
        blueprint.register2app(self)

    def run(self, host, port, debug=None, use_reloader=None, workers=1, handle_signals=None):
        if debug is not None:
            self.debug = debug
        use_reloader = use_reloader if use_reloader is not None else self.debug
        handle_signals = handle_signals if handle_signals is not None else not use_reloader

        def run_wrapper():
            assert workers >= 1

            logger.info('Running server on %s:%s, debug: %s, use_reloader: %s',
                        host, port, self.debug, use_reloader)

            self._spawn_poll_worker_result_thread()
            self._spawn_fork_workers(workers)

            reactor.listenTCP(port, self.connection_factory_class(self, self.box_class), interface=host)

            try:
                reactor.run(installSignalHandlers=handle_signals)
            except KeyboardInterrupt:
                pass
            except:
                logger.error('exc occur.', exc_info=True)

        if use_reloader:
            autoreload.main(run_wrapper)
        else:
            run_wrapper()

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
            inner_p = Process(target=Worker(self, self.box_class, self.request_class).run)
            inner_p.daemon = True
            inner_p.start()
            return inner_p

        p_list = []

        for it in xrange(0, workers):
            p = start_worker_process()
            p_list.append(p)

        while True:
            for idx, p in enumerate(p_list):
                if not p.is_alive():
                    old_pid = p.pid
                    p = start_worker_process()
                    p_list[idx] = p

                    logger.error('process[%s] dead. start new process[%s]', old_pid, p.pid)

            time.sleep(1)

    def _poll_worker_result(self):
        """
        从队列里面获取worker的返回
        """

        def handle_data(_conn, _data):
            if _conn and _conn.transport:
                try:
                    if _data:
                        _conn.transport.write(_data)
                    else:
                        # data 为NULL代表关闭链接的意思
                        _conn.transport.loseConnection()
                except:
                    logger.error('exc occur. msg: %r', msg, exc_info=True)

        while True:
            try:
                msg = self.parent_input.get()
            except KeyboardInterrupt:
                break
            except:
                logger.error('exc occur.', exc_info=True)
                break

            conn = self.conn_dict.get(msg.get('conn_id'))
            data = msg.get('data')

            # 参考 http://twistedsphinx.funsize.net/projects/core/howto/threading.html
            reactor.callFromThread(handle_data, conn, data)
