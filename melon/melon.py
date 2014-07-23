# -*- coding: utf-8 -*-

from multiprocessing import Queue, Process
import weakref
from tornado.tcpserver import TCPServer
from tornado.ioloop import IOLoop
from threading import Thread

from .log import logger
from .connection import Connection
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

    def __init__(self, box_class, conn_class=None, request_class=None):
        super(Melon, self).__init__()
        self.box_class = box_class
        self.conn_class = conn_class or Connection

        self.blueprints = list()
        self.parent_input = Queue()
        self.parent_output = Queue()
        self.conn_dict = weakref.WeakValueDictionary()
        self.request_class = request_class or Request

        self._configure_exc_handler()

    def register_blueprint(self, blueprint):
        blueprint.register2app(self)

    def run(self, host, port, debug=None, use_reloader=None, workers=1):
        if debug is not None:
            self.debug = debug
        use_reloader = use_reloader if use_reloader is not None else self.debug

        def run_wrapper():
            logger.info('Running server on %s:%s, debug: %s, use_reloader: %s',
                        host, port, self.debug, use_reloader)

            class MelonServer(TCPServer):
                def handle_stream(sub_self, stream, address):
                    conn = Connection(self, self.box_class, stream, address)
                    self.conn_dict[id(conn)] = conn
                    conn.handle()

            self.server = MelonServer()
            self.server.listen(port, host)

            # 启动获取数据数据的线程
            thread = Thread(target=self.poll_worker_input)
            thread.daemon = True
            thread.start()

            if workers:
                for it in xrange(0, workers):
                    p = Process(target=Worker(self, self.box_class, self.request_class).run,
                                args=(self.parent_output, self.parent_input))
                    p._daemonic = True
                    p.start()

            IOLoop.instance().start()

        if use_reloader:
            autoreload.main(run_wrapper)
        else:
            run_wrapper()

    def poll_worker_input(self):
        """
        从队列里面获取worker的返回
        """
        while True:
            msg = self.parent_input.get()
            conn = self.conn_dict.get(msg.get('conn_id'))
            if conn:
                if msg.get('data'):
                    conn.write(msg.get('data'))
                else:
                    # data 为NULL代表关闭链接的意思
                    conn.finish()

    def _configure_exc_handler(self):
        """
        因为tornado的默认handler的log不一定会被配置
        """
        def exc_handler(callback):
            logger.error("Exception in callback %r", callback, exc_info=True)

        IOLoop.instance().handle_callback_exception = exc_handler
