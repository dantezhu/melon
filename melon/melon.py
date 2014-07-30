# -*- coding: utf-8 -*-

from multiprocessing import Queue, Process
import weakref
from threading import Thread
from twisted.internet.endpoints import TCP4ServerEndpoint
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

    def __init__(self, box_class, conn_factory_class=None, request_class=None,
                 input_queue_maxsize=None, output_queue_maxsize=None):
        super(Melon, self).__init__()
        self.box_class = box_class
        self.conn_factory_class = conn_factory_class or ConnectionFactory

        self.blueprints = list()
        self.parent_input = Queue(input_queue_maxsize)
        self.parent_output = Queue(output_queue_maxsize)
        self.conn_dict = weakref.WeakValueDictionary()
        self.request_class = request_class or Request

    def register_blueprint(self, blueprint):
        blueprint.register2app(self)

    def run(self, host, port, debug=None, use_reloader=None, workers=1):
        if debug is not None:
            self.debug = debug
        use_reloader = use_reloader if use_reloader is not None else self.debug

        def run_wrapper():
            logger.info('Running server on %s:%s, debug: %s, use_reloader: %s',
                        host, port, self.debug, use_reloader)

            # 启动获取worker数据的线程
            thread = Thread(target=self._poll_worker_result)
            thread.daemon = True
            thread.start()

            if workers:
                for it in xrange(0, workers):
                    p = Process(target=Worker(self, self.box_class, self.request_class).run)
                    p._daemonic = True
                    p.start()

            endpoint = TCP4ServerEndpoint(reactor, port, interface=host)
            endpoint.listen(self.conn_factory_class(self, self.box_class))

            # 否则会报exceptions.ValueError: signal only works in main thread
            installSignalHandlers = 0 if autoreload else 1

            try:
                reactor.run(installSignalHandlers=installSignalHandlers)
            except KeyboardInterrupt:
                pass
            except:
                logger.error('exc occur.', exc_info=True)

        if use_reloader:
            autoreload.main(run_wrapper)
        else:
            run_wrapper()

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
