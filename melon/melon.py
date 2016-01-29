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
from .proxy import ClientConnectionFactory, WorkerConnectionFactory, Request
from .worker.worker import Worker
from .mixins import RoutesMixin, AppEventsMixin
from . import constants


class Melon(RoutesMixin, AppEventsMixin):

    conn_id_max = 9223372036854775807
    conn_id_counter = 0

    client_connection_factory_class = ClientConnectionFactory
    worker_connection_factory_class = WorkerConnectionFactory
    request_class = Request

    box_class = None

    debug = False
    got_first_request = False
    backlog = constants.SERVER_BACKLOG

    group_conf = None
    group_router = None

    conn_dict = None

    blueprints = None

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
        self.conn_dict = weakref.WeakValueDictionary()

    def register_blueprint(self, blueprint):
        blueprint.register_to_app(self)

    def alloc_conn_id(self):
        """
        获取自增的连接ID
        :return:
        """

        # 使用longlong型
        if self.conn_id_counter >= self.conn_id_max:
            self.conn_id_counter = 0

        self.conn_id_counter += 1

        return self.conn_id_counter

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

            reactor.listenTCP(port, self.client_connection_factory_class(self),
                              backlog=self.backlog, interface=host)

            # 启动监听worker

            for group_id in self.group_conf:
                address = "worker_%s.sock"

                # 给内部worker通信用的
                reactor.listenUnix(address, self.worker_connection_factory_class(self, group_id))

            try:
                reactor.run(installSignalHandlers=False)
            except KeyboardInterrupt:
                pass
            except:
                logger.error('exc occur.', exc_info=True)

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

