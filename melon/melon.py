# -*- coding: utf-8 -*-

import sys
import time
from multiprocessing import Queue, Process
import weakref
from threading import Thread
# linux 默认就是epoll
from twisted.internet import reactor
import signal
from collections import Counter
import setproctitle

from .log import logger
from .connection import ConnectionFactory
from .worker import Worker
from .mixins import RoutesMixin, AppEventsMixin
from .request import Request
from . import constants


class Melon(RoutesMixin, AppEventsMixin):

    ############################## configurable begin ##############################
    name = constants.NAME

    box_class = None
    backlog = constants.SERVER_BACKLOG

    debug = False

    group_conf = None
    group_router = None
    ############################## configurable end   ##############################

    connection_factory_class = ConnectionFactory
    request_class = Request

    got_first_request = False

    parent_input_dict = None
    parent_output_dict = None
    conn_dict = None

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
        # 0 不代表无穷大，看代码是 SEM_VALUE_MAX = 32767L
        self.parent_input_dict = dict()
        self.parent_output_dict = dict()
        self.conn_dict = weakref.WeakValueDictionary()

    def register_blueprint(self, blueprint):
        blueprint.register_to_app(self)

    def run(self, host=None, port=None, debug=None):
        self._validate_cmds()

        if host is None:
            host = constants.SERVER_HOST
        if port is None:
            port = constants.SERVER_PORT
        if debug is not None:
            self.debug = debug

        def run_wrapper():
            logger.info('Running server on %s:%s, debug: %s',
                        host, port, self.debug)

            setproctitle.setproctitle(self.make_proc_name('master'))
            self._init_groups()
            self._spawn_poll_worker_result_thread()
            self._spawn_fork_workers()
            self._handle_parent_proc_signals()

            reactor.listenTCP(port, self.connection_factory_class(self),
                              backlog=self.backlog, interface=host)

            try:
                reactor.run(installSignalHandlers=False)
            except KeyboardInterrupt:
                pass
            except:
                logger.error('exc occur.', exc_info=True)

        run_wrapper()

    def make_proc_name(self, subtitle):
        """
        获取进程名称
        :param subtitle:
        :return:
        """
        proc_name = '[%s %s:%s] %s' % (
            self.name,
            constants.NAME,
            subtitle,
            ' '.join([sys.executable] + sys.argv)
        )

        return proc_name

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

    def _spawn_poll_worker_result_thread(self):
        """
        启动获取worker数据的线程
        """
        for group_id in self.group_conf:
            thread = Thread(target=self._poll_worker_result, args=(group_id,))
            thread.daemon = True
            thread.start()

    def _spawn_fork_workers(self):
        """
        通过线程启动多个worker
        """
        thread = Thread(target=self._fork_workers, args=())
        thread.daemon = True
        thread.start()

    def _fork_workers(self):
        def start_worker_process(target):
            inner_p = Process(target=target)
            inner_p.daemon = True
            inner_p.start()
            return inner_p

        p_list = []

        for group_id, conf in self.group_conf.items():

            child_input = self.parent_output_dict[group_id]
            child_output = self.parent_input_dict[group_id]
            worker = Worker(self, group_id, child_input, child_output)

            for it in xrange(0, conf.get('count', 1)):
                p = start_worker_process(worker.run)
                p_list.append(dict(
                    p=p,
                    worker=worker
                ))

        while 1:
            for info in p_list:
                p = info['p']
                worker = info['worker']

                if not p.is_alive():
                    old_pid = p.pid
                    p = start_worker_process(worker.run)
                    info['p'] = p

                    logger.error('process[%s] is dead. start new process[%s]. worker: %s', old_pid, p.pid, worker)

            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break
            except:
                logger.error('exc occur.', exc_info=True)
                break

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
