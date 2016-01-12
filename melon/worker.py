# -*- coding: utf-8 -*-

import signal
from . import constants
from .log import logger


class Worker(object):

    group_id = None
    child_input = None
    child_output = None

    def __init__(self, app, group_id, child_input, child_output):
        """

        :param app: melon app
        :param group_id: group_id
        :param child_input: 读取数据
        :param child_output: 写入数据
        :return:
        """
        self.app = app
        self.group_id = group_id
        self.child_input = child_input
        self.child_output = child_output

    def run(self):
        self._handle_signals()

        self.app.events.create_worker(self)
        for bp in self.app.blueprints:
            bp.events.create_app_worker(self)

        while 1:
            try:
                msg = self.read()
            except KeyboardInterrupt:
                break
            except:
                logger.error('exc occur.', exc_info=True)
                break

            try:
                request = self.app.request_class(self, msg)
                self._handle_request(request)
            except:
                logger.error('exc occur. msg: %r', msg, exc_info=True)

    def read(self):
        """
        读取消息
        :return:
        """
        return self.child_input.get()

    def write(self, msg):
        """
        接收消息
        :param msg:
        :return:
        """

        self.app.events.before_response(self, msg)
        for bp in self.app.blueprints:
            bp.events.before_app_response(self, msg)

        try:
            self.child_output.put_nowait(msg)
            result = True
        except:
            logger.error('exc occur. msg: %r', msg, exc_info=True)
            result = False

        for bp in self.app.blueprints:
            bp.events.after_app_response(self, msg, result)
        self.app.events.after_response(self, msg, result)

        return result

    def _handle_request(self, request):
        """
        出现任何异常的时候，服务器不再主动关闭连接
        """

        if not request.is_valid:
            return False

        if not request.view_func:
            logger.error('cmd invalid. request: %s' % request)
            request.write(dict(ret=constants.RET_INVALID_CMD))
            return False

        if not self.app.got_first_request:
            self.app.got_first_request = True

            self.app.events.before_first_request(request)
            for bp in self.app.blueprints:
                bp.events.before_app_first_request(request)

        self.app.events.before_request(request)
        for bp in self.app.blueprints:
            bp.events.before_app_request(request)
        if request.blueprint:
            request.blueprint.events.before_request(request)

        view_func_exc = None

        try:
            request.view_func(request)
        except Exception, e:
            logger.error('view_func raise exception. request: %s, e: %s',
                         request, e, exc_info=True)
            view_func_exc = e
            request.write(dict(ret=constants.RET_INTERNAL))

        if request.blueprint:
            request.blueprint.events.after_request(request, view_func_exc)
        for bp in self.app.blueprints:
            bp.events.after_app_request(request, view_func_exc)
        self.app.events.after_request(request, view_func_exc)

        return True

    def _handle_signals(self):
        """
        因为主进程的reactor重新处理了SIGINT，会导致子进程也会响应，改为SIG_IGN之后，就可以保证父进程先退出，之后再由父进程term所有的子进程
        :return:
        """
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def __repr__(self):
        return 'worker. group_id: %s' % self.group_id
