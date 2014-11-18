# -*- coding: utf-8 -*-

import signal
from . import constants
from .log import logger


class Worker(object):

    child_input = None
    child_output = None

    def __init__(self, app):
        self.app = app

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

    @property
    def child_input(self):
        return self.app.parent_output

    @property
    def child_output(self):
        return self.app.parent_input

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
            return None

        view_func = self.app.get_route_view_func(request.cmd)
        if not view_func and request.blueprint:
            view_func = request.blueprint.get_route_view_func(request.blueprint_cmd)

        if not view_func:
            logger.error('cmd invalid. request: %s' % request)
            request.write(dict(ret=constants.RET_INVALID_CMD))
            return None

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
        view_func_result = None

        try:
            view_func_result = view_func(request)
        except Exception, e:
            logger.error('view_func raise exception. request: %s, view_func: %s, e: %s',
                         request, view_func, e, exc_info=True)
            view_func_exc = e
            request.write(dict(ret=constants.RET_INTERNAL))

        if request.blueprint:
            request.blueprint.events.after_request(request, view_func_exc or view_func_result)
        for bp in self.app.blueprints:
            bp.events.after_app_request(request, view_func_exc or view_func_result)
        self.app.events.after_request(request, view_func_exc or view_func_result)

        return view_func_result

    def _handle_signals(self):
        """
        因为主进程的reactor重新处理了SIGINT，会导致子进程也会响应，改为SIG_IGN之后，就可以保证父进程先退出，之后再由父进程term所有的子进程
        :return:
        """
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
