# -*- coding: utf-8 -*-

import signal
from . import constants
from .log import logger


class Worker(object):

    child_input = None
    child_output = None

    def __init__(self, app, box_class, request_class):
        self.app = app
        self.box_class = box_class
        self.request_class = request_class

    def run(self):
        self._handle_signals()

        while True:
            try:
                msg = self.child_input.get()
            except KeyboardInterrupt:
                break
            except:
                logger.error('exc occur.', exc_info=True)
                break

            try:
                request = self.request_class(self, self.box_class, msg)
                self._handle_request(request)
            except:
                logger.error('exc occur. msg: %r', msg, exc_info=True)

    @property
    def child_input(self):
        return self.app.parent_output

    @property
    def child_output(self):
        return self.app.parent_input

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

        view_func_result = None

        try:
            view_func_result = view_func(request)
        except Exception, e:
            logger.error('view_func raise exception. request: %s, view_func: %s, e: %s',
                         request, view_func, e, exc_info=True)
            request.write(dict(ret=constants.RET_INTERNAL))

        return view_func_result

    def _handle_signals(self):
        """
        因为主进程的reactor重新处理了SIGINT，会导致子进程也会响应，改为SIG_IGN之后，就可以保证父进程先退出，之后再由父进程term所有的子进程
        :return:
        """
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
