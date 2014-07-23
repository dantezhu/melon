# -*- coding: utf-8 -*-

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
        while True:
            msg = self.child_input.get()

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

