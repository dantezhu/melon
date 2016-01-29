# -*- coding: utf-8 -*-

import functools
from events import Events

from ..utils import safe_func


class RoutesMixin(object):
    """
    专门做路由管理
    """

    rule_map = None

    def __init__(self):
        self.rule_map = dict()

    def add_route_rule(self, cmd, view_func, **options):
        if cmd in self.rule_map and view_func != self.rule_map[cmd]['view_func']:
            raise Exception, 'duplicate view_func for cmd: %(cmd)s, old_view_func:%(old_view_func)s, new_view_func: %(new_view_func)s' % dict(
                cmd=cmd,
                old_view_func=self.rule_map[cmd]['view_func'],
                new_view_func=view_func,
            )

        self.rule_map[cmd] = dict(
            endpoint=view_func.__name__,
            view_func=view_func,
        )

    def route(self, cmd, **options):
        def decorator(f):
            self.add_route_rule(cmd, f, **options)
            return f
        return decorator

    def get_route_rule(self, cmd):
        return self.rule_map.get(cmd)


def _reg_event_handler(func):
    @functools.wraps(func)
    def func_wrapper(obj, handler):
        event = getattr(obj.events, func.__name__)
        event += safe_func(handler)

        return handler
    return func_wrapper


class AppEventsMixin(object):
    events = None

    def __init__(self):
        self.events = Events()

    @_reg_event_handler
    def create_worker(self, f):
        """
        创建worker
        f()
        """

    @_reg_event_handler
    def create_conn(self, f):
        """
        worker连接建立后
        f(conn)
        """

    @_reg_event_handler
    def before_first_request(self, f):
        """
        第一个请求，请求解析成功后
        f(request)
        """

    @_reg_event_handler
    def before_request(self, f):
        """
        请求解析成功后
        f(request)
        """

    @_reg_event_handler
    def after_request(self, f):
        """
        执行完route对应的view_func后
        f(request, exc)
        """

    @_reg_event_handler
    def before_response(self, f):
        """
        在 stream.write 之前，传入encode之后的data
        f(conn, response)
        """

    @_reg_event_handler
    def after_response(self, f):
        """
        在 stream.write 之后，传入encode之后的data
        f(conn, response, result)
        """

    @_reg_event_handler
    def close_conn(self, f):
        """
        worker连接close之后
        f(conn)
        """

    @_reg_event_handler
    def create_client(self, f):
        """
        client 连接之后.
        f(request)
        """

    @_reg_event_handler
    def close_client(self, f):
        """
        client close之后.
        f(request)
        """


class BlueprintEventsMixin(object):

    events = None

    def __init__(self):
        self.events = Events()

    @_reg_event_handler
    def before_request(self, f):
        """
        请求解析成功后
        f(request)
        """

    @_reg_event_handler
    def after_request(self, f):
        """
        执行完route对应的view_func后
        f(request, exc)
        """

    @_reg_event_handler
    def create_app_worker(self, f):
        """
        创建worker
        f()
        """

    @_reg_event_handler
    def create_app_conn(self, f):
        """
        worker连接建立后
        f(conn)
        """

    @_reg_event_handler
    def before_app_first_request(self, f):
        """
        第一次请求，请求解析成功后
        f(request)
        """

    @_reg_event_handler
    def before_app_request(self, f):
        """
        请求解析成功后
        f(request)
        """

    @_reg_event_handler
    def after_app_request(self, f):
        """
        执行完route对应的view_func后
        f(request, exc)
        """

    @_reg_event_handler
    def before_app_response(self, f):
        """
        在 stream.write 之前，传入encode之后的data
        f(conn, response)
        """

    @_reg_event_handler
    def after_app_response(self, f):
        """
        在 stream.write 之后，传入encode之后的data
        f(conn, response, result)
        """

    @_reg_event_handler
    def close_app_conn(self, f):
        """
        worker连接close之后
        f(conn)
        """

    @_reg_event_handler
    def create_app_client(self, f):
        """
        client 连接之后.
        f(request)
        """

    @_reg_event_handler
    def close_app_client(self, f):
        """
        client close之后.
        f(request)
        """