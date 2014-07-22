# -*- coding: utf-8 -*-


class RoutesMixin(object):
    """
    专门做路由管理
    """

    rule_map = None
    events = None

    def __init__(self):
        self.rule_map = dict()

    def add_route_rule(self, cmd=None, view_func=None, **options):
        assert view_func is not None, 'expected view func if cmd is not provided.'

        cmd = cmd or view_func.__name__

        if cmd in self.rule_map and view_func != self.rule_map[cmd]:
            raise Exception, 'duplicate view_func for cmd: %(cmd)s, old_view_func:%(old_view_func)s, new_view_func: %(new_view_func)s' % dict(
                cmd=cmd,
                old_view_func=self.rule_map[cmd],
                new_view_func=view_func,
            )

        self.rule_map[cmd] = view_func

    def route(self, cmd=None, **options):
        def decorator(f):
            self.add_route_rule(cmd, f, **options)
            return f
        return decorator

    def get_route_view_func(self, cmd):
        return self.rule_map.get(cmd)
