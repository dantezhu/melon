# -*- coding: utf-8 -*-

import os
import numbers
from .log import logger


class Request(object):
    """
    请求
    """

    worker = None
    msg = None
    box = None
    is_valid = False
    blueprint = None
    route_rule = None
    # 是否中断处理，即不调用view_func，主要用在before_request中
    interrupted = False

    def __init__(self, worker, msg):
        self.worker = worker
        self.msg = msg
        self.is_valid = self._parse_raw_data()

    def _parse_raw_data(self):
        try:
            self.box = self.worker.app.box_class()
        except Exception, e:
            logger.error('create box fail. e: %s, request: %s', e, self)
            return False

        if self.box.unpack(self.msg.get('data') or '') > 0:
            self._parse_route_rule()
            return True
        else:
            logger.error('unpack fail. request: %s', self)
            return False

    def _parse_route_rule(self):
        if self.cmd is None:
            return

        route_rule = self.app.get_route_rule(self.cmd)
        if route_rule:
            # 在app层，直接返回
            self.route_rule = route_rule
            return

        for bp in self.app.blueprints:
            route_rule = bp.get_route_rule(self.cmd)
            if route_rule:
                self.blueprint = bp
                self.route_rule = route_rule
                break

    @property
    def app(self):
        return self.worker.app

    @property
    def address(self):
        return self.msg.get('address')

    @property
    def cmd(self):
        try:
            return self.box.cmd
        except:
            return None

    @property
    def view_func(self):
        return self.route_rule['view_func'] if self.route_rule else None

    @property
    def endpoint(self):
        if not self.route_rule:
            return None

        bp_endpoint = self.route_rule['endpoint']

        return '.'.join([self.blueprint.name, bp_endpoint] if self.blueprint else [bp_endpoint])

    def write(self, data):
        if isinstance(data, dict):
            # 生成box
            data = self.box.map(data)

        if isinstance(data, self.worker.app.box_class):
            data = data.pack()

        msg = dict(
            conn_id=self.msg.get('conn_id'),
            data=data,
            pid=os.getpid(),
        )

        return self.worker.write(msg)

    def close(self, exc_info=False):
        return self.write(None)

    def interrupt(self, data=None):
        """
        中断处理
        :param data: 要响应的数据，不传即不响应
        :return:
        """
        self.interrupted = True
        if data is not None:
            return self.write(data)
        else:
            return True

    def __repr__(self):
        return 'client_address: %r, cmd: %r, endpoint: %s, msg: %r' % (
            self.address, self.cmd, self.endpoint, self.msg
        )
