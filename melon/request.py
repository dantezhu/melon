# -*- coding: utf-8 -*-

import os
import numbers
from .log import logger


class Request(object):
    """
    请求
    """

    worker = None
    box_class = None
    msg = None
    box = None
    is_valid = False
    blueprint = None
    blueprint_name = None
    blueprint_cmd = None

    def __init__(self, worker, box_class, msg):
        self.worker = worker
        self.box_class = box_class
        self.msg = msg
        self.is_valid = self._parse_raw_data()

    def _parse_raw_data(self):
        try:
            self.box = self.box_class()
        except Exception, e:
            logger.error('create box fail. e: %s, request: %s', e, self)
            return False

        if self.box.unpack(self.msg.get('data') or '') > 0:
            self._parse_blueprint_info()
            return True
        else:
            logger.error('unpack fail. request: %s', self)
            return False

    def _parse_blueprint_info(self):
        if self.cmd is None:
            return

        cmd_parts = str(self.cmd).split('.')
        self.blueprint_name, self.blueprint_cmd = cmd_parts if len(cmd_parts) == 2 else (None, self.cmd)

        for bp in self.app.blueprints:
            if self.blueprint_name == bp.name or isinstance(self.blueprint_cmd, numbers.Number):
                # blueprint name 匹配; 或者cmd是数字类型，即与blueprint name无关
                if bp.get_route_view_func(self.blueprint_cmd):
                    self.blueprint = bp
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

    def write(self, data):
        if isinstance(data, dict):
            # 生成box
            data = self.box.map(data)

        if isinstance(data, self.box_class):
            data = data.pack()

        msg = dict(
            conn_id=self.msg.get('conn_id'),
            data=data,
            pid=os.getpid(),
        )

        return self.worker.write(msg)

    def close(self, exc_info=False):
        return self.write(None)

    def __repr__(self):
        return 'client_address: %r, cmd: %r, msg: %r' % (self.address, self.cmd, repr(self.msg))
