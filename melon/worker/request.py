# -*- coding: utf-8 -*-

import numbers
from ..gw_box import GWBox
from ..log import logger
from .. import constants
from ..gw_pb2 import RspToUsers, CloseUsers


class Request(object):
    """
    请求
    """

    conn = None
    gw_box = None
    box = None
    is_valid = False
    # 是否已经作出回应
    responded = False
    blueprint = None
    route_rule = None

    def __init__(self, conn, gw_box):
        self.conn = conn
        self.gw_box = gw_box
        # 赋值
        self.is_valid = self._parse_raw_data()

    @property
    def app(self):
        return self.conn.app

    def _parse_raw_data(self):
        if not self.gw_box.body:
            return True

        try:
            self.box = self.app.box_class()
        except Exception, e:
            logger.error('parse raw_data fail. e: %s, request: %s', e, self)
            return False

        if self.box.unpack(self.gw_box.body) > 0:
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

    def write_to_client(self, data):
        """
        写回
        :param data: 可以是dict也可以是box
        :return:
        """

        assert not (self.app.rsp_once and self.responded), 'request has been responded'

        if isinstance(data, self.app.box_class):
            data = data.pack()
        elif isinstance(data, dict):
            data = self.box.map(data).pack()

        gw_box = self.gw_box.map(dict(
            cmd=constants.CMD_WRITE_TO_CLIENT,
            body=data,
        ))

        succ = self.conn.write(gw_box.pack())

        if succ:
            # 如果发送成功，就标记为已经回应
            self.responded = True

        return succ

    def write_to_users(self, data_list):
        """
        格式为
        [(uids, box), (uids, box, userdata) ...]
        :param data_list: userdata可不传，默认为0，conn.userdata & userdata == userdata
        :return:
        """

        msg = RspToUsers()

        for data_tuple in data_list:
            if len(data_tuple) == 2:
                uids, data = data_tuple
                userdata = None
            else:
                uids, data, userdata = data_tuple

            if isinstance(data, self.app.box_class):
                data = data.pack()
            elif isinstance(data, dict):
                data = self.app.box_class(data).pack()

            row = msg.rows.add()
            row.buf = data
            row.userdata = userdata or 0
            row.uids.extend(uids)

        gw_box = GWBox()
        gw_box.cmd = constants.CMD_WRITE_TO_USERS
        gw_box.body = msg.SerializeToString()

        return self.conn.write(gw_box.pack())

    def close_client(self):
        gw_box = self.gw_box.map(dict(
            cmd=constants.CMD_CLOSE_CLIENT,
        ))

        return self.conn.write(gw_box.pack())

    def close_users(self, uids, userdata=None):
        msg = CloseUsers()
        msg.uids.extend(uids)
        msg.userdata = userdata or 0

        gw_box = GWBox()
        gw_box.cmd = constants.CMD_CLOSE_USERS
        gw_box.body = msg.SerializeToString()

        return self.conn.write(gw_box.pack())

    def login_client(self, uid, userdata=None):
        gw_box = self.gw_box.map(dict(
            cmd=constants.CMD_LOGIN_CLIENT,
            uid=uid,
            userdata=userdata or 0,
        ))

        return self.conn.write(gw_box.pack())

    def logout_client(self):
        gw_box = self.gw_box.map(dict(
            cmd=constants.CMD_LOGOUT_CLIENT,
        ))

        return self.conn.write(gw_box.pack())

    def __repr__(self):
        return 'cmd: %r, endpoint: %s, gw_box: %r' % (self.cmd, self.endpoint, self.gw_box)
