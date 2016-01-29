# -*- coding: utf-8 -*-

from twisted.internet.protocol import Protocol, Factory

from melon.utils import safe_call
from melon.log import logger
from melon.proxy.worker_box import WorkerBox


class ClientConnectionFactory(Factory):

    def __init__(self, app):
        self.app = app

    def buildProtocol(self, addr):
        return ClientConnection(self, (addr.host, addr.port))


class ClientConnection(Protocol):
    _read_buffer = None

    def __init__(self, factory, address):
        self.factory = factory
        self.address = address
        self._read_buffer = ''

        # 放到弱引用映射里去
        self.conn_id = self.factory.app.alloc_conn_id()
        self.factory.app.conn_dict[self.conn_id] = self

    def dataReceived(self, data):
        """
        当数据接受到时
        :param data:
        :return:
        """
        self._read_buffer += data

        while self._read_buffer:
            # 因为box后面还是要用的
            box = self.factory.app.box_class()
            ret = box.unpack(self._read_buffer)
            if ret == 0:
                # 说明要继续收
                return
            elif ret > 0:
                # 收好了
                box_data = self._read_buffer[:ret]
                self._read_buffer = self._read_buffer[ret:]
                safe_call(self._on_read_complete, box_data, box)
                continue
            else:
                # 数据已经混乱了，全部丢弃
                logger.error('buffer invalid. ret: %d, read_buffer: %r', ret, self._read_buffer)
                self._read_buffer = ''
                return

    def _on_read_complete(self, data, box):
        """
        完整数据接收完成
        :param data: 原始数据
        :param box: 解析之后的box
        :return:
        """
        worker_box = WorkerBox(init_data=dict(
            body=data
        ))

        # 获取映射的group_id
        group_id = self.factory.app.group_router(box)

