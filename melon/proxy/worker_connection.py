# -*- coding: utf-8 -*-

from twisted.internet.protocol import Protocol, Factory, connectionDone

from melon.utils import safe_call
from melon.log import logger


class WorkerConnectionFactory(Factory):

    def __init__(self, app, group_id):
        self.app = app
        self.group_id = group_id

    def buildProtocol(self, addr):
        return WorkerConnection(self, (addr.host, addr.port), self.group_id)


class WorkerConnection(Protocol):
    _read_buffer = None

    def __init__(self, factory, address, group_id):
        """
        :param factory: 工厂类
        :param address: 地址
        :param group_id: 所属的组
        :return:
        """
        self.factory = factory
        self.address = address
        self.group_id = group_id
        self._read_buffer = ''
        # 放到弱引用映射里去
        self.factory.app.conn_dict[id(self)] = self

    def connectionMade(self):
        pass

    def connectionLost(self, reason=connectionDone):
        pass

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
        msg = dict(
            conn_id=id(self),
            address=self.address,
            data=data,
        )

        # 获取映射的group_id
        group_id = self.factory.app.group_router(box)

        try:
            self.factory.app.parent_output_dict[group_id].put_nowait(msg)
        except:
            logger.error('exc occur. msg: %r', msg, exc_info=True)
