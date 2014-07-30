# -*- coding: utf-8 -*-

from twisted.internet.protocol import Protocol, Factory

from .utils import safe_call
from .log import logger


class ConnectionFactory(Factory):

    def __init__(self, app, box_class):
        self.app = app
        self.box_class = box_class

    def buildProtocol(self, addr):
        return Connection(self, (addr.host, addr.port))


class Connection(Protocol):
    _read_buffer = None

    def __init__(self, factory, address):
        self.factory = factory
        self.address = address
        self._read_buffer = ''
        # 放到弱引用映射里去
        self.factory.app.conn_dict[id(self)] = self

    def dataReceived(self, data):
        """
        当数据接受到时
        :param data:
        :return:
        """
        self._read_buffer += data
        box = self.factory.box_class()

        while True:
            ret = box.check(self._read_buffer)
            if ret == 0:
                # 说明要继续收
                return
            elif ret > 0:
                # 收好了
                box_data = self._read_buffer[:ret]
                self._read_buffer = self._read_buffer[ret:]
                safe_call(self.fullDataReceived, box_data)
                continue
            else:
                # 数据已经混乱了，全部丢弃
                logger.error('buffer invalid. ret: %d, read_buffer: %r', ret, self._read_buffer)
                self._read_buffer = ''
                return

    def fullDataReceived(self, data):
        """
        完整数据接收完成
        不需要解包成box，因为还要再发出去
        :param data:
        :return:
        """
        try:
            self.factory.app.parent_output.put_nowait(dict(
                conn_id=id(self),
                address=self.address,
                data=data,
            ))
        except:
            logger.error('exc occur.', exc_info=True)
