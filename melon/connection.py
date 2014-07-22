# -*- coding: utf-8 -*-

from .utils import safe_call
from .log import logger
from .stream import Stream


class Connection(object):
    def __init__(self, app, box_class, stream, address):
        self.app = app
        self.box_class = box_class
        self.stream = Stream(socket=stream.socket,
                             io_loop=stream.io_loop,
                             max_buffer_size=stream.max_buffer_size,
                             read_chunk_size=stream.read_chunk_size,
                             max_write_buffer_size=stream.max_write_buffer_size,
                             )
        self.address = address

        self._clear_request_state()

        self.stream.set_close_callback(self._on_connection_close)

    def write(self, data, callback=None):
        """
        发送数据(实现是放到发送队列)
        """
        if not self.stream.closed():
            self._write_callback = callback
            self.stream.write(data, self._on_write_complete)

    def finish(self):
        """Finishes the request."""
        self._request_finished = True
        # No more data is coming, so instruct TCP to send any remaining
        # data immediately instead of waiting for a full packet or ack.
        self.stream.set_nodelay(True)
        if not self.stream.writing():
            self.close()

    def close(self, exc_info=False):
        """
        直接关闭连接
        注意: 不要在write完了之后直接调用，会导致write发送不出去(这个不太确定，tcp是全双工，理论上关闭了对方也会收到)
        """
        self.stream.close(exc_info)
        self._clear_request_state()

    def set_close_callback(self, callback):
        """Sets a callback that will be run when the connection is closed.
        """
        self._close_callback = callback

    def handle(self):
        """
        启动执行
        """
        # 开始等待数据
        self._read_message()

    def _on_connection_close(self):
        # 链接被关闭的回调
        logger.debug('socket closed')

        if self._close_callback is not None:
            callback = self._close_callback
            self._close_callback = None
            safe_call(callback)

    def _read_message(self):
        if not self.stream.closed():
            self.stream.read_with_checker(self.box_class().check, self._on_read_complete)
            #self.stream.read_until('\n', self._on_read_complete)

    def _on_read_complete(self, raw_data):
        """
        数据获取结束
        """
        logger.debug('raw_data: %s', raw_data)
        # 数据写入
        try:
            #self.app.output_queue.put(struct.pack('i' + str(len(raw_data)) + 's', id(self), raw_data))
            #self.app.output_queue.put((id(self), raw_data))
            self.app.output_queue.put(dict(
                conn_id=id(self),
                address=self.address,
                data=raw_data,
            ))
        finally:
            # 不管怎么样也得继续读
            self._read_message()

    def _on_write_complete(self):
        if self._write_callback is not None:
            callback = self._write_callback
            self._write_callback = None
            safe_call(callback)
        if self._request_finished and not self.stream.writing():
            self.close()

    def _clear_request_state(self):
        """Clears the per-request state.

        This is run in between requests to allow the previous handler
        to be garbage collected (and prevent spurious close callbacks),
        and when the connection is closed (to break up cycles and
        facilitate garbage collection in cpython).
        """
        self._request_finished = False
        self._write_callback = None
        self._close_callback = None
