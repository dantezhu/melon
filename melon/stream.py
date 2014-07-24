# -*- coding: utf-8 -*-

"""
兼容tornado 4.0
"""

from tornado.iostream import IOStream, gen_log, UnsatisfiableReadError, _double_prefix


class Stream(IOStream):

    def __init__(self, *args, **kwargs):
        super(Stream, self).__init__(*args, **kwargs)
        self._read_checker = None

    def read_with_checker(self, checker, callback=None):
        future = self._set_read_callback(callback)
        self._read_checker = checker
        try:
            self._try_inline_read()
        except UnsatisfiableReadError as e:
            # Handle this the same way as in _handle_events.
            gen_log.info("Unsatisfiable read, closing connection: %s" % e)
            self.close(exc_info=True)
            return future
        return future

    def _read_from_buffer(self, pos):
        self._read_checker = None
        return super(Stream, self)._read_from_buffer(pos)

    def _find_read_pos(self):
        """
        为了解决checker
        :return:
        """
        if self._read_checker is not None:
            if self._read_buffer:
                while True:
                    loc = self._read_checker(self._read_buffer[0])
                    if loc > 0:
                        # 说明就是要这些长度
                        return loc
                    elif loc < 0:
                        # 说明接受的数据已经有问题了，直接把数据删掉，并退出
                        self._read_buffer.popleft()
                        break

                    if len(self._read_buffer) == 1:
                        break
                    _double_prefix(self._read_buffer)

        return super(Stream, self)._find_read_pos()
