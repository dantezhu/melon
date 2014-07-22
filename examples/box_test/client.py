# -*- coding: utf-8 -*-


import time
from netkit.stream import Stream
from netkit.box import Box

import logging
import socket

logger = logging.getLogger('melon')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

address = ('127.0.0.1', 7777)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(address)

stream = Stream(s)

box = Box()
box.cmd = 1
box.body = 'on no'

t1 = time.time()
stream.write(box.pack())

while True:
    # 阻塞
    buf = stream.read_with_checker(Box().check)
    print 'time:', time.time() - t1

    if buf:
        box2 = Box()
        box2.unpack(buf)
        print box2

    if stream.closed():
        print 'server closed'
        break

s.close()
