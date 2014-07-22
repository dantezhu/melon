# -*- coding: utf-8 -*-

import logging
from melon import Melon, logger
from netkit.box import Box
import user

logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


app = Melon(Box)
app.register_blueprint(user.bp)

@app.route(1)
def index(request):
    request.write(dict(ret=1))

app.run('127.0.0.1', 7777, workers=2, debug=True)
