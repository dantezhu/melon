# -*- coding: utf-8 -*-

import logging
from melon import Melon, logger
from json_box import JsonBox
import user

logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

app = Melon(JsonBox)
app.register_blueprint(user.bp)

@app.route()
def index(request):
    request.write(dict(ret=1))

app.run('127.0.0.1', 7777, workers=2)
