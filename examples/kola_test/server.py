# -*- coding: utf-8 -*-

import logging
from melon import Melon, logger
from kola_box import KolaBox
import user

logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

app = Melon(KolaBox)
app.register_blueprint(user.bp)

@app.route()
def index(request):
    request.write(dict(ret=1))

app.run('127.0.0.1', 7777, workers=1)
