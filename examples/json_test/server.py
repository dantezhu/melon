# -*- coding: utf-8 -*-

from melon import Melon
from json_box import JsonBox
import user

app = Melon(JsonBox)
app.register_blueprint(user.bp)

@app.route()
def index(request):
    request.write(dict(ret=1))

app.run('127.0.0.1', 7777, workers=2)
