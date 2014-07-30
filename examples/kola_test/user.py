# -*- coding: utf-8 -*-

from melon import Blueprint

bp = Blueprint('user')


@bp.route()
def login(request):
    request.write(dict(ret=11, address=request.address))
