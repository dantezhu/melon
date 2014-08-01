# -*- coding: utf-8 -*-

from reimp import Blueprint

bp = Blueprint('user')


@bp.route(101)
def login(request):
    request.write(dict(ret=11))
