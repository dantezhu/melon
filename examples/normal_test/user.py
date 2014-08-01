# -*- coding: utf-8 -*-

from reimp import Blueprint

bp = Blueprint()


@bp.route(101)
def login(request):
    request.write(dict(ret=11))
