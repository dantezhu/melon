# -*- coding: utf-8 -*-

from reimp import Blueprint, logger

bp = Blueprint('user')


@bp.route(101)
def login(request):
    logger.error('request: %s, worker: %s', request, request.worker)
    request.write(dict(ret=101, body=repr(request.address)))


@bp.create_app_worker
def create_app_worker(worker):
    logger.error('bp.create_app_worker: %r', worker)


@bp.before_app_first_request
def before_app_first_request(request):
    logger.error('bp.before_app_first_request')


@bp.before_app_request
def before_app_request(request):
    logger.error('bp.before_app_request')


@bp.after_app_request
def after_app_request(request, exc):
    logger.error('bp.after_app_request')


@bp.before_request
def before_request(request):
    logger.error('bp.before_request')
    request.interrupt(dict(ret=-100))
    # request.interrupt()


@bp.after_request
def after_request(request, exc):
    logger.error('bp.after_request')


@bp.before_app_response
def before_app_response(worker, rsp):
    logger.error('bp.before_app_response rsp: %r', rsp)


@bp.after_app_response
def after_app_response(worker, rsp, result):
    logger.error('bp.after_app_response rsp: %r, %s', rsp, result)
