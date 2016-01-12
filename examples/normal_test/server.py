# -*- coding: utf-8 -*-

import logging

from reimp import Melon, logger, Box
import user

app = Melon(Box, {
    1: {
        'count': 2,
    }
}, lambda box: 1)


@app.create_worker
def create_worker(worker):
    logger.error('create_worker: %r', worker)


@app.before_first_request
def before_first_request(request):
    logger.error('before_first_request')


@app.before_request
def before_request(request):
    logger.error('before_request')


@app.after_request
def after_request(request, exc):
    logger.error('after_request: %s', exc)


@app.before_response
def before_response(worker, rsp):
    logger.error('before_response: %r', rsp)


@app.after_response
def after_response(worker, rsp, result):
    logger.error('after_response: %r, %s', rsp, result)


@app.route(1)
def index(request):
    logger.error('request: %s', request)
    request.write(dict(ret=100))


app.register_blueprint(user.bp)
app.run('127.0.0.1', 7777, workers=2)
