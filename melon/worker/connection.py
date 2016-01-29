# -*- coding: utf-8 -*-

import os
import socket
import thread
from netkit.contrib.tcp_client import TcpClient
from ..gw_box import GWBox
import time
from .. import constants
from ..log import logger


class Connection(object):

    job_info = None

    def __init__(self, app, host, port, conn_timeout):
        self.app = app
        # 直接创建即可
        self.client = TcpClient(GWBox, host, port, conn_timeout)

    def run(self):
        thread.start_new_thread(self._monitor_job_timeout, ())
        while 1:
            try:
                self._handle()
            except KeyboardInterrupt:
                break
            except:
                logger.error('exc occur.', exc_info=True)

    def _monitor_job_timeout(self):
        """
        监控job的耗时
        :return:
        """

        while self.app.enable:
            time.sleep(1)

            job_info = self.job_info
            if job_info:
                past_time = time.time() - job_info['begin_time']
                if self.app.job_timeout is not None and past_time > self.app.job_timeout:
                    # 说明worker的处理时间已经太长了
                    logger.error('job is timeout: %s / %s, request: %s',
                                 past_time, self.app.job_timeout, job_info['request'])
                    # 强制从子线程退出worker
                    os._exit(-1)

    def _handle(self):
        while self.app.enable and self.closed():
            if not self._connect():
                logger.error('connect fail, host: %s, port: %s, sleep %ss', 
                             self.client.host, self.client.port, constants.TRY_CONNECT_INTERVAL)
                time.sleep(constants.TRY_CONNECT_INTERVAL)

        if not self.app.enable:
            # 安全退出
            raise KeyboardInterrupt

        # 跟gateway要job
        self._ask_for_job()
        self._read_message()

    def _ask_for_job(self):
        gw_box = GWBox()
        gw_box.cmd = constants.CMD_WORKER_ASK_FOR_JOB

        return self.write(gw_box.pack())

    def _connect(self):
        try:
            self.client.connect()
        except KeyboardInterrupt, e:
            raise e
        except:
            return False
        else:
            self.app.events.create_conn(self)
            for bp in self.app.blueprints:
                bp.events.create_app_conn(self)

            return True

    def write(self, data):
        """
        发送数据    True: 成功   else: 失败
        """
        if self.client.closed():
            logger.error('connection closed. data: %r', data)
            return False

        # 只支持字符串
        self.app.events.before_response(self, data)
        for bp in self.app.blueprints:
            bp.events.before_app_response(self, data)

        ret = self.client.write(data)
        if not ret:
            logger.error('connection write fail. data: %r', data)

        for bp in self.app.blueprints:
            bp.events.after_app_response(self, data, ret)
        self.app.events.after_response(self, data, ret)

        return ret

    def _read_message(self):
        req_gw_box = None

        while 1:
            try:
                # 读取数据 gw_box
                req_gw_box = self.client.read()
            except socket.timeout:
                # 超时了
                if not self.app.enable:
                    return
                else:
                    # 继续读
                    continue
            else:
                # 正常收到数据了
                break

        if req_gw_box:
            self._on_read_complete(req_gw_box)

        if self.closed():
            self._on_connection_close()

    def _on_connection_close(self):
        # 链接被关闭的回调

        logger.error('connection closed, host: %s, port: %s', self.client.host, self.client.port)

        for bp in self.app.blueprints:
            bp.events.close_app_conn(self)
        self.app.events.close_conn(self)

    def _on_read_complete(self, data):
        """
        数据获取结束
        """
        request = self.app.request_class(self, data)

        # 设置job开始处理的时间和信息
        self.job_info = dict(
            begin_time=time.time(),
            request=request,
        )
        self._handle_request(request)
        self.job_info = None

    def _handle_request(self, request):
        """
        出现任何异常的时候，服务器不再主动关闭连接
        """

        if not request.is_valid:
            return False

        if request.gw_box.cmd == constants.CMD_CLIENT_CREATED:
            self.app.events.create_client(request)
            for bp in self.app.blueprints:
                bp.events.create_app_client(request)
            return True
        elif request.gw_box.cmd == constants.CMD_CLIENT_CLOSED:
            self.app.events.close_client(request)
            for bp in self.app.blueprints:
                bp.events.close_app_client(request)
            return True

        if not request.view_func:
            logger.info('cmd invalid. request: %s' % request)
            if not request.responded:
                request.write_to_client(dict(ret=constants.RET_INVALID_CMD))
            return False

        if not self.app.got_first_request:
            self.app.got_first_request = True
            self.app.events.before_first_request(request)
            for bp in self.app.blueprints:
                bp.events.before_app_first_request(request)

        self.app.events.before_request(request)
        for bp in self.app.blueprints:
            bp.events.before_app_request(request)
        if request.blueprint:
            request.blueprint.events.before_request(request)

        view_func_exc = None

        try:
            request.view_func(request)
        except Exception, e:
            logger.error('view_func raise exception. request: %s, e: %s',
                         request, e, exc_info=True)
            view_func_exc = e
            # 必须是没有回应过
            if not request.responded:
                request.write_to_client(dict(ret=constants.RET_INTERNAL))

        if request.blueprint:
            request.blueprint.events.after_request(request, view_func_exc)
        for bp in self.app.blueprints:
            bp.events.after_app_request(request, view_func_exc)
        self.app.events.after_request(request, view_func_exc)

        return True

    def close(self):
        """
        直接关闭连接
        """
        self.client.close()

    def closed(self):
        """
        连接是否已经关闭
        :return:
        """
        return self.client.closed()

