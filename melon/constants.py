# -*- coding: utf-8 -*-

LOGGER_NAME = 'melon'

# 系统返回码
RET_INVALID_CMD = -10000
RET_INTERNAL = -10001

# 默认host和port
SERVER_BACKLOG = 256

# 重连等待时间
TRY_CONNECT_INTERVAL = 1

# worker的group_id env
WORKER_GROUP_ENV_KEY = 'MELON_WORKER_GROUP'

# 网络连接超时(秒)，包括 connect once，read once，write once
CONN_TIMEOUT = 3
