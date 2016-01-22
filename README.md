melon
=====

twisted with multiprocessing worker

### 说明

1. 多线程fork时，在子进程中，只有调用fork的线程会存在。 所以melon的设计是没有问题的

### TODO

1. msg中要有一个tag为进程随机数，防止消息发往重启后的错误conn_id


