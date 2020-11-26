import asyncio
import socket
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count

from yaml import safe_load, load as yaml_load

from connection.mq_conn import mq_server
from connection.mysql_conn import mysql_server
from connection.redis_conn import redis_server

from service.send.callback import Callback
from service.send.sms import Sms
from service.send.wechat import Wechat


class CreateConnection:
    def __init__(self):
        self.conf = self.load_yaml_conf()

    @staticmethod
    def load_yaml_conf():
        host_name = socket.gethostname()
        conf_path = 'conf/dev.yaml'
        if host_name == 'PRO':
            conf_path = 'conf/pro.yaml'

        with open(conf_path, 'r') as f:
            # return yaml_load(f)
            return safe_load(f)

    async def get_mq_queue(self):
        channel_pool = await mq_server.connection_queue(self.conf)
        return channel_pool

    async def get_mysql_pool(self):
        mysql_pool = await mysql_server.create_mysql_pool(self.conf)
        return mysql_pool

    async def get_redis_pool(self):
        redis_pool = await redis_server.create_redis_pool(self.conf)
        return redis_pool


class EventLoop:
    def __init__(self):
        self.connection = CreateConnection()

    async def get_conn(self):
        channel_pool = await self.connection.get_mq_queue()

        mysql_pool = await self.connection.get_mysql_pool()

        redis_pool = await self.connection.get_redis_pool()

        return channel_pool, mysql_pool, redis_pool

    async def start(self):
        channel_pool, mysql_pool, redis_pool = await self.get_conn()
        while True:
            # pull rabbitmq message > service.send
            mq_data = await mq_server.consume(channel_pool, self.connection.conf)

            for send in (Callback, Sms, Wechat):
                await send(mq_data, mysql_pool, redis_pool).run()


def event_run():
    event_loop = EventLoop()
    asyncio.run(event_loop.start())


def create_process_pool():
    event_run()

    # cpu core > max_workers
    # max_workers = cpu_count()
    # pool = ProcessPoolExecutor(max_workers=max_workers)
    #
    # for i in range(max_workers):
    #     pool.submit(event_run)
