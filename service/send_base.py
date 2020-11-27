import aiohttp
from datetime import datetime

from service.abstract_base import AbstractBase


class BaseSend(AbstractBase):
    def __init__(self, mq_data, mysql_pool, redis_pool):
        self.mq_data = mq_data
        self.mysql_pool = mysql_pool
        self.redis_pool = redis_pool

    async def redis_executor(self, command, *args):
        return await self.redis_pool.execute(command, *args)

    async def mysql_executor(self, sql, data, many=False):
        # many=True > fetchall
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, data)
                if many is True:
                    return await cur.fetchall()
                return await cur.fetchone()

    @staticmethod
    async def http_executor(url, data=None, method='POST'):
        async with aiohttp.ClientSession() as session:
            if method == 'POST':
                async with session.post(url, json=data, timeout=5) as response:
                    resp = await response.json()
                    return resp
            elif method == 'GET':
                async with session.get(url, timeout=5) as response:
                    resp = await response.json()
                    return resp
            elif method == 'PATCH':
                pass
            elif method == 'PUT':
                pass
            elif method == 'DELETE':
                pass

    async def run(self):
        raise NotImplementedError("Run function hasn't been implemented yet.")
