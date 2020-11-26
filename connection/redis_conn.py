import aioredis


class RedisServer:
    @staticmethod
    async def create_redis_pool(conf: dict):
        """
        REDIS_HOST: 127.0.0.1
        REDIS_PORT: 6379
        REDIS_DB: 0
        :param conf:
        :return:
        """
        pool = await aioredis.create_pool(address=f'redis://{conf.get("REDIS_HOST")}:{conf.get("REDIS_PORT")}',
                                          db=conf.get('REDIS_DB'),
                                          encoding='utf-8',
                                          minsize=10,
                                          maxsize=30)

        return pool


redis_server = RedisServer()
