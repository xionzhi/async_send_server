import asyncio
import json

import aio_pika
from aio_pika.pool import Pool


class MQServer:
    @staticmethod
    async def connection_queue(conf: dict):
        """
        RABBITMQ_HOST: 127.0.0.1
        RABBITMQ_USER: guest
        RABBITMQ_PASSWORD: guest
        RABBITMQ_PORT: 5672
        RABBITMQ_VHOST: /
        :param conf:
        :return: channel_pool
        """

        async def get_connection() -> aio_pika.connect_robust:
            return await aio_pika.connect_robust(url=None,
                                                 loop=asyncio.get_event_loop(),
                                                 host=conf.get('RABBITMQ_HOST'),
                                                 port=conf.get('RABBITMQ_PORT'),
                                                 login=conf.get('RABBITMQ_USER'),
                                                 password=conf.get('RABBITMQ_PASSWORD'),
                                                 virtualhost=conf.get('RABBITMQ_VHOST'))

        connection_pool = Pool(get_connection,
                               max_size=conf.get('RABBITMQ_SIZE') or 100,
                               loop=asyncio.get_event_loop())

        async def get_channel() -> aio_pika.Channel:
            async with connection_pool.acquire() as connection:
                return await connection.channel()

        channel_pool = Pool(get_channel,
                            max_size=conf.get('RABBITMQ_SIZE') or 100,
                            loop=asyncio.get_event_loop())

        return channel_pool

    @staticmethod
    async def consume(channel_pool, conf: dict) -> dict:
        """
        :param channel_pool:
        :param conf:
        :return:
        """
        async with channel_pool.acquire() as channel:
            # set channel qos
            # https://aio-pika.readthedocs.io/en/latest/rabbitmq-tutorial/2-work-queues.html#fair-dispatch
            await channel.set_qos(prefetch_count=conf.get('RABBITMQ_PREFETCH_COUNT') or 1)

            queue = await channel.declare_queue(conf.get('RABBITMQ_QUEUE'),
                                                durable=False,
                                                auto_delete=False)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    info_data = json.loads((message.body.decode()))
                    await message.ack()

                    return info_data


# Singleton rabbitmq
mq_server = MQServer()
