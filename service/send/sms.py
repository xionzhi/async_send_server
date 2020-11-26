from service.send_base import BaseSend


class Sms(BaseSend):
    def __init__(self, mq_data, mysql_pool, redis_pool):
        super().__init__(mq_data, mysql_pool, redis_pool)

    async def run(self):
        print('sms > ', self.mq_data)
