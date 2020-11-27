from service.send_base import BaseSend


class Callback(BaseSend):
    def __init__(self, mq_data, mysql_pool, redis_pool):
        super().__init__(mq_data, mysql_pool, redis_pool)

    async def query_user_settings(self, user_id: int) -> str:
        sql, data = f'SELECT id, u_token ' \
                    f'FROM dev.aio_test ' \
                    f'WHERE id = %s ' \
                    f'AND u_status != 0;', (user_id, )

        user_settings = await self.mysql_executor(sql, data, many=False)

        if user_settings is not None:
            _, u_token = user_settings
            return u_token

    async def query_public_key(self, user_id: int) -> str:
        public_key = await self.redis_executor('get', f'user_public_key:{user_id}')
        return public_key

    async def run(self):
        """
        回调处理
        :return:
        """
        u_id = self.mq_data.get('user_id')

        u_token = await self.query_user_settings(u_id)
        if u_token is None:
            return

        u_public_key = await self.query_public_key(u_id)
        if u_public_key is None:
            return

        print(u_token, u_public_key)
