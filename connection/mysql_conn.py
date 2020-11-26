import aiomysql


class MySQLServer:
    @staticmethod
    async def create_mysql_pool(conf: dict):
        """
        MYSQL_HOST: 127.0.0.1
        MYSQL_PASSWORD: 123456
        MYSQL_USER: root
        MYSQL_DB: dev
        MYSQL_PORT: 3306
        :param conf:
        :return:
        """
        pool = await aiomysql.create_pool(host=conf.get('MYSQL_HOST'),
                                          port=conf.get('MYSQL_PORT'),
                                          user=conf.get('MYSQL_USER'),
                                          password=conf.get('MYSQL_PASSWORD'),
                                          db=conf.get('MYSQL_DB'),
                                          echo=True,
                                          minsize=10,
                                          maxsize=30)
        return pool


mysql_server = MySQLServer()
