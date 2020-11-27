from abc import ABC, abstractmethod


class AbstractBase(ABC):
    @abstractmethod
    async def redis_executor(self, command, *args): ...

    @abstractmethod
    async def mysql_executor(self, sql, data): ...

    @abstractmethod
    async def http_executor(self, url, data): ...

    @abstractmethod
    async def run(self): ...
