import uvloop
import asyncio

from factory import create_process_pool

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == '__main__':
    create_process_pool()
