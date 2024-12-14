import requests
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
import mysql.connector
from mysql.connector import pooling
from concurrent.futures import ThreadPoolExecutor
import random
import string

import time
import tushare as ts
from datetime import datetime
from redis_client import RedisClient
from ticket import Ticket
from ticketmain001 import realtime_tick
import warnings
import pandas as pd
import requests
from io import StringIO
from tushare.util.verify_token import require_permission
from tushare.util.format_stock_code import format_stock_code
from tushare.stock.rtq_vars import zh_sina_a_stock_cookies, zh_sina_a_stock_headers
import time
from redis_client import RedisClient

from typing import Optional
from tushare.util.form_date import get_current_date
from tushare.stock import rtq_vars
from tushare.util.format_stock_code import symbol_verify
from check_proxy_ip import get_proxy_from_pool_new
from data_management import insert_tickt_add_error
import aiohttp
import asyncio
from aiohttp import ClientSession, ClientTimeout
from aiohttp_socks import ProxyConnector

# 创建全局 Redis 客户端实例
redis = RedisClient(host="192.168.0.103", port=6699, password="why123", db=0)# 模拟 HTTP 请求获取数据
# 数据库插入函数


async def get_stock_tx_a_divide_amount():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("http://httpbin.org/ip") as response:
                data = await response.json()
                if data['origin']:
                    return True
                else:
                    return False
        except Exception as e:
            insert_tickt_add_error("symbol",1, str(e))
    return False
#code page





def pull_ticket_data(data_queue: Queue):
    loop = asyncio.new_event_loop()  # 每个线程创建独立的事件循环
    asyncio.set_event_loop(loop)  # 设置当前线程的事件循环
    while not data_queue.empty():
        item = data_queue.get()
        print("-------------",item)
        result = loop.run_until_complete(get_stock_tx_a_divide_amount())
        if result:
           print("-- --success_count---------", item)
           redis.incr("success_count")
        else:
            print("-----error_count--------",item)
            redis.incr("error_count")
        # 运行异步HTTP请求
        data_queue.task_done()  # 标记任务完成
def get_ticket_data(queue_ticket_task):
    # 消费拉取队列
    with ThreadPoolExecutor(max_workers=100) as executor:
        for _ in range(100):
            executor.submit(pull_ticket_data, queue_ticket_task)

def main():
    try:
        queue_ticket_task = Queue(maxsize=10000)
    except Exception as e:
        print(f"创建队列失败- {e}: {e}")
        pass
    for item in range(10000):
        print(item)
        queue_ticket_task.put(item)
    thread1 = threading.Thread(target=get_ticket_data, args=(queue_ticket_task,))

    thread1.start()  # 启动第一个线程

    thread1.join()   # 等待第一个线程完成



    print("All data has been fetched and inserted.")

if __name__ == '__main__':
    main()
    # df = realtime_tick(ts_code="601727.SH", src="tx", page_count=1)
    # print(help(realtime_tick))
    # print(df)
