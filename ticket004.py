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
TICK_COLUMNS = ['TIME', 'PRICE', 'CHANGE', 'VOLUME', 'AMOUNT', 'TYPE']
TODAY_TICK_COLUMNS = ['TIME', 'PRICE', 'PCHANGE', 'CHANGE', 'VOLUME', 'AMOUNT', 'TYPE']
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
}
# 数据库插入函数
def insert_data_to_db(ticket_dict):
    data=ticket_dict['data']
    name=ticket_dict['code']
    page=ticket_dict['page']
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='test'
    )
    cursor = connection.cursor()
    query = (
        "INSERT INTO tickts20241118 (id,order_date,order_time,order_price,order_change,order_volume,order_amount,order_type, order_code,order_symbol,"
        "ticket_name,ticket_area,industry,list_date,page_num,page_var)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s,%s,%s, %s,%s, %s)")
    current_date = datetime.now().strftime("%Y-%m-%d")
    varr=data_dict.get(name)
    for iterrow in data.iterrows():
        ticket = Ticket(
                        varr.ts_code+"_"+current_date+"_"+iterrow[1]['TIME'],
                        current_date,
                        iterrow[1]['TIME'],
                        iterrow[1]['PRICE'],
                        iterrow[1]['CHANGE'],
                        iterrow[1]['VOLUME'],
                        iterrow[1]['AMOUNT'],
                        iterrow[1]['TYPE'],
                        varr.ts_code,
                        varr.symbol,
                        varr.name ,
                        varr.area,
                        varr.industry,
                        varr.list_date,
                        page,
                        varr.ts_code+"_"+str(page)
                        )
        params = (ticket.id, ticket.order_date, ticket.order_time, ticket.order_price, ticket.order_change, ticket.order_volume,
                  ticket.order_amount, ticket.order_type, ticket.order_code, ticket.order_symbol, ticket.ticket_name,
                  ticket.ticket_area, ticket.industry, ticket.list_date, ticket.page_num, ticket.page_var)
        cursor.execute(query, params)
    connection.commit()
    cursor.close()
    connection.close()


async def get_stock_tx_a_divide_amount(symbol: str = "sz000001", page: Optional[int] = None) -> pd.DataFrame:
    big_df = pd.DataFrame()
    warnings.warn("正在下载数据，请稍等")
    async with aiohttp.ClientSession() as session:
        symbols = str(symbol).lower().split(".")
        symbol = f"{symbols[1]}{symbols[0]}"
        proxy = get_proxy_from_pool_new()  # 获取代理
        url = "http://stock.gtimg.cn/data/index.php"
        params = {
            "appn": "detail",
            "action": "data",
            "c": symbol,
            "p": page,
        }
        try:
            async with session.get(url, headers=headers, proxy=proxy, params=params) as response:
                text_data = await response.text()
                temp_df = (
                    pd.DataFrame(eval(text_data[text_data.find("["):])[1].split("|"))
                    .iloc[:, 0]
                    .str.split("/", expand=True)
                )
                big_df = pd.concat([big_df, temp_df], ignore_index=True)
                if not big_df.empty:
                    print("查询到数据-----------------------------------")
                    big_df = big_df.iloc[:, 1:].copy()
                    # big_df.columns = ["成交时间", "成交价格", "价格变动", "成交量", "成交金额", "性质"]
                    big_df.columns = rtq_vars.TICK_COLUMNS

                    big_df.reset_index(drop=True, inplace=True)
                    property_map = {
                        "S": "卖盘",
                        "B": "买盘",
                        "M": "中性盘",
                    }
                    # big_df["性质"] = big_df["性质"].map(property_map)
                    big_df["TYPE"] = big_df["TYPE"].map(property_map)
                    big_df = big_df.astype(
                        {
                            "TIME": str,
                            "PRICE": float,
                            "CHANGE": float,
                            "VOLUME": int,
                            "AMOUNT": int,
                            "TYPE": str,
                        }
                    )
                    ticket_dict = {
                        'code': symbol,
                        'page': page,
                        'data': big_df
                    }
                return big_df
                print(f"ID:  - Proxy: {proxy} - Response:")
        except Exception as e:
            insert_tickt_add_error(symbol,page, str(e))
            print(f"-------------------------- {proxy}. Error: {e}")
    try:
            print("查询股票数据成功-----------", page)
    except  Exception as e:
        print("查询股票数据失败-----------",e)
        time.sleep(1)
    return big_df
#code page
def fetch_ticket_task(ticket_queue, db_queue):
    while True:
        try:
            if not ticket_queue.empty():
                item = ticket_queue.get()
                code = item['code']
                page = item['page']
                data = realtime_tick(ts_code=code, src='', page_count=page)
                if data is None:
                    ticket_queue.put(item)
                if len(data) >69:
                    ticket = {
                        'code': code,
                        'page': page+1
                    }
                    ticket_queue.put(ticket)
                ticket_dict = {
                    'code': code,
                    'page': page,
                    'data': data
                }
                db_queue.put(ticket_dict)
                print(f"正在消费: {item}")
                time.sleep(1)
            else:
                time.sleep(1)  # 如果队列为空，稍微等待一下
        except Exception as e:
            print(f"拉取数据失败------------------------ {e}: {e}")
            time.sleep(2)
            continue

def sink_ticket_task(db_queue):
    while True:
        try:
            if not db_queue.empty():
                insert_data_to_db(db_queue.get())
            else:
                print("队列为空----------------")
                time.sleep(3)  # 如果队列为空，稍微等待一下
        except Exception as e:
            print(f"拉取数据失败----111-------------------- {e}: {e}")
            time.sleep(2)
            continue


# 将查询结果放入队列

# 定义插入数据的任务
def insert_db_task(queue):
    while True:
        data = queue.get()  # 从队列中获取数据
        if data is None:  # 检查是否结束
            break
        insert_data_to_db(data)
        queue.task_done()


def pull_ticket_data(data_queue: Queue,db_queue:Queue):
    loop = asyncio.new_event_loop()  # 每个线程创建独立的事件循环
    asyncio.set_event_loop(loop)  # 设置当前线程的事件循环
    while not data_queue.empty():
        item = data_queue.get()
        code = item['code']
        page = item['page']
        result = loop.run_until_complete(get_stock_tx_a_divide_amount(code,page))
        ticket_dict = {
            'code': code,
            'page': page,
            'data': result
        }
        db_queue.put(ticket_dict)
        # 运行异步HTTP请求
        data_queue.task_done()  # 标记任务完成
def get_ticket_data(queue_ticket_task, queue_add_db_task):
    # 消费拉取队列
    with ThreadPoolExecutor(max_workers=300) as executor:
        for _ in range(300):
            executor.submit(pull_ticket_data, queue_ticket_task, queue_add_db_task)

def sink_ticket_data( queue_add_db_task):
    # 消费入库队列
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 启动多个消费者线程
        for _ in range(10):
            executor.submit(sink_ticket_task, queue_add_db_task)
def main():
    try:
        queue_ticket_task = Queue(maxsize=5500)
        queue_add_db_task = Queue(maxsize=100000)
    except Exception as e:
        print(f"创建队列失败- {e}: {e}")
        pass
    pro = ts.pro_api()
    data1 = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    global data_dict  # 声明使用全局变量
    data_dict = {row.ts_code: row for row in data1.itertuples(index=False)}  # 使用 itertuples() 生成字典
    for item in data1.itertuples(index=False):
        queue_ticket_task.put({'code': item.ts_code, 'page': 1})

    thread1 = threading.Thread(target=get_ticket_data, args=(queue_ticket_task, queue_add_db_task))
    thread2 = threading.Thread(target=sink_ticket_data, args=(queue_add_db_task,))

    thread1.start()  # 启动第一个线程
    thread2.start()  # 启动第二个线程

    thread1.join()   # 等待第一个线程完成
    thread2.join()   # 等待第二个线程完成



    print("All data has been fetched and inserted.")

if __name__ == '__main__':
    main()
    # df = realtime_tick(ts_code="601727.SH", src="tx", page_count=1)
    # print(help(realtime_tick))
    # print(df)
