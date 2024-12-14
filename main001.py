import requests
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from ticket import Ticket
from ticketmain import realtime_tick
import tushare as ts


data_dict = {}
# 模拟 HTTP 请求获取数据
def fetch_data(element):
    try:
        df = realtime_tick(ts_code=element.ts_code)
    except Exception as e:
        print(f"Failed to fetch data for {var}: {e}")
        pass
    return df  # 假设返回的是 JSON 格式


# 数据库插入函数
def insert_data_to_db(ticket_dict):
    data=ticket_dict['value']
    name=ticket_dict['key']
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='test'
    )
    cursor = connection.cursor()
    query = (
        "INSERT INTO tickts20241115 (order_date,order_time,order_price,order_change,order_volume,order_amount,order_type, order_code,order_symbol,ticket_name,ticket_area,industry,list_date)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s)")
    current_date = datetime.now().strftime("%Y-%m-%d")
    varr=data_dict.get(name)
    aaaa=varr.ts_code
    print()
    for iterrow in data.iterrows():
        ticket = Ticket(current_date,
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
                        varr.list_date
                        )
        params = (ticket.order_date, ticket.order_time, ticket.order_price, ticket.order_change, ticket.order_volume,
                  ticket.order_amount, ticket.order_type, ticket.order_code, ticket.order_symbol, ticket.ticket_name,
                  ticket.ticket_area, ticket.industry, ticket.list_date)
        cursor.execute(query, params)
    connection.commit()
    cursor.close()
    connection.close()

# 定义查询数据的任务
def fetch_task(element, queue):
    data = fetch_data(element)
    ticket_dict={
        'key':element.ts_code,
        'value':data
    }
    queue.put(ticket_dict)  # 将查询结果放入队列

# 定义插入数据的任务
def insert_task(queue):
    while True:
        data = queue.get()  # 从队列中获取数据
        if data is None:  # 检查是否结束
            break
        insert_data_to_db(data)
        queue.task_done()

def main():
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    global data_dict  # 声明使用全局变量
    data_dict = {row.ts_code: row for row in data.itertuples(index=False)}  # 使用 itertuples() 生成字典
    queue = Queue(maxsize=200)  # 队列最大容量设置为 10
    insert_threads = []
    for _ in range(20):  # 创建 5 个插入线程
        t = threading.Thread(target=insert_task, args=(queue,))
        t.start()
        insert_threads.append(t)

    # 启动 HTTP 查询线程池
    with ThreadPoolExecutor(max_workers=40) as fetch_executor:
        for element in data.itertuples() :  # 使用 itertup
            fetch_executor.submit(fetch_task, element, queue)

    # 等待所有 HTTP 查询任务完成
    fetch_executor.shutdown(wait=True)

    # 在队列中放入结束标志，通知插入线程退出
    for _ in insert_threads:
        queue.put(None)

    # 等待所有插入线程结束
    for t in insert_threads:
        t.join()

    print("All data has been fetched and inserted.")

if __name__ == "__main__":
    main()
