import mysql.connector
from mysql.connector import pooling
from concurrent.futures import ThreadPoolExecutor
import random
import string
import time
from datetime import datetime
from ticket import Ticket
import tushare as ts



# 配置 MySQL 连接池
class MySQLConnectionPool:
    def __init__(self, host, user, password, database, pool_size=5):
        self.pool = pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=pool_size,  # 连接池大小
            host=host,
            user=user,
            password=password,
            database=database
        )

    def get_connection(self):
        return self.pool.get_connection()


# 插入数据到数据库的函数
def insert_student_data(connection,Ticket):
    cursor = connection.cursor()

    query = (
        "INSERT INTO tickts (order_date,order_time,order_price,order_change,order_volume,order_amount,order_type, order_code,order_symbol,ticket_name,ticket_area,industry,list_date)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s)")
    params = (Ticket.order_date, Ticket.order_time, Ticket.order_price, Ticket.order_change, Ticket.order_volume,
              Ticket.order_amount, Ticket.order_type, Ticket.order_code, Ticket.order_symbol, Ticket.ticket_name,
              Ticket.ticket_area, Ticket.industry, Ticket.list_date)

    cursor.execute(query,params)
    connection.commit()
    cursor.close()


# 生成随机学生数据
def generate_student_data():
    student_id = random.randint(1000, 9999)
    name = ''.join(random.choices(string.ascii_letters, k=8))
    age = random.randint(18, 25)
    return student_id, name, age


# 线程池任务
def add_student_task(pool,ts_code,symbol,name,area,industry,list_date,):
    df = ts.realtime_tick(ts_code=name)
    current_date = datetime.now().strftime("%Y-%m-%d")
    for iterrow in df.iterrows():
        ticket = Ticket(current_date,
                        iterrow[1]['TIME'],
                        iterrow[1]['PRICE'],
                        iterrow[1]['CHANGE'],
                        iterrow[1]['VOLUME'],
                        iterrow[1]['AMOUNT'],
                        iterrow[1]['TYPE'],
                        ts_code,
                        symbol,
                        name,
                        area,
                        industry,
                        list_date
                        )
        connection = pool.get_connection()
        student_id, name, age = generate_student_data()
        insert_student_data(connection, ticket)
        connection.close()




# 主函数
def main():
    # 配置数据库连接池
    pool = MySQLConnectionPool(
        host='localhost',    # MySQL 地址
        user='root',         # MySQL 用户名
        password='admin', # MySQL 密码
        database='test',  # 数据库名称
        pool_size=10         # 连接池大小
    )

    # 使用线程池并发插入数据
    start_time = time.time()  # 记录开始时间
    with ThreadPoolExecutor(max_workers=10) as executor:
        pro = ts.pro_api()
        data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        # 创建任务列表
        tasks = [
            (pool, row['ts_code'], row['symbol'], row['name'], row['area'], row['industry'], row['list_date'])
            for _, row in data.iterrows()
        ]

        # 提交任务
        for task in tasks:
            executor.submit(add_student_task, *task)
    print(f"Time taken for inserting 50 records: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()

