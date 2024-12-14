import mysql.connector
from datetime import datetime


def select_all_ip():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='test'
    )
    cursor = connection.cursor()
    # 查询 IP 是否存在
    query_select = "SELECT ip FROM ip_list"
    cursor.execute(query_select)
    results = cursor.fetchall()
    ip_list = [result[0] for result in results]
    connection.commit()
    cursor.close()
    connection.close()
    return ip_list
def insert_ip(ip):
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='test'
    )
    cursor = connection.cursor()

    # 查询 IP 是否存在
    query_select = "SELECT num FROM ip_list WHERE ip = %s"
    cursor.execute(query_select, (ip,))
    result = cursor.fetchone()

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if result:
        # 如果 IP 存在，更新 num 字段 +1
        num = result[0] + 1
        query_update = "UPDATE ip_list SET num = %s, update_time = %s WHERE ip = %s"
        cursor.execute(query_update, (num, current_date, ip))
    else:
        # 如果 IP 不存在，插入新记录，num 初始为 1
        query_insert = "INSERT INTO ip_list (ip, create_time,update_time, num) VALUES (%s, %s,%s, %s)"
        cursor.execute(query_insert, (ip, current_date,current_date, 1))

    connection.commit()
    cursor.close()
    connection.close()

def insert_tickt_add_error(code,page,msg):
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='test'
    )
    cursor = connection.cursor()

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query_insert = "INSERT INTO ticket_add_error (code, time,msg, page) VALUES (%s, %s,%s, %s)"
    cursor.execute(query_insert, (code, current_date,msg, page))
    connection.commit()
    cursor.close()
    connection.close()


