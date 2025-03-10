# app.py
from mysql.connector import Error
import mysql.connector

if __name__ == '__main__':
    try:
        # 连接到 DorisDB 数据库
        with mysql.connector.connect(
                host='192.168.0.104',  # 替换为你的 DorisDB 主机地址
                port=9030,  # 替换为你的 DorisDB 端口号
                user='root',  # 替换为你的用户名
                password='why123',  # 替换为你的密码
                database='demo',  # 替换为你的数据库名称
        ) as conn:
            # 创建一个游标对象
            with conn.cursor() as cursor:
                # 执行查询获取 ticket 表的所有数据
                cursor.execute("SELECT * FROM ticket_base_list")                # 获取所有查询结果
                rows = cursor.fetchall()
                #
                num= 0
                for row in rows:
                    print("all--num:" + str(rows.__len__()) + "---run--num:")
    except Error as e:
        print(f"数据库操作出现错误: {e}")
