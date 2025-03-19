import mysql.connector
from mysql.connector import Error

def base_job_query(code):
    try:
        # 连接到 DorisDB 数据库
        with mysql.connector.connect(
                host='192.168.0.106',  # 替换为你的 DorisDB 主机地址
                port=9030,  # 替换为你的 DorisDB 端口号
                user='root',  # 替换为你的用户名
                password='why123',  # 替换为你的密码
                database='demo',  # 替换为你的数据库名称
        ) as conn:
            # 创建一个游标对象
            with conn.cursor() as cursor:
                # 执行查询获取 ticket 表的所有数据
                cursor.execute("SELECT * FROM job_detail WHERE job_code = %s ORDER BY job_time DESC limit 1", (code,))
                # 获取所有查询结果
                rows = cursor.fetchall()
                #如果没有记录或者记录全是已完成的 记录则新增一条记录，如果存在未完成记录则创建一条记录
                return rows

    except Error as e:
        print(f"数据库操作出现错误: {e}")
