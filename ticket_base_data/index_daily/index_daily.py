import time

import tushare as ts
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta


def save_to_dorisdb(df, host, port, user, password, database):
    try:        # 连接到 DorisDB 数据库
        with mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        ) as conn:
            # 创建一个游标对象
            with conn.cursor() as cursor:
                # 根据选中的列动态生成插入数据的 SQL 语句
                columns = df.columns.tolist()
                placeholders = ', '.join(['%s'] * len(columns))
                insert_query = f"""
                INSERT INTO ticket_index_daily (
                    {', '.join(columns)}
                ) VALUES (
                    {placeholders}
                )
                """
                # 将 DataFrame 中的数据插入到表中，使用批量插入提高性能
                data_to_insert = [tuple(row) for row in df.values]
                cursor.executemany(insert_query, data_to_insert)
                # 提交事务
                conn.commit()

        print("数据已成功保存到 DorisDB 数据库")

    except Error as e:
        print(f"数据库操作出现错误: {e}")


def generate_date_range(start_date, end_date):
    # 将输入的字符串转换为日期对象
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')

    # 创建一个空集合来存储日期
    date_set = set()

    # 使用循环生成日期范围内的所有日期
    while start <= end:
        # 将日期格式化为'YYYYMMDD'并添加到集合中
        date_set.add(start.strftime('%Y%m%d'))
        # 日期增加一天
        start += timedelta(days=1)

    return date_set
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
                cursor.execute("SELECT * FROM ticket_base_list")

                #cursor.execute("SELECT ts_code FROM ticket_base_list WHERE ts_code NOT IN ( SELECT ts_code  FROM ticket_index_daily )")

                # 获取所有查询结果
                rows = cursor.fetchall()
                ts.set_token('68ad60144da544f022cc31fc973db234319ff2c329278cc79d2156aa')
                pro = ts.pro_api()
                #
                num= 0
                for row in rows:
                    num += 1
                    print("all--num:" + str(rows.__len__()) + "---run--num:" + str(num))
                    #   df = pro.index_daily(ts_code='000036.SZ', start_date='20180101', end_date='20181010')
                    df = pro.daily(ts_code=row[0], start_date='20250219', end_date='20250222')
                    if df is not None and not df.empty:
                        save_to_dorisdb(df,
                                        host='192.168.0.104',  # 替换为你的 DorisDB 主机地址
                                        port=9030,  # 替换为你的 DorisDB 端口号
                                        user='root',  # 替换为你的用户名
                                        password='why123',  # 替换为你的密码
                                        database='demo' # 替换为你的数据库名称
                                        )

    except Error as e:
        print(f"数据库操作出现错误: {e}")

    # 日期范围20220801

