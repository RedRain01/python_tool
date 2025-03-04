import time

import tushare as ts
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta


def save_to_dorisdb(df, host, port, user, password, database, selected_columns):
    try:
        # 打印 DataFrame 的列名
        print("DataFrame 的列名:", df.columns)
        # 选择指定的列
        df = df[selected_columns]
        # 连接到 DorisDB 数据库
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
                INSERT INTO ticket_hm_detail (
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
    # 日期范围20220801
    dates = generate_date_range('20250219', '20250222')
    pro = ts.pro_api()
    selected_columns =['trade_date', 'ts_code', 'ts_name', 'buy_amount', 'sell_amount',
       'net_amount', 'hm_name', 'hm_orgs', 'tag']  # 这里是你选择的列名
    ts.set_token('68ad60144da544f022cc31fc973db234319ff2c329278cc79d2156aa')
   # ts.set_token('68ad60144da544f022cc31fc973db234319ff2c329278cc79d2156aa')

    for date in dates:
        time.sleep(1)
        df=pro.hm_detail(trade_date=date,fields='trade_date, ts_code, ts_name, buy_amount, sell_amount,net_amount, hm_name, hm_orgs, tag')
        if df is not None and not df.empty:
            save_to_dorisdb(df,
                            host='192.168.0.104',  # 替换为你的 DorisDB 主机地址
                            port=9030,  # 替换为你的 DorisDB 端口号
                            user='root',  # 替换为你的用户名
                            password='why123',  # 替换为你的密码
                            database='demo',  # 替换为你的数据库名称
                            selected_columns=selected_columns  # 传入选择的列名
                            )

