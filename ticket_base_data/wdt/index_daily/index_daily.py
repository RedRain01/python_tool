from datetime import date
import time
import tushare as ts
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from ticket_base_data.base_db.job_base import  base_job_update,base_create_job


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
                INSERT INTO ticket_index_daily_test (
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


def main():
    code = "index_daily"
    jobflag = True
    params = ("指数日线行情", "index_daily", date.today(), date.today(), datetime.now(), 1, "1", "2",)
    result=base_create_job(code, "2025-03-14",  "2025-03-17", params)
    token = result.data['token']
    id = result.data['id']
    start_date = result.data['start_date']
    end_date = result.data['end_date']
    if result.error:
        return
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
                    cursor.execute("SELECT * FROM ticket_base_list")
                    # cursor.execute("SELECT ts_code FROM ticket_base_list WHERE ts_code NOT IN ( SELECT ts_code  FROM ticket_index_daily )")
                    # 获取所有查询结果
                    rows = cursor.fetchall()
                    ts.set_token(token)
                    pro = ts.pro_api()
                    #
                    num = 0
                    for row in rows:
                        num += 1
                        if num ==100:
                            break
                        print(f"all--num:{len(rows)}---run--num:{num}")
                        #   df = pro.index_daily(ts_code='000036.SZ', start_date='20180101', end_date='20181010')
                        start_date =convert_to_string(start_date)
                        end_date =convert_to_string(end_date)
                        df = pro.daily(ts_code=row[0], start_date=start_date, end_date=end_date)
                        time.sleep(0.03)
                        if df is not None and not df.empty:
                            if jobflag:
                                jobflag = False
                                base_job_update(id, 0)
                            save_to_dorisdb(df,
                                            host='192.168.0.106',  # 替换为你的 DorisDB 主机地址
                                            port=9030,  # 替换为你的 DorisDB 端口号
                                            user='root',  # 替换为你的用户名
                                            password='why123',  # 替换为你的密码
                                            database='demo'  # 替换为你的数据库名称
                                            )
                        else:
                            continue
                    if not jobflag:
                        base_job_update(id, 9)
                    print(f"任务执行完成:{jobflag}")
                    print(f"任务更新结果：{result}")
    except Error as e:
     print(f"数据库操作出现错误: {e}")
def convert_to_string(value):
    if isinstance(value, date):  # 判断是否是 date 类型
        return value.strftime("%Y%m%d")  # 转换为字符串格式 "YYYY-MM-DD"
    return value  # 如果不是 date 类型，则原样返回
if __name__ == '__main__':
    main()

