from datetime import date
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
                INSERT INTO cyq_perf (
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
    code = "cyq_perf"
    str = "指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情指数日线行情"
    params = ("每日筹码及胜率", code, date.today(), date.today(), datetime.now(), 0, "", "",)
    result=base_create_job(code, "2020-01-01",  "2025-03-10", params)
    if result.error:
        return
    token = result.data['token']
    id = result.data['id']
    start_date = result.data['start_date']
    end_date = result.data['end_date']

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
                    # cursor.execute("SELECT ts_code FROM ticket_base_list WHERE ts_code NOT IN ( SELECT ts_code  FROM ticket_index_daily )")
                    # 获取所有查询结果
                    rows = cursor.fetchall()
                    ts.set_token(token)
                    pro = ts.pro_api()
                    #
                    num = 0
                    for row in rows:
                        num += 1
                        print(f"all--num:{len(rows)}---run--num:{num}")
                        start_date =convert_to_string(start_date)
                        end_date =convert_to_string(end_date)
                        df = pro.cyq_perf(ts_code=row[0], start_date=start_date, end_date=end_date)
                        if df is not None and not df.empty:
                            save_to_dorisdb(df,
                                            host='192.168.0.104',  # 替换为你的 DorisDB 主机地址
                                            port=9030,  # 替换为你的 DorisDB 端口号
                                            user='root',  # 替换为你的用户名
                                            password='why123',  # 替换为你的密码
                                            database='demo'  # 替换为你的数据库名称
                                            )
                        else:
                            continue
                    result=base_job_update(id)
                    print("任务执行完成")
                    print(f"任务更新结果：{result}")
    except Error as e:
     print(f"数据库操作出现错误: {e}")
def convert_to_string(value):
    if isinstance(value, date):  # 判断是否是 date 类型
        return value.strftime("%Y%m%d")  # 转换为字符串格式 "YYYY-MM-DD"
    return value  # 如果不是 date 类型，则原样返回
if __name__ == '__main__':
    main()

