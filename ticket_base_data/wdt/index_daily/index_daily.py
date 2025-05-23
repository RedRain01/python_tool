from datetime import date
import time
import tushare as ts
import mysql.connector
from io import IOBase
from mysql.connector import Error
from datetime import datetime, timedelta
from ticket_base_data.doris_connect.dorise_db import doris_db
from ticket_base_data.base_db.job_base import  base_job_update,base_create_job


def save_to_dorisdb(df):
    try:
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""
                        INSERT INTO demo.wdt_index_daily (
                            {', '.join(columns)}
                        ) VALUES (
                            {placeholders}
                        )
                        """
        # 将 DataFrame 中的数据插入到表中，使用批量插入提高性能
        data_to_insert = [tuple(row) for row in df.values]
        doris_db.execute_many(insert_query, data_to_insert)
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
    result=base_create_job(code, "2020-01-01",  "2025-05-13", params)
    token = result.data['token']
    id = result.data['id']
    start_date = result.data['start_date']
    end_date = result.data['end_date']
    if result.error:
        return
    try:
        rows = doris_db.execute_query("SELECT * FROM ticket_base_list", None, False)
        ts.set_token(token)
        pro = ts.pro_api()
        #
        num = 0
        for row in rows:
            num += 1
            print(f"all--num:{len(rows)}---run--num:{num}")
            #   df = pro.index_daily(ts_code='000036.SZ', start_date='20180101', end_date='20181010')
            start_date = convert_to_string(start_date)
            end_date = convert_to_string(end_date)
            df = pro.daily(ts_code=row['ts_code'], start_date=start_date, end_date=end_date)
            time.sleep(3)
            if df is not None and not df.empty:
                if jobflag:
                    jobflag = False
                    base_job_update(id, 0)
                save_to_dorisdb(df )
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

