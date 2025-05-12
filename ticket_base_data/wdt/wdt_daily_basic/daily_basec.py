import time
from datetime import date
import tushare as ts
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from ticket_base_data.doris_connect.dorise_db import doris_db
from ticket_base_data.base_db.job_base import  base_job_update,base_create_job



def save_to_dorisdb(df):
    try:
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""
                       INSERT INTO demo.wdt_daily_basic (
                           {', '.join(columns)}
                       ) VALUES (
                           {placeholders}
                       )
                       """
        # 将 DataFrame 中的数据插入到表中，使用批量插入提高性能
        data_to_insert = [tuple(row) for row in df.values]
        doris_db.execute_many(insert_query, data_to_insert)
    except Error as e:
        print(f"数据库操作出现错误: {e}")

def convert_to_string(value):
    if isinstance(value, date):  # 判断是否是 date 类型
        return value.strftime("%Y%m%d")  # 转换为字符串格式 "YYYY-MM-DD"
    return value  # 如果不是 date 类型，则原样返回
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
    code = "daily_basic"
    params = ("每日指标", code, datetime.today().today(), datetime.today().today(), datetime.now(), 0, "1", "2",)
    result=base_create_job(code, "20250101",  "20250429", params)
    if result.error:
        return
    token = result.data['token']
    id = result.data['id']
    start_date = result.data['start_date']
    end_date = result.data['end_date']
    dates = generate_date_range(convert_to_string(start_date), convert_to_string(end_date))
    pro = ts.pro_api()
    ts.set_token(token)
    for date in dates:
        time.sleep(1)
        df = pro.daily_basic(ts_code='', trade_date=date)
        if df is not None and not df.empty:
            df = df.fillna(0.0)
            save_to_dorisdb(df)
    base_job_update(id,9)
    print("任务执行完成")
    print(f"任务更新结果：{result}")
if __name__ == '__main__':
    main()


