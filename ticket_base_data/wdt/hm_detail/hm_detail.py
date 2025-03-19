import time
from datetime import date
import tushare as ts
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from ticket_base_data.base_db.job_base import  base_job_update,base_create_job



def save_to_dorisdb(df, host, port, user, password, database, selected_columns):
    try:
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
    code = "hm_detail"
    params = ("游资每日明细", code, datetime.today().today(), datetime.today().today(), datetime.now(), 0, "1", "2",)
    result=base_create_job(code, "20250315",  "20250316", params)
    if result.error:
        return
    token = result.data['token']
    id = result.data['id']
    start_date = result.data['start_date']
    end_date = result.data['end_date']
    dates = generate_date_range(convert_to_string(start_date), convert_to_string(end_date))
    pro = ts.pro_api()
    selected_columns = ['trade_date', 'ts_code', 'ts_name', 'buy_amount', 'sell_amount',
                        'net_amount', 'hm_name', 'hm_orgs', 'tag']  # 这里是你选择的列名
    ts.set_token(token)
    for date in dates:
        time.sleep(1)
        df = pro.hm_detail(trade_date=date,
                           fields='trade_date, ts_code, ts_name, buy_amount, sell_amount,net_amount, hm_name, hm_orgs, tag')
        if df is not None and not df.empty:
            save_to_dorisdb(df,
                            host='192.168.0.106',  # 替换为你的 DorisDB 主机地址
                            port=9030,  # 替换为你的 DorisDB 端口号
                            user='root',  # 替换为你的用户名
                            password='why123',  # 替换为你的密码
                            database='demo',  # 替换为你的数据库名称
                            selected_columns=selected_columns  # 传入选择的列名
                            )
    base_job_update(id)
    print("任务执行完成")
    print(f"任务更新结果：{result}")
if __name__ == '__main__':
    main()


