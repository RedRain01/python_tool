from datetime import date
import tushare as ts
from mysql.connector import Error
from datetime import datetime, timedelta
from ticket_base_data.base_db.job_base import  base_job_update,base_create_job
from ticket_base_data.doris_connect.dorise_db import doris_db

def save_to_dorisdb(df):
    try:        # 连接到 DorisDB 数据库
        # 根据选中的列动态生成插入数据的 SQL 语句
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""
                       INSERT INTO demo.wdt_cyq_chips (
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


def split_date_ranges(start_date: str, end_date: str):
    """
    将时间范围按 10 天一组拆分，最后不足 10 天的部分按实际天数处理。
    :param start_date: 开始日期，格式 "YYYY-MM-DD"
    :param end_date: 结束日期，格式 "YYYY-MM-DD"
    :return: 时间范围列表，每个范围是 (start, end)
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    ranges = []
    while start <= end:
        # 计算 10 天后的时间（不能超过 `end`）
        next_end = min(start + timedelta(days=14), end)
        ranges.append((start.strftime("%Y%m%d"), next_end.strftime("%Y%m%d")))

        # 更新 `start` 到 `next_end` 的下一天
        start = next_end + timedelta(days=1)
    return ranges


def split_date_ranges(start_date: str, end_date: str):
    """
    将时间范围按 10 天一组拆分，最后不足 10 天的部分按实际天数处理。

    :param start_date: 开始日期，格式 "YYYY-MM-DD"
    :param end_date: 结束日期，格式 "YYYY-MM-DD"
    :return: 时间范围列表，每个范围是 (start, end)
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    ranges = []

    while start <= end:
        # 计算 10 天后的时间（不能超过 `end`）
        next_end = min(start + timedelta(days=9), end)
        ranges.append((start.strftime("%Y%m%d"), next_end.strftime("%Y%m%d")))

        # 更新 `start` 到 `next_end` 的下一天
        start = next_end + timedelta(days=1)

    return ranges


def main():
    code = "cyq_chips"
    params = ("每日筹码分布", code, date.today(), date.today(), datetime.now(), 0, "", "",)
    result=base_create_job(code, "20250515",  "20250805", params)
    if result.error:
        return
    token = result.data['token']
    id = result.data['id']
    start_date = result.data['start_date']
    end_date = result.data['end_date']
    try:
        ten_day_dic = split_date_ranges(convert_to_string(start_date), convert_to_string(end_date))
        rows = doris_db.execute_query("SELECT * FROM ticket_base_list", None, False)
        ts.set_token(token)
        pro = ts.pro_api()
        #
        num = 0
        for row in rows:
            num += 1
            datenum = 0
            for e in ten_day_dic:
                print(f"all--num:{len(rows)}---run--num:{num}")
                print(f"--datenum:{datenum}")
                start_date = e[0]
                end_date = e[1]
                datenum += 1
                try:
                    ts_code = row['ts_code']
                    # rows_old =doris_db.execute(
                    #     "SELECT * FROM wdt_cyq_chips WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s",
                    #     (ts_code, start_date, end_date))
                    # if len(rows_old) > 0:
                    #     print(f"数据已存在，跳过")
                    #     continue
                    df = pro.cyq_chips(ts_code=ts_code, start_date=start_date, end_date=end_date)
                    # df = pro.cyq_chips(ts_code='000050.SZ', start_date='20250206', end_date='20250209')
                except Exception as e:
                    print(f"error获取 {row} {start_date} {end_date} 筹码分布数据失败：{e}")
                    continue
                if df is not None and not df.empty:
                    save_to_dorisdb(df)
                else:
                    continue
        result = base_job_update(id, 9)
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

