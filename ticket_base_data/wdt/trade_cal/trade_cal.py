from ticket_base_data.base_db.job_base import base_dic_query
from ticket_base_data.doris_connect.dorise_db import doris_db

import tushare as ts


def save_to_dorisdb(df):
    try:        # 连接到 DorisDB 数据库
        # 根据选中的列动态生成插入数据的 SQL 语句
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""
                       INSERT INTO demo.wdt_trade_cal (
                           {', '.join(columns)}
                       ) VALUES (
                           {placeholders}
                       )
                       """
        # 将 DataFrame 中的数据插入到表中，使用批量插入提高性能
        data_to_insert = [tuple(row) for row in df.values]
        doris_db.execute_many(insert_query, data_to_insert)
        print("数据已成功保存到 DorisDB 数据库")
    except Exception as e:
        print(f"数据库操作出现错误: {e}")



def main():
    token = base_dic_query("wdt_token")
    ts.set_token(token[0]['dic_value'])
    pro = ts.pro_api()
    df = pro.trade_cal(exchange='', start_date='20180101', end_date='20250606',fields='exchange,cal_date,is_open,pretrade_date')
    save_to_dorisdb(df)
if __name__ == '__main__':
    main()
    print("------------------")