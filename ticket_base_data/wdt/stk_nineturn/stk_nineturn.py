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


if __name__ == '__main__':
    # 日期范围20220801
    pro = ts.pro_api()
    ts.set_token('e8778dbd9b06d8fcee802d6d60d80ce706d2863979b551aad68845cf')
    df = pro.stk_nineturn(ts_code='600340.SH', freq='60min',
                          fields='ts_code,trade_date,freq,up_count,down_count,nine_up_turn,nine_down_turn')
    print(df)

