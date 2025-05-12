import tushare as ts
import mysql.connector
from mysql.connector import Error


def save_to_dorisdb(df, host, port, user, password, database, selected_columns):
    try:
        # 打印 DataFrame 的列名
        print("DataFrame 的列名:", df.columns)

        # 打印 DataFrame 的第一行数据
        print("DataFrame 的第一行数据:")
        print(df.iloc[0])

        # 选择指定的列
        df = df[selected_columns]
        print(f"选择的列: {selected_columns}")
        print("选择后的 DataFrame 的第一行数据:")
        df = df.fillna(0)  # 将所有 NaN 值替换为 0

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
                INSERT INTO ticket_base_list (
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
    ts.set_token('f5c0dd50765d8f388d7dd63aaae06ad8aa19c341ed764fad1d8d7dca')
    pro = ts.pro_api()
    df = pro.stock_basic(exchange='', list_status='L')

    # 指定要插入的列名
    # 指定要插入的列名
    selected_columns =['ts_code', 'symbol', 'name', 'area', 'industry',
       'cnspell', 'market', 'list_date', 'act_name', 'act_ent_type']  # 这里是你选择的列名

    # 保存到 DorisDB
    save_to_dorisdb(df,
                    host='192.168.1.4',  # 替换为你的 DorisDB 主机地址
                    port=9030,  # 替换为你的 DorisDB 端口号
                    user='root',  # 替换为你的用户名
                    password='',  # 替换为你的密码
                    database='demo',  # 替换为你的数据库名称
                    selected_columns=selected_columns  # 传入选择的列名
                    )
