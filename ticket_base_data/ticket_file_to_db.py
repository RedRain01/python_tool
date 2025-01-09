import os
import threading
import time
import mysql.connector  # 导入 MySQL 连接库（Doris 与 MySQL 兼容）
from concurrent.futures import ThreadPoolExecutor

# 设置 Doris 数据库连接配置
DORIS_HOST = '192.168.0.104'
DORIS_PORT = 9030  # Doris 默认端口，可能因配置不同而不同
DORIS_USER = 'root'
DORIS_PASSWORD = 'why123'
DORIS_DB = 'demo'


# 获取 Doris 数据库连接
def get_db_connection():
    conn = mysql.connector.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        database=DORIS_DB
    )
    return conn


# 插入数据到 ticket_file 表
def insert_ticket_data(order_date, order_code, flag):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = '''INSERT INTO ticket_file (order_date, order_code)
               VALUES (%s, %s)'''
    cursor.execute(query, (order_date, order_code))
    conn.commit()
    cursor.close()
    conn.close()


# 处理文件夹（文件夹插入一条数据）
def process_folder(folder_name):
    insert_ticket_data(folder_name, None, 0)


# 处理 CSV 文件（文件插入一条数据）
def process_csv_file(folder_name, csv_file):
    order_code = os.path.splitext(csv_file)[0]  # 获取文件名（不带扩展名）
    insert_ticket_data(folder_name, order_code, 0)


# 处理每个文件夹
def process_directory(directory):
    for folder_name in os.listdir(directory):
        folder_path = os.path.join(directory, folder_name)

        if os.path.isdir(folder_path):  # 如果是文件夹
            # 先处理文件夹本身
            process_folder(folder_name)

            # 然后处理文件夹中的所有 CSV 文件
            for file_name in os.listdir(folder_path):
                if file_name.endswith('.csv'):  # 只处理 CSV 文件
                    process_csv_file(folder_name, file_name)


# 主函数
def main():

    base_dir = '/mnt/data3/ticket_date/2024_csv'  # 根目录
    start_time = time.time()

    # 使用线程池来并行处理每个文件夹
    with ThreadPoolExecutor(max_workers=10) as executor:  # 你可以根据需要调整 max_workers
        process_directory(base_dir)


    end_time = time.time()
    print(f"处理完成，总耗时: {end_time - start_time}秒")


if __name__ == "__main__":
    main()
