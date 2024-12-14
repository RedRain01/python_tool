import os
import requests
from concurrent.futures import ThreadPoolExecutor

# 配置参数
DORIS_HOST = "http://192.168.0.104:8030"
DATABASE = "demo"
TABLE = "ticket"
USERNAME = "admin"
PASSWORD = "why123"
CSV_DIR = "D:/ticket_test/2024-01-02"

def stream_load(file_path):
    if os.path.exists(CSV_DIR):
        print("路径存在")
    else:
        print("路径不存在")
    try:
        url = f"{DORIS_HOST}/api/{DATABASE}/{TABLE}/_stream_load"
        headers = {
            "label": os.path.basename(file_path),
            "column_separator": ",",
            "Expect": "100-continue",
            "skip_lines": "1",  # 跳过第一行
            "columns": "tran_id,order_time,price,volume,sale_volume, buy_volume,type,sale_order_id,sale_order_price,buy_order_id,buy_order_price,order_date,order_code,order_dc"  # 忽略 extra_field
        }
        with open(file_path, 'rb') as f:
            response = requests.put(url, data=f, headers=headers, auth=(USERNAME, PASSWORD))
        if response.status_code == 200:
            print(f"Success: {file_path}")
        else:
            print(f"Failed: {file_path}, {response.text}")
    except Exception as e:
        print(f"Error: {file_path}, {e}")
        pass


if __name__ == '__main__':
    # 多线程并发
    with ThreadPoolExecutor(max_workers=1) as executor:
        for csv_file in os.listdir(CSV_DIR):
            executor.submit(stream_load, os.path.join(CSV_DIR, csv_file))

