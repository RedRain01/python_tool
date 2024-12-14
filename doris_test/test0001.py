import requests
import json



if __name__ == '__main__':

    # Doris集群的Stream Load接口URL
    url = 'http://192.168.0.104:8030/api/demo/ticket/_stream_load'

    # 认证信息 (如果需要)
    auth = ('admin','why123')

    # 需要上传的文件路径
    file_path = r'D:\ticket_test\2024-01-02\000001.csv'

    # 设置请求的headers
    # headers = {
    #     "Content-Type": "application/octet-stream",  # 传输二进制数据
    #     'columns': 'tran_id,order_time,price,volume,sale_volume,buy_volume,type,sale_order_id,sale_order_price,buy_order_id,buy_order_price,order_date,order_code,order_dc',
    #     "Expect": "",  # 避免出现 '100-continue' 问题
    #     'strict_mode': 'false',
    #     "label": "stream_load_1"  # 每次上传需要指定一个唯一的label，避免重复
    # }

    headers = {
        'column_separator': ',',
        'columns': 'tran_id,order_time,price,volume,sale_volume,buy_volume,type,sale_order_id,sale_order_price,buy_order_id,buy_order_price,order_date,order_code,order_dc',
        'max_filter_ratio': '0.8',
        'strict_mode': 'false',
        'skip_header': '1',
        'Expect': ''  # 这个可以显式禁用 100-Continue 机制
    }

    # 要上传的数据文件
    with open(file_path, 'rb') as file:
        # 发起POST请求
        response = requests.put(url, headers=headers, data=file, auth=auth)

        # 打印响应结果
        if response.status_code == 200:
            print(response.text)
        else:
            print("Stream Load 失败")
            print(response.status_code, response.text)
