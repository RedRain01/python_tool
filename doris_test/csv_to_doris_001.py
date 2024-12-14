import requests



if __name__ == '__main__':
    # 多线程并发
    # URL to which the data will be uploaded
    url = 'http://192.168.0.104:8030/api/demo/ticket006/_stream_load'

    # Authentication details
    auth = ('admin', 'why123')

    # Headers
    # headers = {
    #     'column_separator': ',',
    #     'columns': 'tran_id,order_time,price,volume,sale_volume,buy_volume,type,sale_order_id,sale_order_price,buy_order_id,buy_order_price,order_date,order_code,order_dc',
    #     'max_filter_ratio': '0.1',
    #     'strict_mode': 'false',
    #     'skip_header': '1'
    # }

    headers = {
        'column_separator': ',',
        'columns': 'tran_id,order_time,price,volume,sale_volume,buy_volume,type,sale_order_id,sale_order_price,buy_order_id,buy_order_price,order_date,order_code,order_dc',
        'max_filter_ratio': '0.8',
        'strict_mode': 'false',
        'skip_header': '1',
        'Expect': ''  # 这个可以显式禁用 100-Continue 机制
    }

    # Path to the file that needs to be uploaded
    # file_path = '/mnt/data1/2024-01-02/000001.csv'
    file_path = r'D:\ticket_test\2024-01-02\000001.csv'

    #
    #
#    file_path = "D:/ticket_test/2024-01-02"
    # Open the file and prepare the data for uploading
    with open(file_path, 'rb') as f:
        # Send the POST request to upload the file
        response = requests.put(url, auth=auth, headers=headers, files={'file': f})

    # Check the response
    if response.status_code == 200:
        print('File uploaded successfully')
    else:
        print(f'Error: {response.status_code}, {response.text}')