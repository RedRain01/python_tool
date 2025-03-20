import os
import csv
from concurrent.futures import ThreadPoolExecutor


# 定义处理每个文件的函数
def process_csv(file_path, folder_name):
    print("CSV文件------------！" + file_path+"---------------"+folder_name)
    # 获取文件名（不带路径）
    file_name = os.path.basename(file_path)
    # 获取文件夹名（即日期）
    date = folder_name
    # 获取文件名作为代码
    code = file_name[:-4]  # 去掉最后的 4 个字符
    # 拼接date_code字段
    date_code = f"{date}_{code}"

    # 读取和修改CSV文件
    temp_rows = []

    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # 读取文件头
        # 添加新字段到头部
        header.extend(['date', 'code', 'date_code'])
        # 遍历文件的每一行
        for row in reader:
            row.extend([date, code, date_code])  # 添加新字段值
            temp_rows.append(row)

    # 覆盖写入原文件
    with open(file_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)  # 写入新的头部
        writer.writerows(temp_rows)  # 写入修改后的数据


# 定义遍历目录并调用处理函数的函数
def traverse_directory(base_dir):
    # 获取所有子文件夹（日期）
    for folder_name in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder_name)

        if os.path.isdir(folder_path):  # 确保是文件夹
            # 遍历每个CSV文件
            for file_name in os.listdir(folder_path):
                if file_name.endswith('.csv'):
                    file_path = os.path.join(folder_path, file_name)
                    # 使用线程池并行处理
                    executor.submit(process_csv, file_path, folder_name)






if __name__ == '__main__':
    # 设置基本目录
    base_directory = '/mnt/data3/ticket_date/2025/csv'

    # 使用线程池并发执行
    with ThreadPoolExecutor(max_workers=200) as executor:  # 设置最大线程数为10
        traverse_directory(base_directory)
print("所有CSV文件处理完成！")