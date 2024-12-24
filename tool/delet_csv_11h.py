import os
import csv
import concurrent.futures


# 处理 CSV 文件
def process_csv_file(file_path):
    try:
        # 读取原始 CSV 文件
        with open(file_path, mode='r', newline='', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            rows = list(reader)

        # 创建一个新的列表，存储合法行
        valid_rows = []

        # 遍历每一行，检查是否符合要求
        for line_num, row in enumerate(rows, start=1):
            # 如果列数不满 11 或第七列数据不符合条件，跳过该行
            if len(row) < 11:
                print(f"Warning: Deleting row {line_num} in file {file_path} due to insufficient columns: {row}")
                continue

            # 检查第七列数据是否是单个字符串
            if not isinstance(row[6], str) or len(row[6].strip()) == 0:
                print(
                    f"Warning: Deleting row {line_num} in file {file_path} due to invalid value in 7th column: {row[6]}")
                continue

            # 如果列数大于 11，截取前 11 列
            if len(row) > 11:
                row = row[:11]  # 只保留前 11 列

            # 将符合要求的行加入到 valid_rows
            valid_rows.append(row)

        # 重新写入处理后的数据到原 CSV 文件
        with open(file_path, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(valid_rows)

        print(f"Successfully processed file: {file_path}")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")


# 递归遍历目录下的所有 CSV 文件，并使用多线程并发处理
def process_csv_files_in_directory(directory_path, max_workers=10):
    # 存储所有 CSV 文件路径
    csv_files = []

    # 遍历目录下的所有文件
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            # 只处理 .csv 文件
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                csv_files.append(file_path)

    # 使用 ThreadPoolExecutor 来并发处理文件
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_csv_file, csv_files)


if __name__ == "__main__":
    # 指定要处理的目录路径
    directory_path = "/home/why/whyData/2024"

    # 设置并发数为 10
    max_workers = 10

    # 调用函数递归处理 CSV 文件
    process_csv_files_in_directory(directory_path, max_workers)
