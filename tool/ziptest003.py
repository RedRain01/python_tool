import os
import py7zr
from concurrent.futures import ThreadPoolExecutor, as_completed


def extract_single_7z_file(archive_file, archive_folder, output_folder):
    """
    解压单个 7z 压缩包中的所有文件，并将它们存储到目标文件夹。

    Args:
        archive_file (str): 压缩包文件的文件名。
        archive_folder (str): 存放 7z 压缩包的文件夹路径。
        output_folder (str): 解压后文件存放的目标文件夹路径。
    """
    archive_path = os.path.join(archive_folder, archive_file)

    try:
        # 打开压缩包并解压所有文件
        with py7zr.SevenZipFile(archive_path, mode='r') as archive:
            archive.extractall(path=output_folder)
            print(f"Extracted {archive_file} to {output_folder}")
    except Exception as e:
        # 如果解压失败，记录错误
        print(f"Failed to extract {archive_file}: {e}")


def extract_7z_files_in_parallel(archive_folder, output_folder, max_workers=40):
    """
    使用多线程解压 7z 压缩包中的所有文件，解压结果保存到目标文件夹。

    Args:
        archive_folder (str): 存放 7z 文件的文件夹路径。
        output_folder (str): 解压后文件存放的目标文件夹路径。
        max_workers (int): 最大线程数。
    """

    os.makedirs(output_folder, exist_ok=True)  # 确保输出目录存在
    # 获取文件夹中所有 7z 文件
    sss=os.listdir(archive_folder)
    archive_files = [f for f in os.listdir(archive_folder) if f.endswith('.7z')]

    # 使用线程池并行处理多个压缩包的解压任务
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(extract_single_7z_file, archive_file, archive_folder, output_folder): archive_file for
            archive_file in archive_files}

        for future in as_completed(future_to_file):
            archive_file = future_to_file[future]
            try:
                future.result()  # 获取任务执行结果，若有异常会抛出
            except Exception as e:
                print(f"Error with {archive_file}: {e}")


if __name__ == '__main__':
    archive_folder = '/home/why/whyData/2024'  # 存放 7z 文件的文件夹路径
    output_folder = '/mnt/data3/ticket_date/2024_csv'  # 解压后文件存放的目标文件夹路径
    extract_7z_files_in_parallel(archive_folder, output_folder, max_workers=100)
