import os
import py7zr


def extract_and_rename_csv_files(archive_folder, output_folder):
    """
    解压 7z 压缩包中的 CSV 文件，并将其重命名为包含日期的文件名，保存到目标文件夹。

    Args:
        archive_folder (str): 存放 7z 压缩包的文件夹路径。
        output_folder (str): 解压后文件存放的目标文件夹路径。
    """
    os.makedirs(output_folder, exist_ok=True)  # 确保输出目录存在

    # 遍历压缩包文件夹中的所有 7z 文件
    for archive_file in os.listdir(archive_folder):
        if archive_file.endswith('.7z'):
            archive_path = os.path.join(archive_folder, archive_file)
            date_prefix = os.path.splitext(archive_file)[0]  # 提取日期部分（压缩包名）

            try:
                with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                    all_files = archive.getnames()  # 获取压缩包中所有文件
                    for file_name in all_files:
                        # 仅处理 .csv 文件
                        if file_name.endswith('.csv'):
                            # 生成新的文件名，添加日期前缀
                            new_file_name = f"{date_prefix}_{file_name}"
                            new_file_path = os.path.join(output_folder, new_file_name)

                            # 解压文件到目标文件夹
                            archive.extract(targets=[file_name], path=output_folder)

                            # 原始文件的解压路径
                            original_file_path = os.path.join(output_folder, file_name)
                            if os.path.exists(original_file_path):
                                # 重命名文件
                                os.rename(original_file_path, new_file_path)
                                print(f"Renamed and saved: {new_file_path}")
                            else:
                                print(f"Warning: File not found after extraction: {file_name}")
                        else:
                            print(f"Skipping non-CSV file: {file_name}")
            except Exception as e:
                # 如果解压失败，记录错误
                print(f"Failed to extract {archive_file}: {e}")


if __name__ == '__main__':
    archive_folder = '/mnt/data3/ticket_date/2025/allzip'  # 存放 7z 文件的文件夹路径
    output_folder = '/mnt/data3/ticket_date/2025/csv'
    extract_and_rename_csv_files(archive_folder, output_folder)
