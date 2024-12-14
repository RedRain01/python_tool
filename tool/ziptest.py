import os
import py7zr


def extract_all_7z_files(archive_folder, output_folder):
    """
    解压 7z 压缩包中的所有文件，为文件添加日期前缀，处理压缩包内的所有文件。

    Args:
        archive_folder (str): 存放 7z 压缩包的文件夹路径。
        output_folder (str): 解压后文件存放的目标文件夹路径。
    """
    os.makedirs(output_folder, exist_ok=True)  # 确保输出目录存在
    log_file = os.path.join(output_folder, "error_log.txt")

    # 遍历压缩包文件夹中的所有 7z 文件
    for archive_file in os.listdir(archive_folder):
        if archive_file.endswith('.7z'):
            archive_path = os.path.join(archive_folder, archive_file)
            date_prefix = os.path.splitext(archive_file)[0]  # 提取日期部分（压缩包名）

            try:
                with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                    # 获取压缩包中所有文件的名称
                    file_names = archive.getnames()
                    for file_name in file_names:
                        # 生成带日期前缀的文件名
                        new_file_name = f"{date_prefix}_{os.path.basename(file_name)}"
                        new_file_path = os.path.join(output_folder, new_file_name)

                        # 解压文件
                        archive.extract(targets=[file_name], path=output_folder)

                        # 将解压后的文件重命名为带日期前缀的文件
                        original_file_path = os.path.join(output_folder, file_name)
                        if os.path.exists(original_file_path):
                            os.rename(original_file_path, new_file_path)
                            print(f"Extracted and renamed: {new_file_name}")
                        else:
                            raise FileNotFoundError(f"Extracted file not found: {file_name}")
            except Exception as e:
                # 如果解压失败，记录错误
                with open(log_file, "a") as log:
                    log.write(f"Failed to extract {archive_file}: {e}\n")
                print(f"Failed to extract {archive_file}: {e}")


if __name__ == '__main__':
    zip_folder = 'D:/ticket'  # 替换为存放压缩包的文件夹路径
    output_folder = 'D:/ticket_excle001'  # 替换为存放解压后文件的文件夹路径
    extract_all_7z_files(zip_folder, output_folder)
