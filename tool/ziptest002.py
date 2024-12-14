import os
import py7zr


def extract_7z_files(archive_folder, output_folder):
    """
    解压 7z 压缩包中的所有文件，并将它们存储到目标文件夹。

    Args:
        archive_folder (str): 存放 7z 压缩包的文件夹路径。
        output_folder (str): 解压后文件存放的目标文件夹路径。
    """
    os.makedirs(output_folder, exist_ok=True)  # 确保输出目录存在

    # 遍历压缩包文件夹中的所有 7z 文件
    for archive_file in os.listdir(archive_folder):
        if archive_file.endswith('.7z'):
            archive_path = os.path.join(archive_folder, archive_file)

            try:
                # 打开压缩包并解压所有文件
                with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                    archive.extractall(path=output_folder)
                    print(f"Extracted {archive_file} to {output_folder}")
            except Exception as e:
                # 如果解压失败，记录错误
                print(f"Failed to extract {archive_file}: {e}")


if __name__ == '__main__':
    archive_folder = r'D:/ticket'  # 存放 7z 文件的文件夹路径
    output_folder = r'D:/ticket_excle001'  # 解压后文件存放的目标文件夹路径
    extract_7z_files(archive_folder, output_folder)
