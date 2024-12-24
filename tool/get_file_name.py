import os


def get_subdirectories(path):
    # 获取指定路径下的所有子项
    items = os.listdir(path)

    # 过滤出文件夹（只保留文件夹的名字）
    subdirs = [item for item in items if os.path.isdir(os.path.join(path, item))]

    # 将文件夹名称用逗号连接并返回
    return ",".join(subdirs)




if __name__ == '__main__':
    # 测试
    folder_path = r"D:\ticket_excle002"  # 请替换为你自己的路径
    subdirectories = get_subdirectories(folder_path)
    print(subdirectories)



