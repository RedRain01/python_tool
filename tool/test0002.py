import os


def get_sorted_subfolders(path):
    # 获取给定路径下的所有子文件夹名称
    try:
        # 获取路径下所有子文件夹并过滤掉文件
        subfolders = [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
        # 对文件夹名称进行排序
        subfolders.sort()
        return subfolders
    except Exception as e:
        print(f"Error: {e}")
        return []


def main():
    # 定义test路径
    test_path = "/home/why/whyData/2024csv"  # 可以替换为你需要的路径
    # 获取并排序子文件夹
    subfolders = get_sorted_subfolders(test_path)

    # 输出文件夹名称，使用**作为分隔符
    print("**".join(subfolders))


if __name__ == "__main__":
    main()



