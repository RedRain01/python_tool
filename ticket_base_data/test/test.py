import mysql.connector
import random
import string
import time

# Doris/MySQL 配置
DORIS_HOST = '127.0.0.1'  # 服务器地址
DORIS_PORT = 9030  # 查询端口（默认9030）
DATABASE = 'demo'
USERNAME = 'root'
PASSWORD = ''


def generate_data(batch_size):
    """生成批量测试数据（与原始结构一致）"""
    data = []
    for _ in range(batch_size):
        record = (
            random.randint(100000, 999999),  # id
            ''.join(random.choices(string.ascii_letters, k=8)),  # name
            random.randint(18, 25),  # age
            random.choice(['Male', 'Female']),  # gender
            round(random.uniform(60.0, 100.0), 2)  # grade
        )
        data.append(record)
    return data


def batch_insert(data):
    """执行批量插入"""
    try:
        conn = mysql.connector.connect(
            host=DORIS_HOST,
            port=DORIS_PORT,
            user=USERNAME,
            password=PASSWORD,
            database=DATABASE
        )
        cursor = conn.cursor()

        # 使用预编译语句
        sql = """
        INSERT INTO student (id, name, age, gender, grade)
        VALUES (%s, %s, %s, %s, %s)
        """

        # 批量执行
        cursor.executemany(sql, data)
        conn.commit()




        print(f"成功插入 {len(data)} 条数据")
        return True

    except mysql.connector.Error as e:
        print(f"数据库错误: {str(e)}")
        return False
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()





if __name__ == '__main__':
    batch_size = 1000  # 每批次数据量
    total_batches = 1000  # 总批次数

    for i in range(total_batches):
        print(f"正在处理第 {i + 1}/{total_batches} 批数据...")
        test_data = generate_data(batch_size)

        success = batch_insert(test_data)
        if not success:
            print("当前批次插入失败，正在重试...")
            time.sleep(2)
            success = batch_insert(test_data)  # 简单重试

       # time.sleep(0.1)  # 控制频率
