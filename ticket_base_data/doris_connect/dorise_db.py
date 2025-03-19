import mysql.connector
from mysql.connector import pooling

class DorisDB:
    """Doris 数据库操作封装"""

    def __init__(self, host, port, user, password, database, pool_size=5):
        """初始化数据库连接池"""
        self.db_pool = pooling.MySQLConnectionPool(
            pool_name="doris_pool",
            pool_size=pool_size,
            host=host,
            port=port,  # 添加端口参数
            user=user,
            password=password,
            database=database
        )

    def execute_query(self, query, params=None, fetchone=False):
        """执行 SELECT 查询"""
        conn = self.db_pool.get_connection()
        cursor = conn.cursor(dictionary=True)  # 以字典格式返回数据
        try:
            cursor.execute(query, params)
            result = cursor.fetchone() if fetchone else cursor.fetchall()
        except mysql.connector.Error as e:
            print(f"查询错误: {e}")
            result = None
        finally:
            cursor.close()
            conn.close()
        return result

    def execute_update(self, query, params=None):
        """执行 INSERT、UPDATE、DELETE 操作"""
        conn = self.db_pool.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount  # 返回受影响的行数
        except mysql.connector.Error as e:
            print(f"执行错误: {e}")
            conn.rollback()
            return -1
        finally:
            cursor.close()
            conn.close()

# **初始化 Doris 数据库连接池**
doris_db = DorisDB(
    host='192.168.0.106',  # 替换为你的 DorisDB 主机地址
    port=9030,  # 替换为你的 DorisDB 端口号
    user='root',  # 替换为你的用户名
    password='why123',  # 替换为你的密码
    database='demo',  # 替换为你的数据库名称
)
