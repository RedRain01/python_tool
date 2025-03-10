import mysql.connector

def query_mysql(host, user, password, database, query, params=None):
"""
执行 MySQL 查询并返回结果
:param host: 数据库主机地址
:param user: 数据库用户名
:param password: 数据库密码
:param database: 数据库名称
:param query: SQL 查询语句
:param params: 查询参数（可选）
:return: 查询结果（列表）
"""
try:
# 连接到 MySQL
conn = mysql.connector.connect(
host=host,


user=user,
password=password,
database=database
)
cursor = conn.cursor(dictionary=True)

# 执行查询
cursor.execute(query, params or ())

# 获取结果
results = cursor.fetchall()

return results

except mysql.connector.Error as err:
print(f"Error: {err}")
return None

finally:
if cursor:
cursor.close()
if conn:
conn.close()

# 示例使用
if __name__ == "__main__":
host = "localhost"
user = "root"
password = "yourpassword"
database = "yourdatabase"
sql_query = "SELECT * FROM your_table WHERE column_name = %s"
params = ("value",)

result = query_mysql(host, user, password, database, sql_query, params)
print(result)