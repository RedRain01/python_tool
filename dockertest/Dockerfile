# 使用官方 Python 3.9 镜像作为基础镜像
FROM python:3.9

# 设置工作目录
WORKDIR /app

# 将当前目录的内容复制到镜像中的 /app 目录
COPY . /app

# 安装依赖
RUN pip install --no-cache-dir -r requerements.txt

# 设置容器启动时运行的命令
CMD ["python", "app.py"]
