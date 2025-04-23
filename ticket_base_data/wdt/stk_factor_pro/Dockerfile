FROM python:3.9-alpine

# 设置工作目录为 /app
WORKDIR /app

# 复制本地的 hm_detail.py 文件到容器的 /app 目录
COPY wdt/stk_factor_pro/ /app/


RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r /app/requerements.txt
# 设置容器启动命令为运行 hm_detail.py
CMD ["python", "index_daily.py"]
