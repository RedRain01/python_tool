import requests
import redis
import random
import time
from redis_client import RedisClient
from data_management import insert_ip,select_all_ip
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from datetime import datetime
import threading

# 目标验证 URL，用于检查代理 IP 是否有效
TARGET_URL = "http://httpbin.org/ip"  # 可替换为你需要的任何目标URL


# 创建 Redis 连接
redis_client = RedisClient(host="192.168.0.103", port=6699, password="why123", db=0)# 模拟 HTTP 请求获取数据

# 设置代理池 Redis 键前缀
PROXY_POOL_KEY_PREFIX = "proxy_"

# 代理的过期时间（秒）
PROXY_EXPIRY_TIME = 60  # 每个代理存活时间为60秒


def db_to_queue(queue):
    print("异步代码执行")
    try:
        # time.sleep(180)
        while True:
            list = select_all_ip()
            for proxy_ip in list:
                queue.put(proxy_ip)
            time.sleep(60)
    except Exception as e:
        print("db_to_redis error:",e)
        pass

def is_proxy_valid(proxy_ip):
    try:
        response = requests.get(TARGET_URL, proxies={"http": proxy_ip, "https": proxy_ip}, timeout=5)
        if response.status_code == 200:
            return True
    except requests.RequestException:
        pass
    return False

def queue_to_redis(queue):
    while True:
        proxy_ip = queue.get()
        if proxy_ip is None:
            print("Queue is empty")
        else:
            if is_proxy_valid(proxy_ip):
                print("ip is ok--:", proxy_ip)
                redis_client.setex(PROXY_POOL_KEY_PREFIX + proxy_ip, proxy_ip, PROXY_EXPIRY_TIME)
            else:
                print("ip is fail--:", proxy_ip)

def do_work():
    db_queue = Queue(5000)
    # 创建线程来执行 db_to_queue 方法
    thread = threading.Thread(target=db_to_queue, args=(db_queue,))
    thread.start()  # 启动线程
    # 使用线程池来执行 queue_to_redis 方法
    with ThreadPoolExecutor(max_workers=10) as executor:  # 设置线程池的最大线程数
        for _ in range(10):  # 根据需要提交多个任务
            executor.submit(queue_to_redis, db_queue)
 # 启动线程

if __name__ == '__main__':
    do_work()
