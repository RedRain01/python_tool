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



# 创建 Redis 连接
redis_client = RedisClient(host="192.168.0.103", port=6699, password="why123", db=0)# 模拟 HTTP 请求获取数据


# 目标验证 URL，用于检查代理 IP 是否有效
TARGET_URL = "http://httpbin.org/ip"  # 可替换为你需要的任何目标URL

# 获取代理IP的API，替换为实际获取代理IP的API地址
PROXY_LIST_API = "http://58ip.top/api/get?token=182c0d366f278fb24ec56025fb2a9e&number=50&type=http&format=1"

# 设置代理池 Redis 键前缀
PROXY_POOL_KEY_PREFIX = "proxy_"

# 代理的过期时间（秒）
PROXY_EXPIRY_TIME = 60  # 每个代理存活时间为60秒


# 验证代理IP是否可用
def is_proxy_valid(proxy_ip):
    try:
        response = requests.get(TARGET_URL, proxies={"http": proxy_ip, "https": proxy_ip}, timeout=5)
        if response.status_code == 200:
            return True
    except requests.RequestException:
        pass
    return False

def check_proxy_ip(queue):
    while True:
        proxy_ip = queue.get()
        if proxy_ip is None:
            print("Queue is empty")
        else:
            if is_proxy_valid(proxy_ip):
                # 将有效的IP加入Redis，并为每个IP设置过期时间为60秒
                redis_client.setex(PROXY_POOL_KEY_PREFIX + proxy_ip, proxy_ip, PROXY_EXPIRY_TIME)
                insert_ip(proxy_ip)
            #     print(f"代理IP {proxy_ip} 已加入池中，设置过期时间为60秒")
            # else:
            #     print(f"代理IP {proxy_ip} 无效，跳过")




# 获取代理IP列表
def get_proxy_list():
    try:
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        response = requests.get(PROXY_LIST_API)
        response.raise_for_status()  # 检查请求是否成功
        # 假设返回的内容是一个以逗号分隔的字符串
        string_data = response.text
        string_list = string_data.splitlines()
        print("获取代理列表成功",current_date)
        return string_list  # 假设返回的数据中有一个'数据'字段存储了IP
    except Exception as e:
        print("获取代理列表失败",current_date)
        print("获取代理列表失败msg-----",e)
        return []



def db_to_redis():
    print("异步代码执行")
    try:
        # time.sleep(180)
        while True:
            list = select_all_ip()
            for proxy_ip in list:
                if is_proxy_valid(proxy_ip):
                    # 将有效的IP加入Redis，并为每个IP设置过期时间为60秒
                    redis_client.setex(PROXY_POOL_KEY_PREFIX + proxy_ip, proxy_ip, PROXY_EXPIRY_TIME)
                else:
                    print("数据库ip失效:",proxy_ip)
            time.sleep(120)
    except Exception as e:
        print("db_to_redis error:",e)
        pass


# 更新代理池（获取代理IP并存入Redis）
def update_proxy_pool():

    print("同步代码执行")
    queue = Queue(maxsize=2000)
    #consume_queue_data(queue)
    insert_threads = []
    for _ in range(40):  # 创建 5 个插入线程
        t = threading.Thread(target=check_proxy_ip, args=(queue,))
        t.start()
        insert_threads.append(t)

    while True:
        proxy_list = get_proxy_list()
        if not proxy_list:
            print("未获取到代理IP列表或API错误！")
            time.sleep(5)
        for proxy in proxy_list:
            proxy_ip = f"http://{proxy}"  # 假设每个proxy是一个纯粹的IP字符串
            queue.put(proxy_ip)
        time.sleep(5)


def consume_queue_data(queue):
    with ThreadPoolExecutor(max_workers=20) as executor:
        while True:
            try:
                # 从队列中获取数据，timeout 设置任务超时
                item = queue.get(timeout=1)  # 如果 1 秒内没有新任务，则抛出 queue.Empty 异常
            except Exception as e:
                print("Queue is empty",e)
                time.sleep(10)
            executor.submit(check_proxy_ip, queue)  # 提交任务

# 从Redis中随机获取一个有效的代理IP
def get_proxy_from_pool():
    # 获取所有符合代理IP前缀的键名
    keys = redis_client.keys(PROXY_POOL_KEY_PREFIX + "*")
    if keys:
        proxy_key = random.choice(keys)  # 随机选取一个代理键
        proxy_ip = redis_client.get(proxy_key)
        print(f"从代理池中取出代理IP: {proxy_ip}")
        # 更新该代理的过期时间（让其活跃延长60秒）
        redis_client.expire(proxy_key, PROXY_EXPIRY_TIME)
        return proxy_ip
    else:
        print("代理池为空！")
        return None


def get_proxy_from_pool_new():
    while True:
        keys = redis_client.keys(PROXY_POOL_KEY_PREFIX + "*")
        if keys:
            proxy_key = random.choice(keys)  # 随机选取一个代理键
            proxy_ip = redis_client.get(proxy_key)
            print(f"从代理池中取出代理IP: {proxy_ip}")
            # 更新该代理的过期时间（让其活跃延长60秒）
            redis_client.expire(proxy_key, PROXY_EXPIRY_TIME)
            try:
                response = requests.get(TARGET_URL, proxies={"http": proxy_ip, "https": proxy_ip}, timeout=5)
                if response.status_code == 200:
                    return proxy_ip
                else:
                    redis_client.delete(proxy_key)
            except Exception as e:
                print("代理异常！",e)
        else:
            print("代理池为空！")

# 主程序
if __name__ == "__main__":

   # db_to_redis()
    # # 从池中获取一个代理
    # proxy = get_proxy_from_pool()
    # if proxy:
    #     # 你可以用这个代理发起请求
    #     print(f"使用代理 {proxy} 进行操作...")
    # else:
    #     print("未能获取到有效代理")

    # 每60秒更新一次代理
    update_proxy_pool()

