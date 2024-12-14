import requests
from redis_client import RedisClient
from concurrent.futures import ThreadPoolExecutor

# Redis 配置
REDIS_HOST = '192.168.0.103'
REDIS_PORT = 6699
REDIS_DB = 0
REDIS_KEY = 'why123'

# 测试的目标 URL
TEST_URL = 'http://httpbin.org/ip'

# 代理池 (测试用)
proxy_pool = [
    '192.168.175.1:1998',
]


# 初始化 Redis 连接
redis_client = RedisClient(host="192.168.0.103", port=6699, password="why123", db=0)# 模拟 HTTP 请求获取数据

# 检查代理是否可用
def check_proxy(proxy):
    try:
        proxy = "http://1111-2222.0020.aaaa-cloud-cdna-apia-gwsd-aaaa-aabb.com:34795"
        response = requests.get(TEST_URL, proxies={'http': proxy, 'https': proxy}, timeout=5)
        if response.status_code == 200:
            print(f"Proxy {proxy} is valid.")
            # 将可用代理存入 Redis，并设置过期时间为 1 分钟
            redis_client.set(proxy, proxy, 60)
    except requests.RequestException:
        print(f"333333333333333Proxy {proxy} is invalid.")
    except Exception as e:
        print(f"--------111----------------Error: {e}")


# 多线程检查代理
def validate_proxies(proxies):
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(check_proxy, proxies)

if __name__ == '__main__':
    validate_proxies(proxy_pool)
    print("Finished checking proxies.")
