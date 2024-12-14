import requests

def test_proxy(proxy):
    """测试单个代理是否可用"""
    url = "http://httpbin.org/ip"  # 使用 httpbin 来测试请求是否成功
    try:
        # 使用代理发送请求
        response = requests.get(url, proxies={"http": proxy, "https": proxy}, timeout=5)
        
        # 如果请求成功，返回代理的响应
        if response.status_code == 200:
            print(f"代理 {proxy} 可用")
            return True
        else:
            print(f"代理 {proxy} 返回错误: {response.status_code}")
            return False
    except requests.RequestException as e:
        # 捕获请求异常，代理不可用
        print(f"代理 {proxy} 不可用，错误: {e}")
        return False

def test_proxy_list(proxy_list):
    """测试代理列表中的所有代理"""
    working_proxies = []
    
    for proxy in proxy_list:
        if test_proxy(proxy):
            working_proxies.append(proxy)
    
    print("\n可用的代理列表：")
    for proxy in working_proxies:
        print(proxy)

# 代理列表，可以是 IP:PORT 格式的代理
proxy_list = [
    "http://61.160.223.141:7302",
    "http://8.213.128.6:1234",
    "http://121.37.207.154:8118",
    "http://47.100.90.127:6969",
    "http://47.96.70.163:8888",
    "http://58.20.248.139:9002",
    "http://120.55.49.231:6969",
    "http://124.70.221.252:8080",
    "http://47.109.56.77:1081",
    "http://8.213.137.155:8080"
]

# 测试代理列表
if __name__ == '__main__':
    test_proxy_list(proxy_list)


