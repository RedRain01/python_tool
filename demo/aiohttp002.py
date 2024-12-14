import aiohttp
import asyncio
import random
from check_proxy_ip import get_proxy_from_pool_new




# 异步请求函数
async def fetch(session, url):
    proxy = get_proxy_from_pool_new()  # 随机选择一个代理
    try:
        # 发起异步请求时使用代理
        async with session.get(url, proxy=proxy) as response:
            # 打印请求的状态码和返回内容的一部分
            print(f"Request to {url} with proxy {proxy} returned status {response.status}")
            return await response.text()
    except Exception as e:
        print(f"Request to {url} failed with proxy {proxy}. Error: {e}")
        return None


# 主函数，管理多个异步请求
async def main():
    urls = [
        "http://httpbin.org/ip",
        "http://httpbin.org/ip",
        "http://httpbin.org/ip",
        "http://httpbin.org/ip",
        "http://httpbin.org/ip",
        "http://httpbin.org/ip",
        "http://httpbin.org/ip",
        "http://example.net"
    ]

    # 创建aiohttp会话
    async with aiohttp.ClientSession() as session:
        # 创建并发任务
        tasks = [asyncio.create_task(fetch(session, url)) for url in urls]

        # 使用一个循环，逐个等待并处理已完成的请求
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                print("Fetched content:", result[:100])  # 打印前100个字符
            else:
                print("Failed to fetch content.")


# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())
