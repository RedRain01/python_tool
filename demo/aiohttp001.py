import aiohttp
import asyncio
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
        "http://example.com",
        "http://httpbin.org/ip",
        "http://example.net",
        "http://httpbin.org/ip",
        "http://example.net",
        "http://example.net"
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

        # 处理所有请求的结果
        for result in results:
            if result:
                print("Fetched content:", result[:100])  # 打印前100个字符


# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())
