import aiohttp
import asyncio
import random
from aiohttp import ClientSession, ClientTimeout
from aiohttp_socks import ProxyConnector
from check_proxy_ip import get_proxy_from_pool_new


# 异步获取网页内容
async def fetch(url: str, session: ClientSession, proxy: str):
    try:
        async with session.get(url, proxy=proxy) as response:
            print(f"成功访问 {url} 使用代理 {proxy}")
            return await response.text()  # 获取网页内容
    except Exception as e:
        print(f"请求 {url} 时发生错误: {e}")
        return None

# 创建一个异步的请求任务
async def fetch_with_proxy(url: str):
    proxy = get_proxy_from_pool_new()  # 随机选择一个代理
    timeout = ClientTimeout(total=10)  # 请求超时时间
    connector = ProxyConnector.from_url(proxy)  # 创建代理连接器
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        html = await fetch(url, session, proxy)
        if html:
            print(f"获取到页面内容: {html[:100]}...")  # 打印网页内容的前100个字符

# 主程序
async def main():
    url = "http://httpbin.org/ip"  # 目标网址
    await fetch_with_proxy(url)

# 运行事件循环
if __name__ == "__main__":
    asyncio.run(main())
