import aiohttp
import asyncio
from check_proxy_ip import get_proxy_from_pool_new  # 从 check_proxy_ip 导入 get_proxy_from_pool_new
import time


# 异步获取 HTTP 请求的结果
async def fetch(session: aiohttp.ClientSession, id_param: str, proxy: str):
    url = f"http://httpbin.org/ip?id={id_param}"  # 使用队列中的 id 参数来构建 URL
    try:
        # 发起异步请求时使用代理
        async with session.get(url, proxy=proxy) as response:
            data = await response.json()  # 获取 JSON 格式的响应数据
            print(f"ID: {id_param} - Proxy: {proxy} - Response: {data}")
    except Exception as e:
        print(f"Request failed for ID {id_param} with proxy {proxy}. Error: {e}")


# 处理队列中的请求
async def process_queue(queue):
    # 创建一个 aiohttp session
    async with aiohttp.ClientSession() as session:
        while not queue.empty():
            id_param = await queue.get()  # 从队列中取出 id 参数
            proxy = get_proxy_from_pool_new()  # 获取代理
            url = f"http://httpbin.org/ip?id={id_param}"  # 使用队列中的 id 参数来构建 URL
            try:
                async with session.get(url, proxy=proxy) as response:
                    data = await response.json()  # 获取 JSON 格式的响应数据
                    print(f"ID: {id_param} - Proxy: {proxy} - Response: {data}")
            except Exception as e:
                print(f"Request failed for ID {id_param} with proxy {proxy}. Error: {e}")


# 主程序
async def main():
    # 创建一个队列，假设我们有 5 个 ID 参数要请求
    queue = asyncio.Queue()

    # 向队列中添加一些 ID 参数
    for i in range(1, 6):
        await queue.put(str(i))  # 将 ID 作为字符串放入队列中

    # 处理队列中的请求
    await process_queue(queue)


# 运行主程序
if __name__ == "__main__":
    start_time = time.time()  # 记录开始时间
    asyncio.run(main())  # 运行主程序
    print(f"Execution Time: {time.time() - start_time} seconds")  # 打印程序执行时间
