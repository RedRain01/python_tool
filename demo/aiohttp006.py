import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time


# CPU 密集型任务
def cpu_bound_task(data):
    time.sleep(1)  # 模拟 CPU 密集操作
    return f"Processed {data}"


# 异步 HTTP 请求
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


# 异步封装 CPU 密集型任务
async def process_in_thread(data, executor):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, cpu_bound_task, data)


# 主函数
async def main():
    urls = [f"https://jsonplaceholder.typicode.com/posts/{i}" for i in range(1, 21)]  # 示例 20 个 HTTP 请求
    executor = ThreadPoolExecutor(max_workers=5)  # 线程池
    async with aiohttp.ClientSession() as session:
        tasks = []

        # 创建 HTTP 请求和线程池任务
        for i, url in enumerate(urls):
            tasks.append(fetch(session, url))  # 异步 HTTP 请求
            tasks.append(process_in_thread(f"Task-{i}", executor))  # 异步线程池任务

        # 使用 asyncio.as_completed 实现流式处理
        for coro in asyncio.as_completed(tasks):
            result = await coro
            print(result[:50])  # 打印部分结果

if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    print(f"Completed in {time.time() - start:.2f} seconds")