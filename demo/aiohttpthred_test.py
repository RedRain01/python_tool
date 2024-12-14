import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import time


# 异步HTTP请求任务
async def fetch(url):
    print(f"开始访问: {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            print(f"完成访问: {url}，状态码: {response.status}")
            return content  # 返回响应内容


# 线程中执行异步任务
def process_task(queue):
    loop = asyncio.new_event_loop()  # 每个线程创建独立的事件循环
    asyncio.set_event_loop(loop)  # 设置当前线程的事件循环
    while not queue.empty():
        url = queue.get()  # 从队列中获取任务
        if url:
            result = loop.run_until_complete(fetch(url))  # 运行异步HTTP请求
            # 处理结果（可以将结果存储或打印）
            print(f"处理完成：{url}")
        queue.task_done()  # 标记任务完成


# 主函数，创建队列和线程池
def main():
    start_time = time.time()

    # 创建一个队列，并填充任务（HTTP URLs）
    url_queue = Queue()
    urls = [
        "http://httpbin.org/get",
        "http://httpbin.org/ip",
        "http://httpbin.org/delay/2",
        "http://httpbin.org/user-agent",
        # 可以添加更多的 URL
    ]
    for url in urls:
        url_queue.put(url)

    # 创建线程池，最大线程数可以根据需求调整
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 提交多个任务到线程池
        for _ in range(4):  # 启动4个线程
            executor.submit(process_task, url_queue)

    # 等待队列所有任务完成
    url_queue.join()

    print(f"总耗时: {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    main()
