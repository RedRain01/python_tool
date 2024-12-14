import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import time
from redis_client import RedisClient


redis = RedisClient(host="192.168.0.103", port=6699, password="why123", db=0)# 模拟 HTTP 请求获取数据

# 模拟任务生产者
def producer(queue):
    for i in range(100000):
        queue.put("http://httpbin.org/ip")

# 消费者函数：从队列中取 URL，异步发起请求
async def consumer(queue, session, executor):
    while True:
        try:
            # 从队列中取出任务
            url = queue.get_nowait()
        except Exception:
            break  # 队列空时退出循环

        # 模拟多线程操作：可以加入 CPU 密集型逻辑
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, lambda: print(f"Processing {url} in thread"))
        # 异步请求 URL
        try:
            async with session.get(url) as response:
                data = await response.json()
                if data['origin']:
                    redis.incr("success_count")
                    print(f"Fetched {url[:30]}: {data['origin']}")
                else:
                    redis.incr("error_count")
                    print(f"Fetched {url[:30]}: {data}")
        except Exception as e:
            print(f"Error fetching {url}: {e}")

# 主函数：调度生产者和消费者
async def main():
    queue = Queue()
    executor = ThreadPoolExecutor(max_workers=200)  # 线程池

    # 启动生产者
    producer(queue)

    # 启动 aiohttp 会话
    async with aiohttp.ClientSession() as session:
        # 创建多个消费者任务
        consumers = [consumer(queue, session, executor) for _ in range(200)]  # 10 个消费者
        await asyncio.gather(*consumers)


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    print(f"Completed in {time.time() - start:.2f} seconds")