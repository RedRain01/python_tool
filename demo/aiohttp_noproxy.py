import aiohttp
import asyncio


async def fetch(url):
    # 创建异步会话
    async with aiohttp.ClientSession() as session:
        # 发送GET请求
        async with session.get(url) as response:
            # 获取响应的文本
            text = await response.text()
            print(f"Response from {url}: {text[:100]}...")  # 打印前100个字符
            return text


async def main():
    # 你可以在这里指定多个 URL
    urls = [
        'https://httpbin.org/get',
        'https://jsonplaceholder.typicode.com/todos/1',
        'https://api.github.com'
    ]

    # 使用 asyncio.gather 来并发请求多个 URL
    tasks = [fetch(url) for url in urls]

    # 等待所有任务完成
    await asyncio.gather(*tasks)


# 执行主函数
if __name__ == "__main__":
    asyncio.run(main())
