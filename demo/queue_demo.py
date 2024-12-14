import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import time

class BoundedQueue:
    def __init__(self, capacity):
        self.capacity = capacity
        self.queue = queue.Queue(maxsize=capacity)
        self.lock = threading.Lock()

    def enqueue(self, item):
        # 如果队列已满，则阻塞直到有空间
        with self.lock:
            if self.queue.full():
                print("队列已满，暂停新增。")
                return False
            else:
                self.queue.put(item)
                return True

    def dequeue(self):
        # 从队列中取出一个元素
        return self.queue.get()

    def size(self):
        # 返回队列当前的大小
        return self.queue.qsize()

def consumer_task(queue):
    while True:
        if not queue.empty():
            item = queue.dequeue()
            print(f"正在消费: {item}")
            time.sleep(1)  # 模拟消费时间
        else:
            time.sleep(0.1)  # 如果队列为空，稍微等待一下

def producer_task(queue):
    counter = 0
    while True:
        # 尝试将新的任务放入队列
        if queue.enqueue(f"任务 {counter}"):
            print(f"入队: 任务 {counter}")
            counter += 1
        time.sleep(0.5)  # 模拟生成任务的时间

def main():
    # 创建一个队列对象，容量为2000
    queue_instance = BoundedQueue(capacity=2000)

    # 创建线程池，最多10个消费者线程
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 启动多个消费者线程
        for _ in range(10):
            executor.submit(consumer_task, queue_instance)

        # 启动生产者线程
        producer_thread = threading.Thread(target=producer_task, args=(queue_instance,))
        producer_thread.start()

        # 主线程等待生产者线程执行完毕
        producer_thread.join()

if __name__ == "__main__":
    main()
