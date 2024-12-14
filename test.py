from redis_client import RedisClient
import requests
def main(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # 检查请求是否成功
        # 假设返回的内容是一个以逗号分隔的字符串
        string_data = response.text
        string_list = string_data.splitlines()  #  # 将字符串拆分成列表
        return string_list

    except Exception as e:
        print(f"请求出错: {e}")
        return []

if __name__ == "__main__":
    url = "http://58ip.top/api/get?token=182c0d366f278fb24ec56025fb2a9e&number=50&type=http&format=1"
    result = main(url)
    print(result)
