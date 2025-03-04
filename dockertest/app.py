# app.py
import requests

def main():
    url = "https://httpbin.org/get"  # 使用 httpbin.org 作为外网API测试
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print("握手成功！响应数据：")
            print(response.json())  # 打印返回的 JSON 数据
        else:
            print(f"请求失败，状态码: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"网络连接失败：{e}")

if __name__ == "__main__":
    main()
