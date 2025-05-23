#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Created on 2023/12/06
@author: Monday
@group : waditu
Desc: 腾讯-股票-实时行情-成交明细
成交明细-每个交易日 16:00 提供当日数据
港股报价延时 15 分钟
"""
import warnings
import pandas as pd
import requests
from io import StringIO
from tushare.util.verify_token import require_permission
from tushare.util.format_stock_code import format_stock_code
from tushare.stock.rtq_vars import zh_sina_a_stock_cookies, zh_sina_a_stock_headers
import time
from redis_client import RedisClient

from typing import Optional
from tushare.util.form_date import get_current_date
from tushare.stock import rtq_vars
from tushare.util.format_stock_code import symbol_verify
from check_proxy_ip import get_proxy_from_pool_new
from data_management import insert_tickt_add_error
import aiohttp
import asyncio
from aiohttp import ClientSession, ClientTimeout
from aiohttp_socks import ProxyConnector




redis = RedisClient(host="192.168.0.103", port=6699, password="why123", db=0)# 模拟 HTTP 请求获取数据
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
}


@require_permission(event_name="realtime_tick", event_detail="个股历史分笔数据")
def realtime_tick(ts_code: str = "000001.SZ", src: Optional[str] = "tx",
                  page_count: Optional[int] = None) -> pd.DataFrame:
    """
    历史分笔数据
    :param ts_code: 股票代码
    :type ts_code: str
    :param src: 来源  腾讯财经tx   新浪财经sina
    :type src: str
    :param page_count: 限制页数
    :type page_count: str
    :return: 历史分笔数据
    :rtype: pandas.DataFrame
        1、TIME : 成交时间
        2、PRICE : 成交价格
        3、PCHANGE : 涨跌幅
        4、CHANGE : 价格变动
        5、VOLUME : 成交量(手)
        6、AMOUNT : 成交额(元)
        7、TYPE : 性质
    """
    symbol = symbol_verify(ts_code)
    if src == "sina":
        return get_stock_sina_a_divide_amount(symbol, page_count)
    else:
        return get_stock_tx_a_divide_amount(symbol, page_count)

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
    proxy = get_proxy_from_pool_new() # 随机选择一个代理
    timeout = ClientTimeout(total=10)  # 请求超时时间
    connector = ProxyConnector.from_url(proxy)  # 创建代理连接器
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        html = await fetch(url, session, proxy)
        if html:
            print(f"获取到页面内容: {html[:100]}...")  # 打印网页内容的前100个字符
def get_stock_tx_a_divide_amount(symbol: str = "sz000001", page_count: Optional[int] = None) -> pd.DataFrame:
    symbols = str(symbol).lower().split(".")
    symbol = f"{symbols[1]}{symbols[0]}"
    big_df = pd.DataFrame()
    page = page_count
    warnings.warn("正在下载数据，请稍等")
    while True:
        proxy = get_proxy_from_pool_new()
        try:
            url = "http://stock.gtimg.cn/data/index.php"
            params = {
                "appn": "detail",
                "action": "data",
                "c": symbol,
                "p": page,
            }
            r = requests.get(url, headers=headers,proxies={"http": proxy, "https": proxy},params=params)
            text_data = r.text
            temp_df = (
                pd.DataFrame(eval(text_data[text_data.find("["):])[1].split("|"))
                    .iloc[:, 0]
                    .str.split("/", expand=True)
            )
            big_df = pd.concat([big_df, temp_df], ignore_index=True)
            print("查询股票数据成功-----------",page)
            break
        except  Exception as e:
            print("查询股票数据失败-----------",e)
            time.sleep(1)
            insert_tickt_add_error(symbol,page, str(e))

    if not big_df.empty:
        big_df = big_df.iloc[:, 1:].copy()
        # big_df.columns = ["成交时间", "成交价格", "价格变动", "成交量", "成交金额", "性质"]
        big_df.columns = rtq_vars.TICK_COLUMNS

        big_df.reset_index(drop=True, inplace=True)
        property_map = {
            "S": "卖盘",
            "B": "买盘",
            "M": "中性盘",
        }
        # big_df["性质"] = big_df["性质"].map(property_map)
        big_df["TYPE"] = big_df["TYPE"].map(property_map)
        big_df = big_df.astype(
            {
                "TIME": str,
                "PRICE": float,
                "CHANGE": float,
                "VOLUME": int,
                "AMOUNT": int,
                "TYPE": str,
            }
        )
    return big_df


def get_stock_sina_a_divide_amount(symbol: str = "sz000001", page_count: Optional[int] = None, ) -> pd.DataFrame:
    """
    腾新浪财经-历史分笔数据
    https://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php?symbol=sh688553
    :param symbol: 股票代码
    :type symbol: str
    :param page_count: 限制页数
    :type page_count: str
    :return: 历史分笔数据
    :rtype: pandas.DataFrame
    """
    warnings.warn("正在下载数据，请稍等")
    symbols = str(symbol).lower().split(".")
    symbol = f"{symbols[1]}{symbols[0]}"
    page = 0
    big_df = pd.DataFrame()
    while True:
        try:
            url = "https://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php"
            params = {
                "symbol": symbol,
                "date": get_current_date(date_format="%Y-%m-%d"),
                "page": page
            }
            response = requests.get(url, headers=zh_sina_a_stock_headers, cookies=zh_sina_a_stock_cookies,
                                    params=params)
            temp_df = (pd.read_html(StringIO(response.content.decode("GBK")))[3])
            big_df = pd.concat([big_df, temp_df], ignore_index=True)
            page += 1
            if page_count and page >= page_count:
                break
        except:
            break
        time.sleep(0.5)
    if not big_df.empty:
        big_df = big_df.iloc[:, 0:].copy()
        # big_df.columns = ["成交时间", "成交价格", "涨跌幅", "价格变动", "成交量(手)", "成交额(元)", "性质"]
        big_df.columns = rtq_vars.TODAY_TICK_COLUMNS
        big_df.reset_index(drop=True, inplace=True)
        # big_df = big_df.astype(
        #     {
        #         "成交时间": str,
        #         "成交价格": float,
        #         "涨跌幅": str,
        #         "价格变动": str,
        #         "成交量(手)": int,
        #         "成交额(元)": int,
        #         "性质": str,
        #     }
        # )
        big_df = big_df.astype(
            {
                "TIME": str,
                "PRICE": float,
                "PCHANGE": str,
                "CHANGE": str,
                "VOLUME": int,
                "AMOUNT": int,
                "TYPE": str,
            }
        )
    return big_df


if __name__ == '__main__':
    df = realtime_tick(ts_code="601727.SH", src="tx", page_count=1)
    print(help(realtime_tick))
    print(df)
