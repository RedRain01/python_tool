import tushare as ts
import mysql.connector
from mysql.connector import Error


if __name__ == '__main__':
    ts.set_token('e8778dbd9b06d8fcee802d6d60d80ce706d2863979b551aad68845cf')
    pro = ts.pro_api()
    df = pro.hm_detail(trade_date='20250212')
    print(df)
