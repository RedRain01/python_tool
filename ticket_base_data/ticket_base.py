import tushare as ts

if __name__ == '__main__':
    pro = ts.pro_api()
    data1 = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
