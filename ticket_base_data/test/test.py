import tushare as ts
if __name__ == '__main__':
    pro = ts.pro_api()
    ts.set_token("68ad60144da544f022cc31fc973db234319ff2c329278cc79d2156aa")
   # df = pro.trade_cal(exchange='', start_date='20250305', end_date='20250329')
    df = pro.daily_basic(ts_code='', trade_date='20240228',
                         fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb')
    print(df)
