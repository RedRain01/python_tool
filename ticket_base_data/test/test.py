from datetime import date
import tushare as ts
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from ticket_base_data.base_db.job_base import  base_job_update,base_create_job

if __name__ == '__main__':
    pro = ts.pro_api()
    ts.set_token("68ad60144da544f022cc31fc973db234319ff2c329278cc79d2156aa")
    df = pro.cyq_chips(ts_code='000050.SZ', start_date='20250206', end_date='20250209')
    print(len(df))
