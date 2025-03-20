from ticket_base_data.doris_connect.dorise_db import doris_db
from datetime import  date,timedelta,datetime
from dataclasses import dataclass
from typing import Generic, TypeVar, Optional

T = TypeVar('T')
@dataclass
class Result(Generic[T]):
    success: bool
    data: Optional[T] = None  # 存储数据
    error: Optional[str] = None  # 存储错误信息

def base_job_query(code):
    sql = "SELECT * FROM job_details WHERE job_code = %s ORDER BY job_time DESC limit 1"
    params = (code,)
    job= doris_db.execute_query(sql, params)
    return job

def base_dic_query(key):
    sql = "SELECT * FROM dictionary WHERE dic_key= %s"
    params = (key,)
    job= doris_db.execute_query(sql, params)
    return job

def base_job_insert(params):
    try:
        query = '''INSERT INTO job_details ( job_name, job_code, job_start_date , job_end_date ,job_time, job_status, remark, job_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
        num = doris_db.execute_update(query, params)
        if num == 1:
            query_last_id = "SELECT MAX(id) as id FROM job_details"
            last_id = doris_db.execute_query(query_last_id, fetchone=True)
            return last_id
    except Exception as e:
        print(e)
        return -1
    else:
        return -1
    return num


def base_job_update(id,state):
    update = ''' UPDATE  job_details  SET job_status = %s  WHERE  id = %s'''
    params = (state,id)
    num=doris_db.execute_update(update, params)
    return num

def base_create_job(code, start_date, end_date, params)->Result[dict]:
    id = -1
    detail = base_job_query(code)
    print(f"get {code} job detail：", detail)
    token = base_dic_query("wdt_token")
    if token is None:
        print("token is None")
        return Result(success=False, error="token is None")
    print(token[0]['dic_value'])
    # 空 9 都认为已经完成，需要重新创建任务，跑任务，1 代表有需要执行的手动任务，只执行不创建
    if detail is None or len(detail) == 0 or detail[0]['job_status'] == 9 or detail[0]['job_status'] == 1:
        # 新增任务 且状态是初始状态
        if detail is None or len(detail) == 0 or detail[0]['job_status'] == 9:
            if detail is None or len(detail) == 0:
                start_date = start_date
            else:
                # 开始时间+1
                # 将字符串转换为 datetime 对象
                # 给日期加一天
                start_date = detail[0]['job_end_date'] + timedelta(days=1)
                if detail[0]['job_end_date'].strftime('%Y%m%d') ==date.today().strftime('%Y%m%d'):
                    print("当日任务已经完成")
                    return Result(success=False, error="当日任务已经完成")
                # 将新的日期转换回字符串
                start_date = start_date.strftime('%Y%m%d')
                #已存在已完成的任务
            end_date = date.today().strftime('%Y%m%d')
            if start_date != end_date:
                current_date = date.today()
                # 获取前一天的日期
                previous_day = current_date - timedelta(days=1)
                # 格式化前一天的日期为 'YYYYMMDD'
                end_date = previous_day.strftime('%Y%m%d')
            params=params[:2]+(start_date,)+params[3:]
            params=params[:3]+(end_date,)+params[4:]
            id = base_job_insert(params)['id']
            print(f"insert job id:{id}")
            if id < 1:
                print("新增任务失败")
                return Result(success=False, error="新增任务失败")
            else:
                return Result(success=True, data={"id": id, "start_date": start_date, "end_date": end_date,
                                                  "token": token[0]['dic_value']})
        else:
            id = detail[0]['id']
            start_date = detail[0]['job_start_date']
            end_date = detail[0]['job_end_date']
            return Result(success=True, data={"id": id,"start_date": start_date, "end_date": end_date, "token": token[0]['dic_value']})
    else:
        print(f"任务已经存在，id:{detail[0]['id']}")
        return Result(success=False, error="任务已经存在")