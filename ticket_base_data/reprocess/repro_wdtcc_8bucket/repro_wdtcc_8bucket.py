import numpy as np
from scipy.stats import linregress, zscore
from sklearn.decomposition import PCA
from ticket_base_data.doris_connect.dorise_db import doris_db
from datetime import datetime



def extract_price_band_features(data):
    """
    输入：
        data: List[float]，任意天数的价格区间占比序列

    输出：
        dict：包含趋势、波动、形态、标准化、压缩等多个特征
    """
    result = {}
    data = np.array(data, dtype=float)
    n = len(data)

    if n < 2:
        # 如果数据太短，返回默认值
        return {
            "slope": 0.0,
            "std": 0.0,
            "max_up_streak": 0,
            "z_score_last": 0.0,
            "pca_explained_ratio": 0.0
        }

    # 1. 趋势：线性回归斜率
    days = np.arange(n)
    slope, _, _, _, _ = linregress(days, data)
    result["slope"] = slope

    # 2. 波动性：标准差
    result["std"] = np.std(data)

    # 3. 连续上涨天数
    max_streak = 0
    current_streak = 0
    for i in range(1, n):
        if data[i] > data[i - 1]:
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0
    result["max_up_streak"] = max_streak

    # 4. Z-score（最后一个值）
    if np.std(data) != 0:
        z_scores = zscore(data)
        result["z_score_last"] = float(z_scores[-1])
    else:
        result["z_score_last"] = 0.0

    # 5. PCA：第一主成分解释率
    try:
        reshaped = data.reshape(-1, 1)
        pca = PCA(n_components=1)
        pca.fit(reshaped)
        result["pca_explained_ratio"] = float(pca.explained_variance_ratio_[0])
    except Exception:
        result["pca_explained_ratio"] = 0.0

    return result

def getdata():
    rows = doris_db.execute_query("SELECT cal_date FROM  wdt_trade_cal wtc  WHERE cal_date <'20220109' and is_open ='1'order by cal_date  desc limit 5 ", None, False)

def get_latetime():
    rows = doris_db.execute_query("SELECT * FROM  derived_wdtcc_bucket dwb order by trade_date desc  limit 1", None, False)
    # 判断是否有结果，并返回 trade_date 或当天日期
    if rows and len(rows) > 0 and 'trade_date' in rows[0]:
        return rows[0]['trade_date']
    else:
        return datetime.now().strftime('%Y%m%d')


def get_wdtcc_list(ts_code, trade_date):
    sql = "SELECT * FROM wdt_cyq_chips wcc WHERE ts_code = %s AND trade_date = %s"
    params = (ts_code, trade_date)
    rows = doris_db.execute_query(sql, params, False)
    return rows


def get_date_list():
    rows = doris_db.execute_query("SELECT cal_date  FROM  wdt_trade_cal wtc WHERE wtc.cal_date >'20220101' and wtc.is_open ='1' order by cal_date desc", None, False)
    cal_date_list = [row['cal_date'] for row in rows] if rows else []
    return cal_date_list

def main():
    # need a latest time
    last_time=get_latetime()
    # get date list
    date_list=get_date_list()
    rows = doris_db.execute_query("SELECT * FROM ticket_base_list", None, False)
    for row in rows:
        ts_code = row['ts_code']
        for date in date_list:
            get_wdtcc_list(ts_code, date)



    # get ticketcode list



    # for latetime to targetTime

    # for all ticket pricePercent

    print()

if __name__ == "__main__":
    print()
    main()