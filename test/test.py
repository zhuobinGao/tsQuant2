from initParam.R import pro
from tsDBUtil.MongoDBUtil import TSDB_CONN,save_data_update_one,find_data,update_date,find_simple_data
from datetime import datetime, timedelta
from log.stock_log import logger
from pymongo import UpdateOne,ASCENDING
import numpy as np
import threading
import pandas as pd
import tushare as ts
from initParam.R import pro
from matplotlib import pyplot as plt
from initParam.mysql_init import get_conn
from turtle import *

def simplePlot(code):
    # data_info = find_data(TSDB_CONN['stock_basic'], whereParam={'ts_code': code}, selParam={'_id':False})

    data_info = pd.read_sql("select * from stock_basic where ts_code='%s'" % code, get_conn())
    if len(data_info)<1:
        logger.info("没有该stock信息")
        return
    logger.info(data_info['fullname'][0])
    print(type(data_info['fullname'][0]))
    sql = "SELECT * FROM tsquant.daily where ts_code='%s' order by trade_date;"
    df = pd.read_sql(sql % code, get_conn())

    df['close'] = pd.to_numeric(df['close'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index(df['trade_date'], inplace=True)
    df.drop('trade_date', axis=1, inplace=True)
    plt.plot(df['close'], label='收盘价')
    # plt.plot(df['hfq_close'], label='后复权收盘价')
    plt.legend()
    plt.title(data_info['fullname'][0])
    plt.xlabel('日期')
    plt.ylabel('收盘价')

    plt.rcParams['font.sans-serif']=['SimHei']
    plt.show()


if __name__ == '__main__':
    pass

