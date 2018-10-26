#coding=utf-8
from initParam.R import pro
from tsDBUtil.MongoDBUtil import TSDB_CONN,save_data_update_one,find_data,update_date
from datetime import datetime, timedelta
from log.stock_log import logger
from pymongo import UpdateOne,ASCENDING
import numpy as np
import pandas as pd
import threading

start1 = '19901201'
end1 = '20180826'


def init_base_schedule(start=start1, end=end1):

    datas = find_data(TSDB_CONN['job_basic'], whereParam={'start': start, 'end': end})

    if len(datas) > 0:
        logger.info("已设置初始任务")
        return
    datas = find_data(TSDB_CONN['stock_basic'], selParam={'_id': False, 'ts_code': True})
    update_requests = []
    for data in datas:
        doc = {'ts_code': data['ts_code'], 'start': start, 'end': end,
               'daily': False, 'adj_factor': False, 'suspend': False,
               'daily_basic': False, 'cal_hfq': False, 'index': False}
        index = {'ts_code': data['ts_code']}
        update_requests.append(UpdateOne(index, {'$set': doc}, upsert=True))
    save_data_update_one(None, TSDB_CONN['job_basic'], update_requests=update_requests, memo='设置基础任务')


def daily_crawler_job(query_name, is_before7=False, is_thread=False, thread_n=3, fields=None):

    if is_before7:
        now = datetime.now()
        before = now - timedelta(days=7)
        end_date= now.strftime('%Y%m%d')
        start_date = before.strftime('%Y%m%d')
        init_base_schedule(start=start_date, end=end_date)

    datas = find_data(TSDB_CONN['job_basic'], whereParam={query_name: False},
                      selParam={'ts_code': True, 'start': True, 'end': True, '_id': False})
    if is_thread:
        code_list = np.array_split(np.asarray(datas), thread_n, axis=0)
    else:
        code_list = np.array_split(np.asarray(datas), 1, axis=0)
    for code_ls in code_list:
        t1 = threading.Thread(target=__daily_crawler_job, args=(query_name, code_ls, {'fields': fields}))
        t1.start()


def __daily_crawler_job(query_name, code_ls, fields= None):

    i = 0
    l = len(code_ls)
    for code in code_ls:
        i += 1
        logger.info('%s抓取%s%s [%d/%d]'%(threading.current_thread().name,query_name,code,i,l))
        ts_code = code['ts_code']
        start = code['start']
        end = code['end']
        data = None
        update_field = ''
        if query_name == 'daily':
            update_field = "daily"
            index = ['ts_code', 'trade_date']
            data = pro.query(query_name,ts_code=ts_code, start_date=start, end_date=end)
        elif query_name=='adj_factor':
            update_field = "adj_factor"
            data = pro.query(query_name, ts_code=ts_code)
            index = ['ts_code', 'trade_date']
        elif query_name == 'suspend':
            update_field = "suspend"
            data = pro.query(query_name, ts_code=ts_code,
                             fields="ts_code,suspend_date,resume_date,ann_date,suspend_reason,reason_type")
            index = ['ts_code']
        elif query_name == 'daily_basic':
            update_field = "daily_basic"
            data = pro.query(query_name, ts_code=ts_code, start_date=start, end_date=end)
            index = ['ts_code', 'trade_date']
        memo = "执行任务"+query_name
        where_param = {'ts_code': ts_code, 'start': start, 'end': end}
        update_doc = {update_field: True}
        result, rlen = save_data_update_one(data, TSDB_CONN[query_name], indexs=index,  memo=memo)
        memo = "更新执行任务" + query_name + "[%s]状态" % code
        flag = result is not None and (result.matched_count > 0 or result.upserted_count > 0 or result.modified_count > 0)
        flag = flag or rlen == 0
        if flag:
            update_date(TSDB_CONN['job_basic'], where_param=where_param, update_doc=update_doc,memo=memo)


def cal_hfq_close(is_thread=True, thread_n=3):
    datas = find_data(TSDB_CONN['job_basic'], whereParam={'adj_factor': True, 'daily': True, 'cal_hfq': False},
                      selParam={'ts_code': True, 'start': True, 'end': True, '_id': False})
    if is_thread:
        code_list = np.array_split(np.asarray(datas), thread_n, axis=0)
    else:
        code_list = np.array_split(np.asarray(datas), 1, axis=0)
    for code_ls in code_list:
        t1 = threading.Thread(target=__cal_hfq_close, args=(code_ls,))
        t1.start()


def __cal_hfq_close(code_ls, fq_type='hfq'):
    i = 0
    job_len = len(code_ls)
    update_field = ''
    memo = ''
    if fq_type == 'hfq':
        update_field = 'hfq'
        memo = '后复权'
    elif fq_type == 'qfq':
        update_field = 'qfq'
        memo = '前复权'
        return
    else:
        return

    for code in code_ls:
        i += 1
        ts_code = code['ts_code']
        start = code['start']
        end = code['end']
        thread_name = threading.current_thread().name
        logger.info('%s计算%s %s[%s-%s]后复权数据 [%d/%d]\r' % (thread_name, memo, ts_code, start, end, i, job_len))
        close_data = find_data(TSDB_CONN['daily'],
                               whereParam={'ts_code':ts_code,'trade_date': {'$gte': start, '$lte':end}},
                               selParam={'ts_code': True, 'trade_date': True, 'close':True,'high':True,'open':True,'low':True, '_id': False})

        factor_data = find_data(TSDB_CONN['adj_factor'],
                                whereParam={'ts_code':ts_code , 'trade_date': {'$gte': start, '$lte':end}},
                                selParam={'ts_code': True, 'trade_date': True, 'adj_factor': True , '_id': False})

        if len(close_data) < 1:
            logger.info('不计算复权数据，没有日线数据');
            continue

        pd_close = pd.DataFrame(close_data)
        pd_close.set_index('trade_date', inplace=True)

        pd_factor = pd.DataFrame(factor_data)
        pd_factor.set_index('trade_date', inplace=True)
        if pd_factor.shape[0] != pd_close.shape[0]:
            logger.info('不计算复权数据，日线和复权因子没有同步');
            continue

        fq_data = pd.concat([pd_close, pd_factor], axis=1)
        fq_data['adj_factor'] = pd.to_numeric(fq_data['adj_factor'])
        fq_data['close'] = pd.to_numeric(fq_data['close'])
        fq_data['high'] = pd.to_numeric(fq_data['high'])
        fq_data['low'] = pd.to_numeric(fq_data['low'])
        fq_data['open'] = pd.to_numeric(fq_data['open'])
        if fq_type == 'hfq':
            fq_data[update_field+"close"] = fq_data['close']*fq_data['adj_factor']
            fq_data[update_field+"high"] = fq_data['high']*fq_data['adj_factor']
            fq_data[update_field + "low"] = fq_data['low'] * fq_data['adj_factor']
            fq_data[update_field + "open"] = fq_data['open'] * fq_data['adj_factor']
        elif fq_type == 'qfq':
            pass
        else:
            pass
        update_requests = []
        doc = {}
        my_index = {}
        for index,row in fq_data.iterrows():
            my_index['ts_code'] = ts_code
            my_index['trade_date'] = str(index)
            doc[update_field+'_close'] = row[update_field+'close']
            doc[update_field + '_high'] = row[update_field + 'high']
            doc[update_field + '_low'] = row[update_field + 'low']
            doc[update_field + '_open'] = row[update_field + 'open']
            doc['adj_factor'] = row['adj_factor']
            update_requests.append(UpdateOne(my_index, {'$set': doc}, upsert=False))
            doc = {}
            my_index = {}


        result, l = save_data_update_one(None, TSDB_CONN['daily'], update_requests,memo='daily更新%s数据'%memo)
        flag = result is not None and (result.matched_count > 0 or result.upserted_count > 0 or result.modified_count > 0)
        where_param = {'ts_code': ts_code, 'start': start, 'end': end}
        update_doc = {'cal_hfq': True}
        if flag:
            update_date(TSDB_CONN['job_basic'], where_param=where_param, update_doc=update_doc, memo='更新复权任务状态')





if __name__ == '__main__':
    init_base_schedule()
    daily_crawler_job('daily',is_thread=True, thread_n=4)
    daily_crawler_job('adj_factor', is_thread=True, thread_n=4)
    daily_crawler_job('suspend', is_thread=True)
    daily_crawler_job('daily_basic', is_thread=True, thread_n=4)
    cal_hfq_close(is_thread=True, thread_n=10)