from tsDBUtil.MongoDBUtil import *
from log.stock_log import logger
import numpy as np
import threading
from enum import Enum
from initParam.R import pro
from datetime import datetime,timedelta
import os.path

start1 = '19901201'
end1 = '20180826'


class QueryName(Enum):
    income = 'income'
    balance_sheet = 'balancesheet'
    cash_flow = 'cashflow'
    forecast = 'forecast'
    express = 'express'
    fina_indicator = 'fina_indicator'
    fina_audit = 'fina_audit'
    fina_main_bz = 'fina_mainbz'


def init_finance_job_base(start=start1, end=end1):
    data = find_data(TSDB_CONN['job_finance_basic'], whereParam={'start': start, 'end': end})
    if len(data)>0:
        logger.info("已经设置财务任务[%s-%s]"% (start, end))
        return;
    data_s = find_data(TSDB_CONN['stock_basic'], selParam={'_id': False, 'ts_code': True})
    update_requests = []
    for data in data_s:
        doc = {'ts_code': data['ts_code'], 'start': start, 'end': end,
               'income': False, 'balancesheet': False, 'cashflow': False,
               'forecast': False, 'express': False, 'fina_indicator': False,
               "fina_audit": False, "fina_mainbz": False}
        index = {'ts_code': data['ts_code'], 'start':start, 'end':end}
        update_requests.append(UpdateOne(index, {'$set': doc}, upsert=True))
    save_data_update_one(None, TSDB_CONN['job_finance_basic'], update_requests=update_requests, memo='设置财务数据基础任务')



def finance_crawler_job(query_name, thread_n=3):
    data = find_data(TSDB_CONN['job_finance_basic'], whereParam={query_name.value: False},
                      selParam={'ts_code': True, 'start': True, 'end': True, '_id': False})
    code_list = np.array_split(np.asarray(data), thread_n, axis=0)
    for code_ls in code_list:
        t1 = threading.Thread(target=__finance_crawler_job, args=(query_name, code_ls))
        t1.start()


def __finance_crawler_job(query_name, code_ls):
    i = 0
    l = len(code_ls)
    for code in code_ls:
        i += 1
        logger.info('%s抓取%s%s [%d/%d]' % (threading.current_thread().name, query_name.value, code, i, l))
        ts_code = code['ts_code']
        start = code['start']
        end = code['end']
        data = None
        update_field = query_name.value
        if query_name == QueryName.income:
            index = ['ts_code', 'ann_date', 'report_type']
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.balance_sheet:
            index = ['ts_code', 'ann_date', 'report_type']
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.cash_flow:
            index = ['ts_code', 'ann_date', 'report_type']
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.forecast:
            index = ['ts_code', 'ann_date', 'type']
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.express:
            index = ['ts_code', 'ann_date']
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.fina_indicator:
            indicator_field = __get_indicator_field()
            __get_fina_indicator(query_name, ts_code, start, end, indicator_field)
            continue
        elif query_name == QueryName.fina_audit:
            index = ['ts_code', 'ann_date']
            data = pro.fina_audit(ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.fina_main_bz:
            index = ['ts_code', 'end_date']
            pro.fina_mainbz(ts_code=ts_code, start_date=start, end_date=end)
        else:
            logger.info("不在业务范围内")
            return

        memo = "执行任务" + query_name.value
        where_param = {'ts_code': ts_code, 'start': start, 'end': end}
        update_doc = {update_field: True}
        res, r_len = save_data_update_one(data, TSDB_CONN[query_name.value], indexs=index, memo=memo)
        memo = "更新执行任务" + query_name.value + "[%s]状态" % code
        flag = res is not None and (res.matched_count > 0 or res.upserted_count > 0 or res.modified_count > 0)
        flag = flag or r_len == 0
        if flag:
            update_date(TSDB_CONN['job_finance_basic'], where_param=where_param, update_doc=update_doc, memo=memo)


def __get_fina_indicator(query_name, code, start, end, fields):
    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')
    date_list = []
    memo = "%s子线程执行%s[%s-%s]任务[%d/%d]" + query_name.value
    where_param = {'ts_code': code, 'start': start, 'end': end}
    index = ['ts_code', 'ann_date', 'end_date']
    update_doc = {'fina_indicator': True}
    while start_date <= end_date:
        temp_date = start_date+timedelta(days=360)
        temp_date = end_date if temp_date > end_date else temp_date
        start_str = start_date.strftime('%Y%m%d')
        end_str = temp_date.strftime('%Y%m%d')
        date_list.append({'start':start_str, 'end':end_str})
        start_date = temp_date
    all_l = 0
    i = 0
    l = len(date_list)
    for date in date_list:
        i += 1
        memo = "%s子线程执行%s[%s-%s]任务[%d/%d]%s"\
               %(threading.current_thread().name,code,date['start'], date['end'], i,l,query_name.value)
        logger.info(memo)
        data = pro.query(query_name.value, ts_code=code, start_date=date['start'], end_date=date['end'], fields=fields)
        res, r_len = save_data_update_one(data, TSDB_CONN[query_name.value], update_requests=[], indexs=index, memo=memo)
        memo = "更新执行任务" + query_name.value + "[%s][%s-%s]状态" % (code, date['start'],date['end'])
        all_l += r_len
    if all_l>0:
        update_date(TSDB_CONN['job_finance_basic'], where_param=where_param, update_doc=update_doc, memo=memo)


def __get_indicator_field():
    path = os.path.dirname(os.getcwd())+'\\tsCrawler\\z_finance_indicator_field.txt'
    f = open(path, 'r')
    fields = ''
    for line in f.readlines():
        fields += line.strip()
    return fields


def do_finance_job(before_day=7):
    init_finance_job_base()
    now = datetime.now()
    before = now - timedelta(days=before_day)
    end_date = now.strftime('%Y%m%d')
    start_date = before.strftime('%Y%m%d')
    init_finance_job_base(start=start_date, end=end_date)
    for query_name in QueryName:
        thread_n = 10 if query_name == QueryName.fina_indicator else 8
        finance_crawler_job(query_name, thread_n=thread_n)


if __name__ == '__main__':
    do_finance_job()
