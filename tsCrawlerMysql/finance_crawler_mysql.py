from log.stock_log import logger
import numpy as np
import threading
from enum import Enum
from initParam.R import pro
from datetime import datetime,timedelta
from initParam.mysql_init import insert_many,mysql_execute,mysql_search
import pandas as pd
import os.path


start1 = '19901201'
end1 = '20180920'


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
    data = mysql_execute("select * from job_finance_basic where `start`='%s' and `end`='%s'" % (start, end))
    if data > 0:
        logger.info("已经设置财务任务[%s-%s]%d" % (start, end, data))
        return
    data_s = mysql_search("select ts_code from stock_basic order by ts_code")
    update_requests = []
    for data in data_s:
        doc = {'ts_code': data[0], 'start': start, 'end': end,
               'income': '0', 'balancesheet': '0', 'cashflow': '0',
               'forecast': '0', 'express': '0', 'fina_indicator': '0',
               "fina_audit": '0', "fina_mainbz": '0'}
        update_requests.append(doc)
    df = pd.DataFrame(update_requests)
    insert_many('job_finance_basic', df, memo='设置财务数据基础任务')


def set_before_job(before_day=7):
    now = datetime.now()
    before = now - timedelta(days=before_day)
    end_date = now.strftime('%Y%m%d')
    start_date = before.strftime('%Y%m%d')
    init_finance_job_base(start=start_date, end=end_date)


def init_base_data():
    init_finance_job_base()
    do_finance_job(thread_n=5)


def do_finance_job(thread_n=5):
    for query_name in QueryName:
        if query_name ==QueryName.income:
            continue
        finance_crawler_job(query_name, thread_n=thread_n)


def finance_crawler_job(query_name, thread_n=3):
    data = mysql_search("select `ts_code`,`start`,`end` from job_finance_basic where `%s`=0" % query_name.value)
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
        ts_code = code[0]
        start = code[1]
        end = code[2]
        data = None
        update_field = query_name.value
        if query_name == QueryName.income:
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.balance_sheet:
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.cash_flow:
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.forecast:
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.express:
            data = pro.query(query_name.value, ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.fina_indicator:
            __get_fina_indicator(query_name, ts_code, start, end)
            continue
        elif query_name == QueryName.fina_audit:
            data = pro.fina_audit(ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == QueryName.fina_main_bz:
            pro.fina_mainbz(ts_code=ts_code, start_date=start, end_date=end)
        else:
            logger.info("不在业务范围内")
            return

        memo = "执行任务" + query_name.value
        res = insert_many(query_name.value, data, memo=memo)
        memo = "更新执行任务" + query_name.value + "[%s]状态" % code
        if res is not None and res > 0:
            u_sql = "update job_finance_basic set `%s`=1 where `ts_code`='%s' and `start`='%s' and `end`='%s'"
            mysql_execute(u_sql % (update_field, ts_code, start, end), memo=memo)


def __get_fina_indicator(query_name, code, start, end):
    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')
    date_list = []
    memo = "%s子线程执行%s[%s-%s]任务[%d/%d]" + query_name.value
    while start_date < end_date:
        temp_date = start_date+timedelta(days=360*10)
        temp_date = end_date if temp_date > end_date else temp_date
        start_str = start_date.strftime('%Y%m%d')
        end_str = temp_date.strftime('%Y%m%d')
        date_list.append({'start':start_str, 'end': end_str})
        start_date = temp_date
    all_l = 0
    i = 0
    l = len(date_list)
    for date in date_list:
        i += 1
        memo = "%s子线程执行%s[%s-%s]任务[%d/%d]%s"\
               %(threading.current_thread().name,code,date['start'], date['end'], i, l, query_name.value)
        logger.info(memo)
        data = pro.query(query_name.value, ts_code=code, start_date=date['start'], end_date=date['end'])
        res = insert_many('fina_indicator', data, memo=memo)
        memo = "更新执行任务" + query_name.value + "[%s][%s-%s]状态" % (code, date['start'],date['end'])
        all_l += res if res is not None else 0
    if all_l>0:
        u_sql = "update job_finance_basic set `%s`=1 where `ts_code`='%s' and `start`='%s' and `end`='%s'"
        mysql_execute(u_sql % ('fina_indicator', code, start, end), memo=memo)


def __get_indicator_field():
    path = os.path.dirname(os.getcwd())+'\\tsCrawler\\z_finance_indicator_field.txt'
    f = open(path, 'r')
    fields = ''
    for line in f.readlines():
        fields += line.strip()
    return fields


if __name__ == '__main__':
    # income = 'income'
    # balance_sheet = 'balancesheet'
    # cash_flow = 'cashflow'
    # forecast = 'forecast'
    # express = 'express' /f
    # fina_indicator = 'fina_indicator'
    # fina_audit = 'fina_audit'
    # fina_main_bz = 'fina_mainbz'
    # do_finance_job()
    init_finance_job_base()
    do_finance_job()
    # finance_crawler_job(QueryName.fina_indicator, thread_n=1)