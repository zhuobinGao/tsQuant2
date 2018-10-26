from initParam.R import pro
from tsDBUtil.MongoDBUtil import TSDB_CONN,save_data_update_one,find_data,update_date
from datetime import datetime, timedelta
from log.stock_log import logger
from pymongo import UpdateOne,ASCENDING
import numpy as np
import pandas as pd
import threading
from initParam.mysql_init import insert_many,mysql_execute,mysql_search

start1 = '19901201'
end1 = '20180920'
query_name = ['index_daily']


def init_base_schedule(start=start1, end=end1):
    data = mysql_execute("select * from job_basic_index where `start`='%s' and `end`='%s'" % (start, end))
    if data > 0:
        logger.info("已设置初始任务")
        return
    datas = mysql_search("select ts_code from index_basic order by ts_code")
    update_requests = []
    for data in datas:
        doc = {'index_code': data[0], 'start': start, 'end': end, 'index_daily': '0', 'index_weigth': '0'}
        update_requests.append(doc)
    df = pd.DataFrame(update_requests)
    insert_many('job_basic_index', df, memo='设置指数基础任务')


def index_daily_crawler_job(query_name, is_before7=False, is_thread=False, thread_n=3, fields=None):

    if is_before7:
        now = datetime.now()
        before = now - timedelta(days=7)
        end_date= now.strftime('%Y%m%d')
        start_date = before.strftime('%Y%m%d')
        init_base_schedule(start=start_date, end=end_date)

    # datas = find_data(TSDB_CONN['job_basic'], whereParam={query_name: False},
    #                   selParam={'ts_code': True, 'start': True, 'end': True, '_id': False})
    datas = mysql_search("select `index_code`,`start`,`end` from job_basic_index where `%s`=0 order by 1" % query_name)

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
        logger.info('%s抓取%s%s [%d/%d]' % (threading.current_thread().name,query_name,code,i,l))
        ts_code = code[0]
        start = code[1]
        end = code[2]
        data = None
        update_field = ''

        if query_name == 'index_daily':
            update_field = "index_daily"
            data = pro.index_daily(ts_code=ts_code, start_date=start, end_date=end)
        elif query_name == 'index_weigth':
            update_field = "index_weigth"
            data = pro.index_weight(index_code=ts_code, start_date=start, end_date=end)
        else :
            logger.info("不在列表之内")
            return
        memo = "执行任务"+query_name
        result = insert_many(query_name, data, memo=memo)

        memo = "更新执行任务" + query_name + "[%s]状态" % code
        if result is not None and result > 0:
            u_sql = "update job_basic_index set `%s`=1 where `index_code`='%s' and `start`='%s' and `end`='%s'"
            mysql_execute(u_sql % (update_field, ts_code, start, end), memo=memo)


def init_base_data():
    init_base_schedule()
    do_query_job(thread_n=5)


def do_query_job(thread_n=3):
    for name in query_name:
        index_daily_crawler_job(name, is_thread=True, thread_n=thread_n)


def set_before_job(before_day=7):
    now = datetime.now()
    before = now - timedelta(days=before_day)
    end_date = now.strftime('%Y%m%d')
    start_date = before.strftime('%Y%m%d')
    init_base_schedule(start=start_date, end=end_date)



if __name__ == "__main__":
    init_base_schedule()
    # index_daily_crawler_job('index_daily')
    index_daily_crawler_job('index_weigth')
