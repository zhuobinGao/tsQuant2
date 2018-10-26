import schedule
from datetime import datetime, timedelta
from tsCrawler.daily_crawler import *
from tsCrawler.basic_crawler import *

from log.stock_log import logger
import time


def stock_daily_crawler(myday=7):
    now_date = datetime.now();
    pre_date = datetime.now()-timedelta(days=myday)
    now_str = now_date.strftime('%Y%m%d')
    pre_str = pre_date.strftime('%Y%m%d')

    init_base_schedule(start=pre_str, end=now_str)
    daily_crawler_job('daily', is_thread=True, )
    daily_crawler_job('adj_factor', is_thread=True)
    daily_crawler_job('suspend', is_thread=True)
    daily_crawler_job('daily_basic', is_thread=True)
    cal_hfq_close(is_thread=True, thread_n=2)

def week_baic_crawle():
    stock_basic_crawler()
    trade_cal_crawler()



if __name__ == '__main__':

    schedule.every().day.at("15:30").do(stock_daily_crawler)
    schedule.every().day.at("20:30").do(stock_daily_crawler)
    schedule.every(30).minutes.do(cal_hfq_close)
    schedule.every().friday.at("11:30").do(week_baic_crawle)


    logger.info("启动stock_schedule_daily.")
    while True:
        schedule.run_pending()
        time.sleep(10)
