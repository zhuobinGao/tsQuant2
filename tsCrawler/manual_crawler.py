from tsCrawler.daily_crawler import *
from tsCrawler.basic_crawler import *


if __name__ == '__main__':
    now_date = datetime.now();
    pre_date = datetime.now() - timedelta(days=3)
    now_str = now_date.strftime('%Y%m%d')
    pre_str = pre_date.strftime('%Y%m%d')
    init_base_schedule(start=pre_str, end=now_str)
    daily_crawler_job('daily', is_thread=True, thread_n=7)
    daily_crawler_job('adj_factor', is_thread=True, thread_n=20)
    daily_crawler_job('suspend', is_thread=True ,thread_n=7)
    daily_crawler_job('daily_basic', is_thread=True, thread_n=7)
    # cal_hfq_close(is_thread=True, thread_n=12)
