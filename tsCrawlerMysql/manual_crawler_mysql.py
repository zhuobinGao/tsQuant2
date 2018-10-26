from tsCrawlerMysql.daily_crawler_mysql import init_base_data as dy_init
from tsCrawlerMysql.finance_crawler_mysql import init_base_data as fn_init
from tsCrawlerMysql.index_crawler_mysql import init_base_data as index_init
from tsCrawlerMysql.basic_crawler_mysql import *

if __name__ == '__main__':
    # stock_basic_crawler()
    # index_basic_crawler()

    # dy_init()
    fn_init()
    # index_init()
