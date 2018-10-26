import tushare as ts
from tsDBUtil.MongoDBUtil import TSDB_CONN,save_data_update_one
from datetime import datetime, timedelta
from initParam.mysql_init import insert_many


def stock_basic_crawler():
    ts.set_token('7e33fd87cfa25664c9b20f637b9d75ce613aea3d57d7d41ba66cebcc')
    pro = ts.pro_api()
    fields = 'ts_code,symbol,name,fullname,enname,exchange_id,curr_type,list_date,list_status,delist_date,is_hs'
    data = pro.query('stock_basic', fields=fields)
    # index = ['ts_code', 'symbol']
    insert_many('stock_basic', data , memo='更新股票列表')


def index_basic_crawler():
    ts.set_token('7e33fd87cfa25664c9b20f637b9d75ce613aea3d57d7d41ba66cebcc')
    pro = ts.pro_api()
    fields = 'ts_code,name,fullname,market,publisher,index_type,category,' \
             'base_date,base_point,list_date,weight_rule,desc,exp_date'
    market_list = ['MSCI','CSI','SSE','SZSE','CICC','SW','CNI','OTH']
    for market in market_list:
        data = pro.index_basic(market=market, fields=fields)
        insert_many('index_basic', data, memo='更新股票指数基本信息%s' % market)


def trade_cal_crawler(start_date='19901220', end_date='20190101', is_before7=False):
    ts.set_token('7e33fd87cfa25664c9b20f637b9d75ce613aea3d57d7d41ba66cebcc')
    pro = ts.pro_api()
    fields = 'exchange_id,cal_date,is_open,pretrade_date'
    if is_before7:
        now = datetime.now()
        before = now - timedelta(days=7)
        end_date= now.strftime('%Y%m%d')
        start_date = before.strftime('%Y%m%d')
    data = pro.query('trade_cal', start_date=start_date, end_date=end_date, fields=fields)
    # index = ['exchange_id', 'cal_date']
    insert_many('trade_cal', data, memo='更新股票交易日期')


if __name__ == '__main__':
    # stock_basic_crawler()
    # trade_cal_crawler()
    index_basic_crawler()