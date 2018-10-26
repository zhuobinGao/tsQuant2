import tushare as ts
from tsDBUtil.MongoDBUtil import TSDB_CONN,save_data_update_one
from datetime import datetime, timedelta


def stock_basic_crawler():
    ts.set_token('7e33fd87cfa25664c9b20f637b9d75ce613aea3d57d7d41ba66cebcc')
    pro = ts.pro_api()
    fields = 'ts_code,symbol,name,fullname,enname,exchange_id,curr_type,list_date,list_status,delist_date,is_hs'
    data = pro.query('stock_basic', fields=fields)
    index = ['ts_code', 'symbol']
    save_data_update_one(data, TSDB_CONN['stock_basic'], indexs=index, memo='更新股票列表')


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
    index = ['exchange_id', 'cal_date']
    save_data_update_one(data, TSDB_CONN['trade_cal'], indexs=index, memo='更新交易日历')

if __name__ == '__main__':
    stock_basic_crawler()
    trade_cal_crawler()