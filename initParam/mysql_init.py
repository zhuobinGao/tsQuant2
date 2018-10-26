import pymysql
import pandas as pd
from log.stock_log import logger

host = '127.0.0.1'
user = 'root'
pwd = '3362462'
mydb = 'tsquant'
port = 3306

def get_conn():
    return pymysql.connect(host, user, pwd, mydb, port)


def mysql_search(sql):
    # 打开数据库连接
    db = pymysql.connect(host, user, pwd, mydb, port)
    # 使用cursor()方法获取操作游标
    cur = db.cursor()
    try:
        cur.execute(sql)  # 执行sql语句
        results = cur.fetchall()  # 获取查询的所有记录
        my_list = list(results)
        return my_list
    except Exception as e:
        raise e
        return None
    finally:
        db.close()


def mysql_execute(sql , memo=""):
    # 打开数据库连接
    db = pymysql.connect(host, user, pwd, mydb, port)
    # 使用cursor()方法获取操作游标
    cur = db.cursor()
    try:
        result = cur.execute(sql)  # 执行sql语句
        db.commit()
        logger.info(memo)
        return result
    except Exception as e:
        raise e
        return None
    finally:
        cur.close()
        db.close()


def quant_exist_table(table_name):
    sql = "SELECT count(1) FROM INFORMATION_SCHEMA.tables where TABLE_SCHEMA='tsquant' and table_name='%s';"
    sql = sql % table_name
    my_list = mysql_search(sql)
    if my_list is None:
        return False
    if my_list[0][0] > 0:
        return True
    return False


def quant_create_table(table_name):
    sql = "SELECT table_field,field_type,is_pk,is_nn,is_uq,comment FROM ts_table_field " \
          "where table_name='%s' order by o_index"
    sql = sql % table_name
    data_s = mysql_search(sql)
    c_field = '';
    c_pk = ''
    c_uq = ''
    for data in data_s:
        is_nn = 'not null' if data[3] == 1 else ''
        c_field += "`%s` %s %s comment '%s',\n" % (data[0], data[1], is_nn, data[5])
        c_pk += data[0]+',' if data[2] == 1 else ''
        c_uq += data[0]+',' if data[4] == 1 else ''
    c_field = c_field[0: len(c_field)-2]
    c_pk = c_pk[0:len(c_pk)-1] if len(c_pk) > 1 else ''
    c_pk = ',\nPRIMARY KEY ( %s )' % c_pk if c_pk != '' else ''
    c_uq = c_uq[0:len(c_uq) - 1] if len(c_uq) > 1 else ''
    c_uq = ',\nUNIQUE( %s )\n' % c_uq if c_uq != '' else ''

    c_table = 'CREATE TABLE IF NOT EXISTS  %s (\n%s %s %s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'
    c_table = c_table % (table_name, c_field, c_pk, c_uq)
    result = mysql_execute(c_table)
    if result == 0:
        logger.info("成功创建表%s" % table_name)


def insert_many(table_name, df, memo=""):
    if df is None or len(df)<1:
        logger.info("数据为空")
        return
    # 初始化要插入的字段
    f_temp = ""
    f_values = ""
    for field in df.columns:
        f_temp += "`%s`," % field.strip()
        f_values += "%s,"
    f_temp = f_temp[0:len(f_temp)-1]
    f_values = f_values[0:len(f_values)-1]
    sql = "replace into %s(%s) values(%s)" % (table_name, f_temp, f_values)
    # 通过dataFrame构造数据列表
    values = []
    for index, row in df.iterrows():
        value = [None if pd.isna(row[field]) or row[field] is None else row[field] for field in df.columns]
        # value = [row[field] for field in df.columns]
        values.append(value)

    # print(df.columns)
    # print("*",values)
    # print(sql)

    # 打开数据库连接
    db = pymysql.connect(host, user, pwd, mydb, port)
    # 使用cursor()方法获取操作游标
    cur = db.cursor()
    try:
        result = cur.executemany(sql, values)  # 执行sql语句
        db.commit()
        logger.info((memo+" 影响%d行") % result)
        return result
    except Exception as e:
        print("*" * 20, table_name, sql,'\n', e)
        raise e

        return None
    finally:
        cur.close()
        db.close()






def init_all_table():
    sql = "SELECT table_name FROM tsquant.ts_table;"
    table_s = mysql_search(sql)
    for table in table_s:
        flag1 = quant_exist_table(table[0])
        if flag1:
            continue
        quant_create_table(table[0])


if __name__ == '__main__':
    flag = quant_exist_table('stock_basic')
    init_all_table()
