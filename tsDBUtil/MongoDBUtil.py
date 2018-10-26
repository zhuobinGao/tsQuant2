from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
from log.stock_log import logger
from bson import json_util

# DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['myQuant']
TSDB_CONN = MongoClient('mongodb://127.0.0.1:27017')['tsQuant']


def save_data_update_one(data, collection, update_requests = [], indexs=None, extra_fields=None, memo=''):
    """

    :param data: 抓取的数据
    :param collection:  要保存的数据集
    :param indexs: updateOne 的键值
    :param extra_field: 额外的字段
    :param memo: 日志说明字段
    :return:
    """
    update_result = None
    if data is not None:
        update_requests = __daily_obj_2_doc(data, indexs=indexs, extra_fields=None)
    if len(update_requests) > 0:
        update_requests = update_requests
    if len(update_requests) > 0:
        update_result = collection.bulk_write(update_requests, ordered=False)
        logger.info('保存%s数据共%d条 匹配%d条 插入%d条, 更新%d条 额外字段:%s ' %
                    (memo, len(update_requests), update_result.matched_count, update_result.upserted_count,
                     update_result.modified_count, extra_fields))
    else:
        logger.info('无数据更新%d'%len(update_requests))

    return update_result,len(update_requests)


def find_data(collection, whereParam= {}, selParam = {},sort = None):

    if sort is not None:
        data = collection.find(whereParam, selParam).sort(sort)
    else:
        data = collection.find(whereParam, selParam)
    results = [result for result in data]
    return results


def find_simple_data(collection, whereParam={}, selParam={}, sort=None):
    if sort is not None:
        data = collection.find(whereParam, selParam).sort(sort)
    else:
        data = collection.find(whereParam, selParam)
    return list(data)


def update_date(collection, where_param={}, update_doc={}, memo=''):
    result = collection.update(where_param,{"$set": update_doc})
    logger.info(memo)


def __daily_obj_2_doc(data, indexs=None, extra_fields=None):
    update_requests = []
    my_column = [column for column in data.columns]
    for df_index in data.index:
        doc = {}
        my_index = {}
        d_object = data.loc[df_index]
        for column in my_column:
           doc[column] = str(d_object[column])
        if indexs is not None:
            for column in indexs:
                my_index[column] = doc[column]
        if extra_fields is not None:
            doc.update(extra_fields)
        update_requests.append(UpdateOne(my_index, {'$set': doc}, upsert=True))
    return update_requests
