# -*- coding: utf-8 -*-
import os,csv,json,time,datetime,platform
from urllib import parse,request
import configparser
from urllib import parse,request
import logging.config
import pandas as pd
from iris.db import MysqlEngine
from iris.db import OracleEngine

import logging.config

logging.basicConfig(level = logging.INFO,format = '%(levelno)s - %(pathname)s - %(filename)s - %(funcName)s - %(lineno)d - %(thread)d - %(threadName)s - %(process)d - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    os.chdir('../..')

    header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko'}
    url = 'http://76.7.34.11:8060/api/get_amc_s/'

    oracleHelper = OracleEngine.OracleHelper('iomp')
    channel_id_sql='select distinct tradeid,channel_id,area from ECC_CHANNEL_ID'
    result=oracleHelper.get_all(channel_id_sql)
    for res in result:
        url = url+res[0]+","
    print(url)
    req = request.Request(url='%s' % (url), headers=header_dict)
    res = request.urlopen(req)
    retData = res.read().decode('UTF-8')
    retData = json.loads(retData)
    #logger.info(retData)
    valarr = []
    for res in result:
        val = []
        tradeid=res[0]
        if(tradeid not in retData):
            logger.info(tradeid + " not in retData")
            continue


        tradeinfo = retData[tradeid]
        val.append(tradeid)
        val.append(tradeinfo['time'])
        val.append(tradeinfo['sum_amount'])
        val.append(tradeinfo['suc_rate'])
        val.append(tradeinfo['max_trade_num'])
        val.append(tradeinfo['max_sum_trade_num'])
        val.append(tradeinfo['trade_num'])
        val.append(tradeinfo['name'])
        val.append(tradeinfo['sum_trade_num'])
        val.append(tradeinfo['max_trade_date'])
        val.append(tradeinfo['max_sum_date'])
        val.append(tradeinfo['amount'])
        val.append(tradeinfo['s_suc_rate'])
        val.append(tradeinfo['response_time'])
        valarr.append(val)

    df = pd.DataFrame(valarr, columns=['TRADEID', 'checktime', 'sum_amount', 'suc_rate', 'max_trade_num',
                                       'max_sum_trade_num','trade_num','name','sum_trade_num','max_trade_date','max_sum_date',
                                       'amount','s_suc_rate','response_time'])
    logger.info(df)
    oracleHelper.insert_df(df, "ECC_CHANNEL_DATA")
    logger.info("insert into ECC_CHANNEL_DATA finished")
    deletesql="delete from ECC_CHANNEL_DATA where    tradeid||checktime not in(select tradeid||max(checktime) from ECC_CHANNEL_DATA group by tradeid )"
    oracleHelper.update(deletesql)
    logger.info("delete from  ECC_CHANNEL_DATA finished")
    oracleHelper.dispose(1)









