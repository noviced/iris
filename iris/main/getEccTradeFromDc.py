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

    header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko'}
    url = 'http://76.7.34.11:8060/api/get_amc_s/TSM04986,TSM06939,TSM06937,TSM06938,TSM07062,TSM07063,TSM07061,TSM00931,TSM01031,TSM00999,TSM00988,TSM00964,' \
          'TSM34456,TSM34511,TSM34514,TSM34546,TSM34572,TSM34622,TSM34642,TSM34711,TSM34796,TSM34801/'
    req = request.Request(url='%s' % (url), headers=header_dict)
    res = request.urlopen(req)
    retData = res.read().decode('UTF-8')
    retData = json.loads(retData)
    logger.info(retData)
    indmap = {}
    indmap['TSM04986'] = 'ZJ_JYL'
    indmap['TSM06939'] = 'DZ'
    indmap['TSM06937'] = 'HZF'
    indmap['TSM06938'] = 'QT'
    indmap['TSM07062'] = 'DHYH'
    indmap['TSM07063'] = 'WSYH'
    indmap['TSM07061'] = 'SJYH'
    indmap['TSM00931'] = 'ZHQZ'
    indmap['TSM01031'] = 'ATM'
    indmap['TSM00999'] = 'POS'
    indmap['TSM00988'] = 'BSM'
    indmap['TSM00964'] = 'GM'
    indmap['TSM34456'] = 'GM1211'
    indmap['TSM34511'] = 'GM1203'
    indmap['TSM34514'] = 'GM1210'
    indmap['TSM34546'] = 'GM1209'
    indmap['TSM34572'] = 'GM1205'
    indmap['TSM34622'] = 'GM1204'
    indmap['TSM34642'] = 'GM1208'
    indmap['TSM34711'] = 'GM1207'
    indmap['TSM34796'] = 'GM1206'
    indmap['TSM34801'] = 'GM1202'



    valarr=[]
    for j in indmap:
        arr = retData[j]
        TRADEID = indmap[j]
        val = []
        val.append(TRADEID)
        val.append(arr['time'])
        val.append(arr['trade_num'])
        val.append(arr['sum_trade_num'])
        val.append(arr['max_trade_num'])
        val.append(arr['max_sum_trade_num'])
        valarr.append(val)

    df = pd.DataFrame(valarr,columns=['TRADEID','checktime','trade_num','sum_trade_num','LASTWEEK_TRADE_NUM','LASTWEEK_SUM_TRADE_NUM'])
    print(df)
    os.chdir('../..')
    oracleHelper = OracleEngine.OracleHelper("iomp")
    oracleHelper.insert_df(df,"ECC_TRADEDATA")






