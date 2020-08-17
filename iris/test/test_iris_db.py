# -*- coding: utf-8 -*-

from iris.db import MysqlEngine
from iris.db import OracleEngine
from iris.db import SqliteEngine
import sys,os,logging
logging.basicConfig(level = logging.INFO,format = '%(levelno)s - %(pathname)s - %(filename)s - %(funcName)s - %(lineno)d - %(thread)d - %(threadName)s - %(process)d - %(message)s')
logger = logging.getLogger(__name__)


def test_mysql():
    #mysqlHelper =  MysqlHelper("109.6.13.67","zsyw",3306,"sysdba","admin123");
    mysqlHelper = MysqlEngine.MysqlHelper("trading")
    #mysqlHelper = MysqlHelper(None,ip="109.6.13.67",dbuser="sysdba",dbpass="admin123",db="zsyw",port=3306);
    sql="select * from fhts_appindex where appcode='{0}'".format("BF-UNAC(ZJFH)")
    print(sql)
    print(mysqlHelper.get_all(sql))
    df=mysqlHelper.get_df(sql)
    print(df)
    mysqlHelper.insert_df(df,"test")
    mysqlHelper.dispose(1)

def test_oracle():
    oracleHelper =  OracleEngine.OracleHelper("iomp");
    #oracleHelper =  OracleHelper(None,dbuser="doms",dbpass="doms",ip="109.6.13.166",sid="oradb",port="1521");
    sql="update ECC_TRADE_PEAK set checktime='20200730'"
    sql="select  TRADEID, CHECKTIME, MAX_TRADE from  ECC_TRADE_PEAK    t where rownum <2"
    df=oracleHelper.get_df(sql)
    logger.info(df)
    oracleHelper.insert_df(df,"ECC_TRADE_PEAK")
    oracleHelper.dispose(1)

def test_sqlite():
    mysqlHelper = MysqlEngine.MysqlHelper("trading")
    sql="select * from test "
    df = mysqlHelper.get_df(sql)
    print(df)
    sqliteHelper = SqliteEngine.SqliteHelper("sqlite")
    sqliteHelper.insert_df(df,"appindex")

    sql="select * from appindex"
    df_sqlite=sqliteHelper.get_df(sql)
    print(df_sqlite)

    #sql="delete from appindex"
    #sqliteHelper.update(sql)

    result=sqliteHelper.get_all(sql)
    for res in result:
        print(res)
        print(res[0])



    sqliteHelper.dispose(1)
    mysqlHelper.dispose(1)

if __name__ == '__main__':
    print(sys.path)
    print(os.getcwd())
    os.chdir('../..')
    print(os.getcwd())


    #test_oracle()
    #test_mysql()
    test_sqlite()