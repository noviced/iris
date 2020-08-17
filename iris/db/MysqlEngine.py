# coding:utf-8

from iris.db import DBHelper
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import configparser
import logging.config
import traceback

logging.basicConfig(level = logging.INFO,format = '%(levelno)s - %(pathname)s - %(filename)s - %(funcName)s - %(lineno)d - %(thread)d - %(threadName)s - %(process)d - %(message)s')
logger = logging.getLogger(__name__)


class MysqlHelper(DBHelper._DBHelper):

    def __init__(self, dsname, **kw):
        try:
            if(dsname is None):
                self.__dict__.update(kw)
                self.conn = MysqlHelper.get_conn(self)
                self.cursor = self.conn.cursor()
            else:
                dbconfig=configparser.ConfigParser()
                dbconfig.read("iris/conf/datasource.ini")
                print(dbconfig[dsname]['ip'])
                self.ip=dbconfig[dsname]['ip']
                self.dbuser = dbconfig[dsname]['dbuser']
                self.dbpass = dbconfig[dsname]['dbpass']
                self.db = dbconfig[dsname]['db']
                self.port = int(dbconfig[dsname]['port'])
                self.conn = MysqlHelper.get_conn(self)
                self.cursor = self.conn.cursor()
                logger.info("[init Mysql datasource %s succeed ]" % dsname)


        except Exception as e:
            logger.error("[init datasource %s failed ]"% dsname)
            logger.error('Exception: ', e)
            traceback.format_exc()
            self.cursor = False

    def get_conn(self):
        try:
            MysqlConn = pymysql.connect(host=self.ip, user=self.dbuser,
                                        passwd=self.dbpass, db=self.db,
                                        port=self.port,
                                        charset='utf8', cursorclass=pymysql.cursors.DictCursor)
            return MysqlConn
        except Exception as e:
            raise e


    def get_all(self, sql,param=None):
        if param is None:
            count = self.cursor.execute(sql)
        else:
            count = self.cursor.execute(sql, param)

        result = self.cursor.fetchall()
        return result

    def get_one(self, sql,param=None):
        if param is None:
            count = self.cursor.execute(sql)
        else:
            count = self.cursor.execute(sql, param)
        if count > 0:
            result = self.cursor.fetchone()
        else:
            result = None
        return result

    def update(self, sql,param=None):
        if param is None:
            result = self.cursor.execute(sql)
        else:
            result = self.cursor.execute(sql, param)
        return result


    def get_df(self, sql):
        df=pd.read_sql(sql,self.conn)
        return df


    def insert_df(self, df, tbname,if_exists_para="append"):
        conn_string ="mysql+mysqldb://{}:{}@{}/{}?charset=utf8".format(self.dbuser, self.dbpass, "%s:%d"%(self.ip,self.port), self.db)
        engine = create_engine(conn_string)
        con = engine.connect()
        df.to_sql(name=tbname, con=con, if_exists=if_exists_para, index=False)

    def insert_many(self, sql, values):
        count = self.cursor.executemany(sql, values)
        self.conn.commit()
        return count



    def dispose(self, is_end=1):
        try:
            if is_end == 1:
                self.conn.commit()
            else:
                self.rollback()
            self.cursor.close()
            self.conn.close()
        except Exception:
            return False


if __name__ == '__main__':
    #mysqlHelper =  MysqlHelper("109.6.13.67","zsyw",3306,"sysdba","admin123");
    mysqlHelper = MysqlHelper("zsyw")
    #mysqlHelper = MysqlHelper(None,ip="109.6.13.67",dbuser="sysdba",dbpass="admin123",db="zsyw",port=3306);
    sql="select * from fhts_appindex where appcode='{0}'".format("BF-UNAC(ZJFH)")
    print(sql)
    print(mysqlHelper.get_all(sql))
    #df=mysqlHelper.get_df(sql)
    #print(df)
    #mysqlHelper.insert_df(df,"test")
    mysqlHelper.dispose(1)
