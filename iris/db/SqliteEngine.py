# coding:utf-8

from iris.db import DBHelper
import pandas as pd
from sqlalchemy import create_engine
import configparser
import logging.config
import traceback
import sqlite3
logging.basicConfig(level = logging.INFO,format = '%(levelno)s - %(pathname)s - %(filename)s - %(funcName)s - %(lineno)d - %(thread)d - %(threadName)s - %(process)d - %(message)s')
logger = logging.getLogger(__name__)


class SqliteHelper(DBHelper._DBHelper):
    def __init__(self, dsname, **kw):
        try:
            if (dsname is None):
                self.__dict__.update(kw)
                self.conn = SqliteHelper.get_conn(self)
                self.cursor = self.conn.cursor()
            else:
                dbconfig = configparser.ConfigParser()
                dbconfig.read("iris/conf/datasource.ini")
                self.dbpath = dbconfig[dsname]['dbpath']
                self.conn = SqliteHelper.get_conn(self)
                self.cursor = self.conn.cursor()
                logger.info("[init sqlite datasource %s succeed ]" % dsname)


        except Exception as e:
            logger.error("[init datasource %s failed ]" % dsname)
            logger.error('Exception: ', e)
            traceback.format_exc()
            self.cursor = False

    def get_conn(self):
        try:
            sqliteConn = sqlite3.connect(self.dbpath)
            return sqliteConn
        except Exception as e:
            raise e


    def get_all(self, sql,param=None):
        if param is None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, param)

        result = self.cursor.fetchall()
        return result

    def get_one(self, sql,param=None):
        if param is None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, param)

        result = self.cursor.fetchone()
        return result

    def update(self, sql,param=None):
        if param is None:
            result = self.cursor.execute(sql)
        else:
            result = self.cursor.execute(sql, param)
        self.conn.commit()
        return result


    def get_df(self, sql):
        df=pd.read_sql(sql,self.conn)
        return df


    def insert_df(self, df, tbname,if_exists_para="append"):
        df.to_sql(name=tbname, con=self.conn, if_exists=if_exists_para, index=False)

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


