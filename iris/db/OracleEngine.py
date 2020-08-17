# coding:utf-8
# sqlalchemy version should >= s1.2.0
from iris.db import DBHelper
import cx_Oracle as orcl
import pandas as pd
from sqlalchemy import create_engine
import  configparser
import logging.config
logging.basicConfig(level = logging.INFO,format = '%(levelno)s - %(pathname)s - %(filename)s - %(funcName)s - %(lineno)d - %(thread)d - %(threadName)s - %(process)d - %(message)s')
logger = logging.getLogger(__name__)


class OracleHelper(DBHelper._DBHelper):
    def __init__(self, dsname, **kw):
        try:
            if (dsname is None):
                self.__dict__.update(kw)
                self.connstr = self.dbuser + '/' + self.dbpass + '@' + self.ip + ':' + self.port + '/' + self.sid
                self.conn = OracleHelper.get_conn(self)
                self.cursor = self.conn.cursor()
            else:
                dbconfig = configparser.ConfigParser()
                dbconfig.read("iris/conf/datasource.ini")
                self.ip = dbconfig[dsname]['ip']
                self.dbuser = dbconfig[dsname]['dbuser']
                self.dbpass = dbconfig[dsname]['dbpass']
                self.sid = dbconfig[dsname]['sid']
                self.port = dbconfig[dsname]['port']
                self.connstr = self.dbuser + '/' + self.dbpass + '@' + self.ip + ':' + self.port + '/' + self.sid
                self.conn = OracleHelper.get_conn(self)
                self.cursor = self.conn.cursor()
                logger.info("[init Oracle datasource %s succeed ]" % dsname)

        except Exception as e:
            logger.error(str(e))
            self.cursor = False

    def get_conn(self):
        try:
            oracleConn =  self.dbconn=orcl.connect(self.connstr)
            return oracleConn
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
        conn_string='oracle+cx_oracle://%s:%s@%s:%s/%s'%(self.dbuser,self.dbpass,self.ip,self.port,self.sid)
        engine = create_engine(conn_string)
        con = engine.connect()
        tbname=tbname.lower()
        df.to_sql(name=tbname, con=con, if_exists=if_exists_para, index=False)

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


    oracleHelper =  OracleHelper(None,dbuser="doms",dbpass="doms",ip="109.6.13.166",sid="oradb",port="1521");
    #oracleHelper = OracleHelper("iomp")
    sql="update ECC_TRADE_PEAK set checktime='20200730'"
    sql="select * from ECC_TRADE_PEAK"
    df=oracleHelper.get_df(sql)
    print(df)
    oracleHelper.insert_df(df,"ECC_TRADE_PEAK")
