# -*- coding: utf-8 -*-

from iris.db import MysqlEngine
from iris.db import OracleEngine
import sys,os,logging,json,datetime
logging.basicConfig(level = logging.INFO,format = '%(levelno)s - %(pathname)s - %(filename)s - %(funcName)s - %(lineno)d - %(thread)d - %(threadName)s - %(process)d - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    os.chdir('../..')
    start = datetime.datetime.now()
    oracleHelper = OracleEngine.OracleHelper("iomp");
    oraclehelper_dboper= OracleEngine.OracleHelper("dboper");
    mysqlHelper = MysqlEngine.MysqlHelper("predict")
    ctime = (datetime.datetime.now() - datetime.timedelta(minutes=15)).strftime("%Y%m%d%H%M")
    cleantime = (datetime.datetime.now() - datetime.timedelta(hours=2160)).strftime("%Y-%m-%d")


    fssql = "select SERVERIP, CHECKTIME||'00' as checktime, INDID, VAL, RSV1, RSV2, RSV3," \
          " RSV4 from CHECK_DTCHKDATA where (indid='7' or indid='8') and checktime like '" + ctime + "%'"
    fs_df=oracleHelper.get_df(fssql)
    mysqlHelper.insert_df(fs_df,'check_dtchkdata')

    logger.info("collect fs data  [%d] finished" %len(fs_df))

    appdatasql = "select d.appindid,d.checktime,val,node,d.rsv1,d.rsv2,d.rsv3,appcode,appname,subappcode,indcode,indname, i.rsv1 as area " \
          "from CHECK_APP_DTCHKDATA d," \
          "CHECK_APP_DTCHKINDEX i where i.appindid=d.appindid and   checktime like '" + ctime + "%'"

    appdata_df = oracleHelper.get_df(appdatasql)
    mysqlHelper.insert_df(appdata_df, 'check_app_dtchkdata')
    logger.info("collect app data [%d] finished"%len(appdata_df))


    tssql = "select IP, SID, TB_NAME, USED_BYTES, TOTAL_BYTES, USED_PERCENT, WORK_DATE||'060000' as WORK_DATE, AUTOEXTENSIBLE, BLOCK_SIZE, DBFMAXSIZE_BYTES as DBFMAXSIZE_G from TS_INFO"\
            " where work_date||'0600' like '"+ctime+"%'"

    ts_df = oraclehelper_dboper.get_df(tssql)
    mysqlHelper.insert_df(ts_df, 'ts_info')
    #print(ts_df)
    logger.info("collect tablespace [%d] data finished"%len(ts_df))

    if(ctime.endswith("0000")):
        cmdbsql="select ENV_LAST, SYSNODE_IP_OUT_STR as ip, nvl(DEVPRT_CPU_CORE_AMT,SERVER_CPU_AMT) cpu , DEVPRT_MEM_CONTENT as mem, SYSTEM_STR , DB_STR, MID_STR, PART_TYPE, sysdate as checktime from ZJFH_CMDB_APP_NODE"
        cmdb_df = oraclehelper_dboper.get_df(cmdbsql)
        print(cmdb_df)
        mysqlHelper.insert_df(cmdb_df, 'serverinfo')
        logger.info("collect cmdb data finished")


        clean_app_sql="delete from check_app_dtchkdata where checktime < date_sub(CURDATE(),INTERVAL 90 day)"
        mysqlHelper.update(clean_app_sql)
        logger.info("clean app data finished")


        clean_fs_sql="delete from check_dtchkdata where checktime < date_sub(CURDATE(),INTERVAL 90 day)"
        mysqlHelper.update(clean_fs_sql)
        logger.info("clean fs data finished")


    oracleHelper.dispose(1)
    oraclehelper_dboper.dispose(1)
    mysqlHelper.dispose(1)

    end = datetime.datetime.now()
    logger.info("exec times:" + str((end - start).seconds) + "s")
