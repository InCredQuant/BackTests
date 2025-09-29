import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
from sqlalchemy import create_engine
import sys
sys.path.insert(0,r'G:\Shared drives\BackTests\pycode\MainLibs')
from intraday_db_postgres import *
sys.path.insert(0,r'G:\Shared drives\BackTests\pycode\DBUpdation')
import pg_redirect

def connDetails(databaseName,userName='postgres',password='postgres',hostname='192.168.44.4',port=5432):
    print(databaseName,userName,password,hostname,port)
    connection = pg.connect(database=databaseName, user=userName, password=password, host=hostname, port=port)
    return connection

def getconstituensData():
    database = 'pricedata'
    tablename = 'Components'
    connection = connDetails(database)
    query = f'''SELECT * FROM "{tablename}" where "IndexName" = 'NSEBANK INDEX' ''';
    indexData = pd.read_sql(query,connection)
    return indexData

def getScripMaster():
    database = 'pricedata'
    tablename = 'ScripMaster'
    connection = connDetails(database)
    query = f'''SELECT "NSE","Bloomberg" FROM "{tablename}" ''';
    scripMasterdf = pd.read_sql(query,connection)
    scripMasterdf = scripMasterdf.dropna(subset=['Bloomberg','NSE'],how='any').reset_index(drop=True)
    return scripMasterdf

if __name__ == '__main__':
    indexDf = getconstituensData()
    scripMasterDf = getScripMaster()
    print(scripMasterDf,indexDf)

