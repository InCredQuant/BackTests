import sqlite3
import pandas
import datetime
from dateutil.parser import parse
import re
#from matplotlib.cbook import is_numlike
import numpy
import xlrd
import os


def GetConn(requiredstuff: str , gDrive = 'N'):
    '''
    Parameters
    ----------
    requiredstuff : String
        'PriceData', 'AnalystData', 'misdata', 'ResultsData', 'NewsData', 'BhavCopy', 'IntraDay'.
    gDrive : TYPE, optional
        DB connection is Google Drive or No. The default is 'N'.

    Returns
    -------
    conn : Connection to the Required Data Type
        sqlite.Connection.

    '''
    #dbpath = 'G:/Shared drives/BackTests/DB/'
    if gDrive == 'N':
        #dbpath = 'Z:/LiveDB/'
        # dbpath = r'\\192.168.44.8\01.DBLocal\LiveDB\\'
        dbpath = r'\\10.147.0.70\01.DBLocal\LiveDB\\'
    else:
        dbpath = 'G:/Shared drives/BackTests/DB/'
        # dbpath = 'G:/Shared drives/BackTests/DB/'
    MyDict = {'PriceData': dbpath+'PriceData.db',
              'AnalystData' : dbpath+'AnalystData.db',
              'misdata': dbpath+'mis.db',
              'ResultsData': dbpath+'backtesting.db',
              'NewsData': dbpath+'FinalNewsTable.db',              
              'BhavCopy': dbpath+'Bhavcopy.db',
              'IntraDay': dbpath+'Intraday.db',
              }
    conn = sqlite3.connect(MyDict[requiredstuff])
    return conn



def is_numlike(ind):
    try:
        ind = ind+1
        return True
    except:
        return False
    return

def CommaSeparatedList(MyList, appendapost = 1):
    if all([i.isnumeric() for i in MyList]):#
        appendapost = 0
        
    if appendapost == 1:
        temp = ",".join(["'" + i + "'" for i in MyList])
    else:
        temp = ','.join([str(i) for i in MyList])    
    temp = "(" + temp + ")"
    return temp

def ExcelDateToISODate(mynum):
    try:
        mytuple = xlrd.xldate_as_tuple(mynum,0)
        mystr = datetime.datetime(*mytuple).strftime('%Y-%m-%d')
        return mystr
    except:
        return numpy.nan
    

def getdatelist(startdate, enddate):
    DateList = []
    curdate = startdate
    while True:
        DateList.append(curdate)
        curdate = curdate + datetime.timedelta(1)
        if curdate >= enddate:
            break
    return DateList

def GetDataForIndicesFromBloomDB(conn, BloomListOfTickers, fieldname, FromDate =datetime.date(2000, 1, 1), ToDate = datetime.date.today()):
    BloomListOfTickers = [i.lower() for i in BloomListOfTickers]
    basesql = "select Date,Ticker, %s from IndexPriceData where lower(Ticker) in %s and Date between '%s' and '%s';" % (fieldname, CommaSeparatedList(BloomListOfTickers), FromDate.strftime('%Y-%m-%d 00:00:00'), ToDate.strftime('%Y-%m-%d 00:00:00'))
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.index = df.Date
    del df['Date']
    gg = df.groupby('Ticker')
    ResultDF = []
    for key in gg.groups.keys():
        tempdf = gg.get_group(key)
        del tempdf['Ticker']
        tempdf.columns = [key]
        if len(ResultDF) == 0:
            ResultDF = tempdf
        else:
            ResultDF = pandas.concat([ResultDF,tempdf], axis = 1)
    
    ResultDF.index = pandas.to_datetime(ResultDF.index)#.to_datetime()
    ResultDF.columns = [i.upper() for i in ResultDF.columns]
    #ResultDF = ResultDF.fillna(method = 'pad', limit = 3)
    #ResultDF = ResultDF.fillna(method = 'bfill', limit = 3)
    return ResultDF


def getIndicesSpotData(FromDate =datetime.date(2000, 1, 1), ToDate = datetime.date.today()):
    IndDict = {'SENSEX INDEX' : 'SENSEX','NSEBANK INDEX' : 'BANKNIFTY', 'NSEFIN INDEX' : 'FINNIFTY', 'NSEMCAP INDEX' : 'MIDCPNIFTY', 'NIFTY INDEX' : 'NIFTY', 'NSEIT INDEX': 'NIFTYIT', 'NSEPSE INDEX':'NIFTYPSE', 'NIFTYJR INDEX':'JUNIOR', 'NSEINFR INDEX':'NIFTYINFRA'}
    priceConn = GetConn('PriceData', gDrive = 'N')    
    
    SpotPriceDF = GetDataForIndicesFromBloomDB(priceConn, list(IndDict.keys()), 'PX_LAST',FromDate, ToDate)
    SpotPriceDF = SpotPriceDF.sort_index()
    SpotPriceDF.columns = [IndDict[it] for it in SpotPriceDF.columns]
    return SpotPriceDF

def QueryScripMaster(conn, MyList, FromWhat, ToWhat):
    basesqlquery = 'select %s, %s from ScripMaster where %s in %s' %(FromWhat, ToWhat, FromWhat,CommaSeparatedList(MyList))
    curs = conn.cursor()
    curs.execute(basesqlquery)
    qw = curs.fetchall()
    return dict(qw)


def GetNSEBhavCopyFutsData(conn: str, secNames: list, fieldName: str, expiry: str, fromDate: datetime.date, toDate: datetime.date):    
    # secNames = ['SBILIFE', 'STER']
    # fieldName = 'Close'
    # expiry = '31DEC09'
    # fromDate = datetime.date(2009, 11, 27)
    # toDate = datetime.date(2009, 12, 31)    
    #conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    
    fieldName = fieldName.upper()
    tickers = [(i+expiry.upper()+'XX0').lower() for i in secNames]
    basesql = "select SYMBOL as Name, TIMESTAMP as Date, %s from NSEFNO where lower(Ticker) in %s and TIMESTAMP between '%s' and '%s';" % (fieldName, CommaSeparatedList(tickers), fromDate.strftime('%Y-%m-%d'), toDate.strftime('%Y-%m-%d'))
    curs = conn.cursor()
    curs.execute(basesql)
    
    df = pandas.DataFrame(curs.fetchall(), columns = [rec[0] for rec in curs.description])#    
    df.Date = pandas.to_datetime(df.Date)    
    df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
    gg = df.groupby('Name')
    
    temp_dfs = [pandas.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper()}) for grp in gg.groups.keys()]
    finalDF = pandas.concat(temp_dfs, axis=1)
    finalDF = finalDF.sort_index()
    return finalDF


def GetNSEBhavCopyOptsData(conn: str, secName: str, fieldName: str, strikes: list, expiry: str, call_put: list, fromDate: datetime.date, toDate: datetime.date):
    # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    # secNames = 'CNXIT'
    # fieldName = 'Close'
    # strikes = [20800, 20900, 21000]
    # expiry = '26FEB04'
    # call_put = ['CE', 'PE']
    # fromDate = datetime.date(2004, 1, 1)
    # toDate = datetime.date(2004, 2, 26)    
    
    fieldName = fieldName.upper()
    tickers = [(secName.upper() + expiry + cType + str(iR)).lower() for iR in strikes for cType in call_put]
    basesql = "select Ticker, TIMESTAMP as Date, %s from NSEFNO where lower(Ticker) in %s and TIMESTAMP between '%s' and '%s';" % (fieldName, CommaSeparatedList(tickers), fromDate.strftime('%Y-%m-%d'), toDate.strftime('%Y-%m-%d'))
    curs = conn.cursor()
    curs.execute(basesql)
    
    df = pandas.DataFrame(curs.fetchall(), columns = [rec[0] for rec in curs.description])#    
    df.Date = pandas.to_datetime(df.Date)    
    df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
    gg = df.groupby('Ticker')
    
    temp_dfs = [pandas.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper()}) for grp in gg.groups.keys()]
    finalDF = pandas.concat(temp_dfs, axis=1)
    finalDF = finalDF.sort_index()
    return finalDF

def getStrikeString(iNum):
    iNum = float(iNum)
    if iNum == int(iNum):
        return str(int(iNum))
    # mat = re.match(r'\d+\.?[0]+[1-9]+', str(iNum))
    mat = re.match(r'\d+\.?[1-9]*[0]*[1-9]+', str(iNum))
    return mat.group()    
