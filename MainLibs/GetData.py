# -*- coding: utf-8 -*-
"""
Created on Thu Jan 22 13:02:29 2015

@author: Viren@Incred
"""

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
        dbpath = 'Z:/LiveDB/'
    else:
        dbpath = 'G:/Shared drives/BackTests/DB/'
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

def QueryScripMaster(conn, MyList, FromWhat = 'Ticker', ToWhat = 'FutTicker'):
    MyList = [it.replace(' IN', '') for it in MyList]
    basesqlquery = 'select %s, %s from ScripM where %s in %s' %(FromWhat, ToWhat, FromWhat,CommaSeparatedList(MyList))
    curs = conn.cursor()
    curs.execute(basesqlquery)
    qw = curs.fetchall()
    return dict(qw)



def GetComponentsForIndex(conn, WhichDate, IndexName):
    basesql = "select Components from Components where IndexName = '%s' and Date between '%s' and '%s' limit 1" % (IndexName, (WhichDate + datetime.timedelta(-30)).strftime('%Y-%m-%d 00:00:00'), WhichDate.strftime('%Y-%m-%d 00:00:00'))
    curs = conn.cursor()
    curs.execute(basesql)
    commaseplist = curs.fetchone()
    commaseplist = commaseplist[0]
    FinalList = commaseplist.split(', ')
    FinalList = [re.sub(' I[BS]', ' IN', i) for i in FinalList]
    return FinalList


def GetComponentsForIndexForDateRange(conn, FromDate, ToDate, IndexName):
    basesql = "select Date, Components from Components where IndexName = '%s' and Date between '%s' and '%s'" % (IndexName, FromDate.strftime('%Y-%m-%d 00:00:00'), ToDate.strftime('%Y-%m-%d 00:00:00'))
    curs = conn.cursor()
    curs.execute(basesql)
    alldaysdata = curs.fetchall()
    indexmembdict = {}
    for item in alldaysdata:
        tempdate = item[0]
        commaseplist = item[1]
        templistofnames = commaseplist.split(', ')
        templistofnames = [re.sub(' I[BS]', ' IN', i) for i in templistofnames]
        indexmembdict[parse(tempdate)] = templistofnames
    return indexmembdict

def GetComponentsForAll(conn = None, FromDate =datetime.date(2000, 1, 1), ToDate = datetime.date.today()):
    if conn is None:
        try:
            conn = GetConn('PriceData')
        except:
            conn = GetConn('PriceData', gDrive = 'Y')
    basesql = "select Date, Components from Components where Date between '%s' and '%s'" % (FromDate.strftime('%Y-%m-%d 00:00:00'), ToDate.strftime('%Y-%m-%d 00:00:00'))
    curs = conn.cursor()
    curs.execute(basesql)
    alldaysdata = curs.fetchall()
    membList = []
    for item in alldaysdata:
        tempdate = item[0]
        commaseplist = item[1]
        templistofnames = commaseplist.split(', ')
        templistofnames = [re.sub(' I[BS]', ' IN', i) for i in templistofnames]
        membList.extend(templistofnames)
    return membList


def QueryFutTickers(conn, MyList, FromWhat = 'Ticker', ToWhat = 'GenericTicker'):
    MyList = [re.sub(' I[BN]', ' IS', i) for i in MyList]
    basesqlquery = 'select %s, %s from FutLookUpTable where %s in %s' %(FromWhat, ToWhat, FromWhat,CommaSeparatedList(MyList))
    curs = conn.cursor()
    curs.execute(basesqlquery)
    qw = curs.fetchall()
    qw = [(i[1], re.sub(' I[SB]', ' IN', i[0])) for i in qw]
    return dict(qw)

def QueryExpiryDates(conn, expirytype = 'Monthly'):
    basesqlquery = 'select Date from ExpiryDate where %s = 1;' %(expirytype)
    curs = conn.cursor()
    curs.execute(basesqlquery)
    qw = curs.fetchall()
    qw = [i[0] for i in qw]
    qw.sort()
    return qw

def GetQuarterlyData(conn, BloomListOfTickers, fieldname):
    alltickersincapitaline = QueryScripMaster(conn, BloomListOfTickers, 'Bloomberg', 'CompanyCode')
    allcompaniesinstrformat = CommaSeparatedList(alltickersincapitaline.values())
    basesql = 'select CompanyCode as Ticker, YearMonth AS Date, %s from QuarterlyPnLOverall where CompanyCode in %s' % (fieldname, allcompaniesinstrformat)
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.Date = pandas.to_datetime(df.Date.apply(GetRealDateFromYearMonth))
    
    f = lambda x: alltickersincapitaline.keys()[alltickersincapitaline.values().index(x)]
    df.Ticker = df.Ticker.apply(f)
    
    df.index = df.Date
    del df['Date']
    
    gg = df.groupby('Ticker')
    ResultDF = []
    for key in gg.groups.keys():
        tempdf = gg.get_group(key)
        del tempdf['Ticker']
        tempdf.columns = [key]
        tempdf["index"] = tempdf.index
        tempdf.drop_duplicates(cols='index', take_last=True, inplace=True)
        del tempdf["index"]
        
        if len(ResultDF) == 0:
            ResultDF = tempdf
        else:
            ResultDF = pandas.concat([ResultDF,tempdf], axis = 1)
    
    ResultDF.index = pandas.to_datetime(ResultDF.index)#.to_datetime()
#    ResultDF = ResultDF.fillna(method = 'pad', limit = 3)
#    ResultDF = ResultDF.fillna(method = 'bfill', limit = 3)
    
    return ResultDF

def GetFundamentalData(conn, BloomListOfTickers, fieldlist, tablelist):
    alltickersincapitaline = QueryScripMaster(conn, BloomListOfTickers, 'Bloomberg', 'CompanyCode')
    allcompaniesinstrformat = CommaSeparatedList(alltickersincapitaline.values())
    
    if type(tablelist) is str:
        tablelist = [tablelist]
    
    if len(tablelist) == 1:
        tablelist = tablelist*len(fieldlist)
    
    basesql = 'select CompanyCode as Ticker, YearMonth AS Date, %s from %s where CompanyCode in %s'
    curs = conn.cursor()
    myresdata = {}
    
    for count in range(len(fieldlist)):
        thisfieldname = fieldlist[count]
        thistablename = tablelist[count]
        
        thissql = basesql % (thisfieldname, thistablename, allcompaniesinstrformat)
        curs.execute(thissql)
        df = pandas.DataFrame(curs.fetchall())
        df.columns = [rec[0] for rec in curs.description]
        df.Date = pandas.to_datetime(df.Date.apply(GetRealDateFromYearMonth))
        
        f = lambda x: list(alltickersincapitaline.keys())[list(alltickersincapitaline.values()).index(x)]
        df.Ticker = df.Ticker.apply(f)
        
        df.index = df.Date
        del df['Date']
        
        gg = df.groupby('Ticker')
        ResultDF = []
        for key in gg.groups.keys():
            tempdf = gg.get_group(key)
            del tempdf['Ticker']
            tempdf.columns = [key]
            tempdf["index"] = tempdf.index
            tempdf.drop_duplicates(subset='index', keep='last', inplace=True)
            del tempdf["index"]
            
            if len(ResultDF) == 0:
                ResultDF = tempdf
            else:
                ResultDF = pandas.concat([ResultDF,tempdf], axis = 1)
        
        ResultDF.index = pandas.to_datetime(ResultDF.index)#.to_datetime()
        myresdata[thisfieldname] = ResultDF
#    ResultDF = ResultDF.fillna(method = 'pad')
#    ResultDF = ResultDF.fillna(method = 'bfill')
    
    return myresdata


def GetGICS(conn, BloomListOfTickers, sectorName = 'GICS_SECTOR_NAME'):
    basesqlquery = 'select Ticker, %s from GICS where Ticker in %s;' %( sectorName, CommaSeparatedList(BloomListOfTickers))
    curs = conn.cursor()
    curs.execute(basesqlquery)
    qw = curs.fetchall()
    df = pandas.DataFrame(qw, columns = ['Ticker', sectorName])
    gg = df.groupby(sectorName)
    revqw = {}
    for grp in gg.groups.keys():
        revqw[grp] = list(gg.get_group(grp)['Ticker'])
    return dict(qw), revqw

def GetRealDateFromYearMonth(yearmonth):
    try:
        resdate = (datetime.date(int(yearmonth/100),int(yearmonth%100),1) + datetime.timedelta(35)).replace(day = 1) - datetime.timedelta(1)
    except:
        resdate = numpy.nan
    return resdate

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


def GetNiftyOHLC(conn):
    basesql = "select * from StockPriceData where lower(Ticker) = 'nifty index'"
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.index = df.Date
    del df['Date']
    del df['Ticker']
    ResultDF = df
        
    ResultDF.index = pandas.to_datetime(ResultDF.index)#.to_datetime()
    #ResultDF = ResultDF.fillna(method = 'pad', limit = 3)
    #ResultDF = ResultDF.fillna(method = 'bfill', limit = 3)
    return ResultDF

def GetDataForTickersFromBloomDB(conn, BloomListOfTickers, fieldname, fromDate):
    BloomListOfTickers = [re.sub(' I[BS]', ' IN', i) for i in BloomListOfTickers]
    BloomListOfTickers = [i + ' Equity' for i in BloomListOfTickers]
    BloomListOfTickers = [str(i) for i in BloomListOfTickers]
    BloomListOfTickers = [i.lower() for i in BloomListOfTickers]
    
    basesql = "select Date,Ticker,%s from StockPriceData where lower(Ticker) in %s and Date>= '%s';" % (fieldname, CommaSeparatedList(BloomListOfTickers), fromDate)
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.index = df.Date
    del df['Date']
    gg = df.groupby('Ticker')
    ResultDF = pandas.DataFrame()
    for key in gg.groups.keys():
        tempdf = gg.get_group(key)
        del tempdf['Ticker']
        tempdf.columns = [key]
        tempdf.index = pandas.to_datetime(tempdf.index)
        tempdf = tempdf.sort_index()
        ResultDF = pandas.concat([ResultDF,tempdf], axis = 1)
    
    ResultDF.index = pandas.to_datetime(ResultDF.index)#ResultDF.index.to_datetime()
    #ResultDF = ResultDF.fillna(method = 'pad', limit = 3)
    ResultDF.columns = [i.lower().replace(' equity', '').upper() for i in ResultDF.columns]
    return ResultDF

def GetDataForFutTickersFromBloomDB(conn, BloomListOfTickers, fieldname, fromDate):
    indexList = list(set([ind if 'index' in ind.lower() else '' for ind in BloomListOfTickers]))
    stocksList = list(set([ind if 'index' not in ind.lower() else '' for ind in BloomListOfTickers]))
    if '' in indexList:
        indexList.remove('')
    if '' in stocksList:
        stocksList.remove('')
    stocksList = [re.sub(' I[BN]', ' IS', i) for i in stocksList]
    stocksList = [i + ' Equity' for i in stocksList]
    indexList.extend(['NZ1 INDEX', 'AF1 INDEX'])
    stocksList.extend(indexList)
    BloomListOfTickers = list(set(stocksList))
    # BloomListOfTickers = [re.sub(' I[BN]', ' IS', i) for i in BloomListOfTickers]
    # BloomListOfTickers = [i + ' Equity' for i in BloomListOfTickers]
    # BloomListOfTickers.extend(['NZ1 INDEX'])
    BloomListOfTickers = [str(i) for i in BloomListOfTickers]    
    BloomListOfTickers = [i.lower() for i in BloomListOfTickers]    
    basesql = "select Date,Ticker,%s from FutPriceData where lower(Ticker) in %s and Date>= '%s';" % (fieldname, CommaSeparatedList(BloomListOfTickers), fromDate)
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.index = df.Date
    del df['Date']
    gg = df.groupby('Ticker')
    ResultDF = pandas.DataFrame()
    for key in gg.groups.keys():
        tempdf = gg.get_group(key)
        del tempdf['Ticker']
        tempdf.columns = [key]
        tempdf.index = pandas.to_datetime(tempdf.index)
        tempdf = tempdf.sort_index()
        ResultDF = pandas.concat([ResultDF,tempdf], axis = 1)
    
    ResultDF.index = pandas.to_datetime(ResultDF.index)#ResultDF.index.to_datetime()
    #ResultDF = ResultDF.fillna(method = 'pad', limit = 3)
    ResultDF.columns = [i.lower().replace(' equity', '').upper() for i in ResultDF.columns]
    ResultDF = ResultDF.loc[:, ~ResultDF.columns.duplicated()]
    return ResultDF

def GetDataForAnalystEstimate(conn, BloomListOfTickers, fieldname):
    BloomListOfTickers = [re.sub(' I[BS]', ' IN', i) for i in BloomListOfTickers]
    BloomListOfTickers = [i + ' Equity' for i in BloomListOfTickers]
    BloomListOfTickers = [str(i) for i in BloomListOfTickers]
    BloomListOfTickers = [i.lower() for i in BloomListOfTickers]    
    basesql = 'select Date, upper(Ticker) as Ticker, %s from AnalystRatingData where lower(Ticker) in %s and TargetPrice > 0;' % (fieldname, CommaSeparatedList(BloomListOfTickers))
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.index = df.Date
    del df['Date']
    df = df.dropna()
    gg = df.groupby('Ticker')    
    ResultDF = []
    if fieldname in ['TargetPrice', 'Rating', 'AnalystRating']:
        for key in gg.groups.keys():
            tempdf = gg.get_group(key)
            del tempdf['Ticker']
            tempdf = tempdf.groupby(tempdf.index).last()
            tempdf.columns = [key]
            if len(ResultDF) == 0:
                ResultDF = tempdf
            else:
                ResultDF = pandas.concat([ResultDF,tempdf], axis = 1)    
    ResultDF.index = pandas.to_datetime(ResultDF.index)#.to_datetime()
    ResultDF.columns = [i.lower().replace(' equity', '').upper() for i in ResultDF.columns]
    return ResultDF
    
def GetFullDataForAnalystEstimate(conn, BloomListOfTickers):
    BloomListOfTickers = [re.sub(' I[BS]', ' IN', i) for i in BloomListOfTickers]
    BloomListOfTickers = [i + ' Equity' for i in BloomListOfTickers]
    BloomListOfTickers = [str(i) for i in BloomListOfTickers]
    BloomListOfTickers = [i.lower() for i in BloomListOfTickers]
    
    basesql = 'select Date,Ticker, TargetPrice, Rating, AnalystID from InformationData where lower(Ticker) in %s and TargetPrice > 0;' % ( CommaSeparatedList(BloomListOfTickers))
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.index = df.Date
    del df['Date']
    df.Ticker = [i.lower().replace(' equity', '').upper() for i in df.Ticker]
    df.index = pandas.to_datetime(df.index)#.to_datetime()
    return df

def GetDataForBESTFromBloomDB(conn, BloomListOfTickers, fieldname, freq = 'm'):
    #list of tickers with IN Equity, should we do upper of tickers?
    BloomListOfTickers = [re.sub(' I[BS]', ' IN', i) for i in BloomListOfTickers]
    BloomListOfTickers = [i + ' Equity' for i in BloomListOfTickers]
    BloomListOfTickers = [str(i) for i in BloomListOfTickers]
    BloomListOfTickers = [i.lower() for i in BloomListOfTickers]    
    basesql = 'select Date,Ticker,%s from EPSData where lower(Ticker) in %s' % (fieldname, CommaSeparatedList(BloomListOfTickers))
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    #df.index = df.Date
    #del df['Date']
    gg = df.groupby('Ticker')
    ResultDF = []
    for key in gg.groups.keys():
        tempdf = gg.get_group(key)
        del tempdf['Ticker']
        tempdf.index = tempdf.Date
        del tempdf['Date']
        tempdf.columns = [key]
        tempdf.index = pandas.to_datetime(tempdf.index)#.to_datetime()
        tempdf = tempdf.resample(freq, convention = 'end').last()
        tempdf = tempdf.ffill()
        if len(ResultDF) == 0:
            ResultDF = tempdf
        else:
            ResultDF = ResultDF.join(tempdf, how = 'outer')#            
    ResultDF.index = pandas.to_datetime(ResultDF.index)#.to_datetime()
    ResultDF = ResultDF.fillna(method = 'pad', limit = 3)
    ResultDF.columns = [i.lower().replace(' equity', '').upper() for i in ResultDF.columns]
    return ResultDF
    
def ForeignShareHoldingPCT(conn, BloomListOfTickers):
    alltickersincapitaline = QueryScripMaster(conn, BloomListOfTickers, 'Bloomberg', 'CompanyCode')
    allcompaniesinstrformat = CommaSeparatedList(alltickersincapitaline.values())
    basesql = 'select CompanyCode as Ticker, YearMonth as Date, ForeignPromoterGroup, ForeignBankNPI, ForeignBodiesCorporateNPI, ForeignCollaboratorsNPI, FIINPI, FVCINPI, ForeignIndividualsForeignNationalsNPNI, GrandTotal from ShareHoldingFinal where CompanyCode in %s group by Ticker order by Date desc;' % (allcompaniesinstrformat)
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]
    df.Date = pandas.to_datetime(df.Date.apply(GetRealDateFromYearMonth))
    
    f = lambda x: alltickersincapitaline.keys()[alltickersincapitaline.values().index(x)]
    df.Ticker = df.Ticker.apply(f)
    
    del df['Date']
    df.index = df.Ticker
    del df['Ticker']
    total = df['GrandTotal']
    del df['GrandTotal']
    res = df.sum(axis = 1)/total
    res.columns = ['ForeignShareholding']
    return res

'''Get data from Historical Portfolios'''
def PortfoliosData(conn, portfolio = 'Very Aggressive'):
    basesql = 'select * from HistoricalPortfolios where Strategy = "%s" ;' % (portfolio)
    curs = conn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    df.columns = [rec[0] for rec in curs.description]    
    gg = df.groupby('Date')
    ResultDF = []
    tmpkeys = gg.groups.keys()
    tmpkeys.sort()
    for key in tmpkeys:
        tempdf = gg.get_group(key)
        del tempdf['Strategy']
        del tempdf['Date']
        tempdf.index = tempdf['Scheme']
        del tempdf['Scheme']
        tempdf.columns = [key]
        if len(ResultDF) == 0:
            ResultDF = tempdf
        else:
            ResultDF = pandas.concat([ResultDF,tempdf], axis = 1)
    return ResultDF



#--------------------------------------------------------------

def GetIntraDayFutsData(conn: str, secNames: list, fieldName: str, fromDate: datetime.date, toDate: datetime.date):
    '''
    Parameters
    ----------
    conn : str
        Connection to DB.
    secNames : list
        List of the Futures Securities.
    fieldName : str
        Field Name. 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest' .
    fromDate : datetime.date
        Starting Date in Date Format from datetime.date module.
    toDate : datetime.date
        End Date in Date Format from datetime.date module.

    Returns
    -------
    finalDF : Pandas DataFrame
        Returns the data frame for the povided Futures Securities List,  fieldName data.

    '''
    # conn = GetConn('IntraDay')
    # secNames = ['nifty-i', 'banknifty-i']
    # fieldName = 'Close'
    # fromDate = datetime.date(2022, 8, 12)
    # toDate = datetime.datetime.today()
    
    secNames = [i.lower() for i in secNames]
    basesql = "select Name, Date, Time, %s from Futs where lower(Name) in %s and Date between '%s' and '%s';" % (fieldName, CommaSeparatedList(secNames), fromDate.strftime('%Y-%m-%d'), toDate.strftime('%Y-%m-%d'))
    curs = conn.cursor()
    curs.execute(basesql)
    
    qw = curs.fetchall()
    df = pandas.DataFrame(qw)#
    df.columns = [rec[0] for rec in curs.description]
    df.Date = pandas.to_datetime(df.Date)    
    df.index = [datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
    gg = df.groupby('Name')
    
    temp_dfs = [pandas.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper().replace('-I', '').replace('-II', '').replace('-III', '')}) for grp in gg.groups.keys()]
    finalDF = pandas.concat(temp_dfs, axis=1)
    finalDF = finalDF.sort_index()
    return finalDF

def GetIntraDayOptsData(conn: str, secName: str, fieldName: str, strikes: list, expiry: str , call_put: list, fromDate: datetime.date, toDate: datetime.date):
    '''
    Parameters
    ----------
    conn : str
        Connection to DB.
    secName : str
        Options Underlying Security Name. 'nifty', 'banknifty', 'tatamotors' .
    fieldName : str
        Field Name. 'Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest' .
    strikes : list
        Strike Price of the Option.
    expiry : str
        Expiry Date in string Format. '01SEP22'
    call_put : list
        ['CE', 'PE'].
    fromDate : datetime.date
        Starting Date in Date Format from datetime.date module.
    toDate : datetime.date
        End Date in Date Format from datetime.date module.

    Returns
    -------
    finalDF : Pandas DataFrame
        Returns the data frame for the povided option's fieldName data.

    '''
    # conn = GetConn('IntraDay')
    # secNames = 'nifty'#, 'banknifty']
    
        
    # fieldName = 'Close'
    # fromDate = datetime.date(2022, 8, 12)
    # toDate = datetime.datetime.today()
    # strikes = ['17500', '17600', '17700', '17800']
    # expiry = '01SEP22'
    # call_put = ['CE', 'PE']
        
    tableName = secName.upper() if secName.upper() in ['NIFTY', 'BANKNIFTY'] else 'StockOptions'        
    tickers = [secName.upper() + expiry + str(iR) + cType + '.NFO' for iR in strikes for cType in call_put]
    #basesql = f"SELECT Name, Date, Time, {fieldName}, ExpiryDate, StrikePrice, Call_Or_Put FROM {tableName} WHERE Ticker IN ({','.join(tickers)}) AND Date BETWEEN '{fromDate.strftime('%Y-%m-%d')}' AND '{toDate.strftime('%Y-%m-%d')}';"          
    basesql = "select Name, Date, Time, %s, ExpiryDate, StrikePrice,  Call_Or_Put from %s where Ticker in %s and Date between '%s' and '%s';" % (fieldName, tableName, CommaSeparatedList(tickers), fromDate.strftime('%Y-%m-%d'), toDate.strftime('%Y-%m-%d'))
    curs = conn.cursor()
    curs.execute(basesql)    
    df = pandas.DataFrame(curs.fetchall(), columns=[rec[0] for rec in curs.description])
    
    #df.Date = pandas.to_datetime(df.Date)
    #df['DateTime'] = [datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
    df['DateTime'] = pandas.to_datetime(df['Date']) + pandas.to_timedelta(df['Time'].str.pad(width=8, side='left', fillchar='0'))
    df['ID'] = df.Name + df.StrikePrice + df.Call_Or_Put + df.ExpiryDate#[''.join(it) for it in df.loc[:, ['Name', 'StrikePrice', 'Call_Or_Put', 'ExpiryDate']].values]
    df.index = df.DateTime
    gg = df.groupby('ID')
    
    temp_dfs = [pandas.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp}) for grp in gg.groups.keys()]
    finalDF = pandas.concat(temp_dfs, axis=1)
    finalDF = finalDF.sort_index()
    return finalDF

def BuildOptionsData(ticker = 'NIFTY', pointsDiff = 100, otmPoints = 1, reSampleTime = 5, startDate = datetime.date(2019, 3, 21), endDate = datetime.date(2022, 12, 31), indexFields = ['Open', 'High', 'Low', 'Close'], SecFields = ['High', 'Low', 'Close']):
    # ticker = 'NIFTY'    
    # pointsDiff = 100
    # otmPoints = 1
    # startDate = datetime.date(2019, 3, 21)
    # endDate = datetime.date(2022, 12, 31)
    # reSampleTime = 5 # Mins (it will define at how much time interval we have ot check for the StopLosses or Other Signals)    
    #FIELDS = ['Open', 'High', 'Low', 'Close']
    import time
    import os
    import math
    
    t1 = time.time()
    ticker1 = ticker+'-I'
    fieldDict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum', 'OpenInterest': 'sum'}    
    
    conn = GetConn('IntraDay', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    priceconn = GetConn('PriceData', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    #timeAdj = datetime.timedelta(hours = 6, minutes = 16)
    
    mydata = MyBacktestData()
    mydata.Index = Index()
    mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Weekly')
    mydata.ExpiryTimes = [datetime.datetime.strptime(it, '%Y-%m-%d') +datetime.timedelta(hours = 15, minutes=31) for it in mydata.ExpiryDates]     
    mydata.ExpiryTimes = [it for it in mydata.ExpiryTimes if it.date() >= startDate and it.date() <= endDate]
    
    #FIELDS = ['Open', 'High', 'Low', 'Close']
    for field in indexFields:
        setattr(mydata.Index, field, GetIntraDayFutsData(conn, secNames = [ticker1], fieldName = field, fromDate = startDate, toDate =  endDate))
    #mydata.Index.Close = GetIntraDayFutsData(conn, secNames = [ticker1], fieldName = 'Close', fromDate = startDate, toDate =  endDate)
    
    #FIELDS = ['Close']
    for ind, item in enumerate(mydata.ExpiryTimes[:-1]):
        startTime = item -datetime.timedelta(days = 1)
        endTime = mydata.ExpiryTimes[ind+1]
        indexPrice = mydata.Index.Close.loc[startTime: endTime]
        strikesRange = range(pointsDiff*(math.floor(indexPrice.min()/pointsDiff) - otmPoints), pointsDiff*(math.ceil(indexPrice.max()/pointsDiff) + otmPoints+1), pointsDiff)
        for field in SecFields:
            try:
                tempData = GetIntraDayOptsData(conn, secName = ticker, fieldName = field, strikes = strikesRange, expiry = endTime.strftime('%d%b%y').upper()  , call_put = ['CE', 'PE'], fromDate = startTime.date(), toDate = endTime.date())
                
                df = tempData.resample(str(reSampleTime)+'T').apply(fieldDict[field])
                df = df.loc[~( (df.index.hour == 15) & (df.index.minute > 31)| (df.index.hour > 15) | (df.index.hour == 9) & (df.index.minute < 15)|(df.index.hour < 9)) & (df.count(axis = 1) > 0)]        
                #df = df[df.count(axis = 1) > 0]
                
                if hasattr(mydata, field):
                    setattr(mydata, field, pandas.concat([getattr(mydata, field), df], axis = 1))
                else:
                    setattr(mydata, field, df)
            except:
                import pdb
                pdb.set_trace()
    
    for field in indexFields:
        tempDF = getattr(mydata.Index, field)
        df = tempDF.resample(str(reSampleTime)+'T').apply(fieldDict[field])
        df = df.loc[~( (df.index.hour == 15) & (df.index.minute > 31)| (df.index.hour > 15) | (df.index.hour == 9) & (df.index.minute < 15)|(df.index.hour < 9)) & (df.count(axis = 1) > 0)]        
        #df = df[df.count(axis = 1) > 0]
        setattr(mydata.Index, field, df)    
    mydata.indexprice = mydata.Index.Close
    t2 = time.time()
    print('Data Updation Time:', round((t2-t1)/60, 1), 'Mins', sep = ' ')
    return mydata


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

def GetNSEBhavCopyFutsDatabyTicker(conn: str, tickers : list, fieldName: str):    
    # secNames = ['SBILIFE', 'STER']
    # tickers = ['CNXIT25MAR04XX0', 'NIFTY29JAN04XX0']
    # fieldName = 'Close'
    # expiry = '31DEC09'
    # fromDate = datetime.date(2009, 11, 27)
    # toDate = datetime.date(2009, 12, 31)    
    # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    
    fieldName = fieldName.upper()
    tickers = [i.lower() for i in tickers]
    basesql = "select SYMBOL as Name, TIMESTAMP as Date, %s from NSEFNO where lower(Ticker) in %s;" % (fieldName, CommaSeparatedList(tickers))
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

def GetNSEBhavCopyStrikePointsDiff(conn: str, secNames: list, expiry: str, getStrikes = False):
    #conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    #secNames = ['CNXIT', 'NTPC', 'ONGC', 'PNB', 'POWERGRID', 'RANBAXY', 'ICICIBANK']
    #expiry = '31AUG17'
    
    basesql = "select SYMBOL as Name, STRIKE_PR as Strike from NSEFNO where lower(SYMBOL) in %s and EXPIRY_DT = '%s' and INSTRUMENT in ('OPTIDX', 'OPTSTK');" % (CommaSeparatedList(secNames).lower(), expiry)
    # print(basesql)
    curs = conn.cursor()
    curs.execute(basesql)
    
    df = pandas.DataFrame(curs.fetchall(), columns = [rec[0] for rec in curs.description])
    #df.Strike = df.Strike#.astype('float')
    df.index = df.Name
    del df['Name']    
    gg = df.groupby('Name')
    
    
    dtemp = []
    ok = [dtemp.append((grp, gg.get_group(grp).rename(columns = {'Strike' : grp.upper()}).diff().median().values[0])) for grp in gg.groups.keys()]    
    finalDict = dict(dtemp)
    return gg if getStrikes else finalDict
    
def GetNSEBhavCopyDatabyTicker(conn: str, tickers : list, fieldName: str):
    # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    # tickers = ['ITC29JAN04CA1050', 'IPCL25MAR04PA210', 'IOC26FEB04PA410', 'INFOSYSTCH26FEB04PA5100', 'ICICIBANK26FEB04PA300', 'I-FLEX26FEB04CA800']
    fieldName = 'Close'
    # tickers = ['BANKNIFTY25APR19XX0']
    
    basesql = "select Ticker, TIMESTAMP as Date, %s from NSEFNO where lower(Ticker) in %s;" % (fieldName, CommaSeparatedList(tickers).lower())
    curs = conn.cursor()
    curs.execute(basesql)
    
    df = pandas.DataFrame(curs.fetchall(), columns = [rec[0] for rec in curs.description]) 
    df.Date = pandas.to_datetime(df.Date)    
    df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
    del df['Date']
    gg = df.groupby('Ticker')
    
    temp_dfs = [pandas.DataFrame(gg.get_group(grp)[fieldName.upper()]).rename(columns={fieldName.upper(): grp.upper()}) for grp in gg.groups.keys()]
    finalDF = pandas.concat(temp_dfs, axis=1)
    finalDF = finalDF.sort_index()
    return finalDF

def GetNSEBhavCopyAllTickersDailyData(conn: str , symbols: list, fieldName: str, expiry: str,  fromDate: datetime.date, toDate: datetime.date, options = True ):
    '''
    Parameters
    ----------
    conn : str
        Connection to DB, Either Local Drive or GDrive.
    symbols : list
        List of the NSE Symbols.
    fieldName : str
        Name of the Required Data Field.
    expiry : str
        Expiry Date in DDMMMYY format.
    fromDate : datetime.date
        Starting Date.
    toDate : datetime.date
        Ending Date.
    options: Binary
        If True then for Options, False then for Futures for the provided list.
    
    Returns
    -------
    It Returns the Required Field Data between two dates, for the provided tickers list and Expiry Date.

    '''
    # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
    # symbols = ['VEDL', 'AXISBANK', 'HCLTECH', 'HINDUNILVR', 'SUNPHARMA', 'WIPRO',
    #        'NTPC', 'TATAMOTORS', 'BHARTIARTL', 'HDFC', 'LT', 'ICICIBANK', 'INFY',
    #        'SBIN', 'HDFCBANK', 'COALINDIA', 'ITC', 'ONGC', 'RELIANCE', 'TCS']
    # fieldName = 'Close'
    # expiry = '26FEB04'
    # fromDate = datetime.date(2004, 1, 1)
    # toDate = datetime.date(2004, 2, 26)
    # options = True
    if options:
        instrument = ['OPTIDX', 'OPTSTK']
    elif not options:
        instrument = ['FUTIDX', 'FUTSTK']

        
    basesql = "select Ticker, TIMESTAMP as Date, %s from NSEFNO where lower(SYMBOL) in %s and EXPIRY_DT = '%s' and TIMESTAMP between '%s' and '%s' and lower(INSTRUMENT) in %s;" % (fieldName, CommaSeparatedList(symbols).lower(), expiry, fromDate.strftime('%Y-%m-%d'), toDate.strftime('%Y-%m-%d'), CommaSeparatedList(instrument).lower())
    curs = conn.cursor()
    curs.execute(basesql)
    
    df = pandas.DataFrame(curs.fetchall(), columns = [rec[0] for rec in curs.description]) 
    df.Date = pandas.to_datetime(df.Date)    
    df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
    del df['Date']
    gg = df.groupby('Ticker')
    
    temp_dfs = [pandas.DataFrame(gg.get_group(grp)[fieldName.upper()]).rename(columns={fieldName.upper(): grp.upper()}) for grp in gg.groups.keys()]
    finalDF = pandas.concat(temp_dfs, axis=1)
    finalDF = finalDF.sort_index()
    return finalDF

def getStrikeString(iNum):
    iNum = float(iNum)
    if iNum == int(iNum):
        return str(int(iNum))
    #mat = re.match(r'(\d+\.?[1-9]+)', str(iNum))
    mat = re.match(r'\d+\.?[1-9]*[0]*[1-9]+', str(iNum))
    return mat.group()    
    
class MyBacktestData():
    pass

class Index():
    pass

# def getStrikeString(iNum):
#     iNum = float(iNum)
#     if iNum == int(iNum):
#         return str(int(iNum))
#     # mat = re.match(r'\d+\.?[0]+[1-9]+', str(iNum))
#     mat = re.match(r'\d+\.?[1-9]*[0]*[1-9]+', str(iNum))
#     return mat.group()  

#conn = sqlite3.connect('Y:/Viren/backups/backtesting.db')
#curs = conn.cursor()
#curs.execute('select * from BSEDaily limit 100')
