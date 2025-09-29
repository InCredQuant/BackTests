# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 12:06:47 2023

@author: Viren@InCred
Usage- Data Builder for Disperison backTest/ Live Model

"""

import datetime
#import sqlite3
#import pandas
#from GetData import GetConn, QueryFutTickers
from GetData import *
import MyTechnicalLib
import time
#from FactorsDef import *
import pickle
import math

import warnings
warnings.filterwarnings("ignore")

INDEXNAME = 'BANKNIFTY'
delta = 1

t1 = time.time()
priceconn = GetConn('PriceData', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
#analystconn = GetConn('AnalystData')
bhavconn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')

DataStartDate = datetime.date(2014, 4, 24)
DataEndDate = datetime.date(2022, 12, 29)# or latest date


mydata = MyBacktestData()
mydata.Index = Index()

mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')
mydata.ExpiryTimes = [datetime.datetime.strptime(it, '%Y-%m-%d') +datetime.timedelta(hours = 15, minutes=31) for it in mydata.ExpiryDates]     
mydata.ExpiryTimes = [it for it in mydata.ExpiryTimes if (it.date() >= DataStartDate and it.date() <= DataEndDate)]

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'NSEBANK INDEX')# 'BSE100 INDEX'
allstocks = []
ok = [allstocks.extend(i) for i in mydata.indexcomponents.values()]
mydata.allStocksinBloom = list(set(allstocks))
mydata.DictBloomNSE = QueryScripMaster(priceconn, mydata.allStocksinBloom, 'Bloomberg', 'NSE')
mydata.allStocksinNSE = [mydata.DictBloomNSE[it] for it in mydata.allStocksinBloom]
mydata.allStocksinNSE.sort()
mydata.allStocksinNSE.append(INDEXNAME)

#mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['AF1 INDEX'], 'PX_LAST', DataStartDate)
mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, ['NSEBANK Index'], 'PX_LAST', DataStartDate)

mydata.Index.MarketCap = GetDataForTickersFromBloomDB(priceconn, mydata.allStocksinBloom, 'MCAP', DataStartDate)
mydata.Index.MarketCap.columns = [mydata.DictBloomNSE[it] for it in mydata.Index.MarketCap.columns]
indexData = GetDataForIndicesFromBloomDB(priceconn, ['NSEBANK Index'], 'MCAP', DataStartDate)
mydata.Index.MarketCap = pandas.concat([mydata.Index.MarketCap, indexData], axis = 1)

print('Stage-1 Completed')
mydata.Tickers = []
for ind, item in enumerate(mydata.ExpiryTimes[:-1]):
    iStart = item#.date()# -datetime.timedelta(days = 1)
    iEnd = mydata.ExpiryTimes[ind+1]#.date()
    expiryDate = iEnd.strftime('%d%b%y').upper()
    compDate = [i for i in mydata.indexcomponents.keys() if i <= iStart][-1]
    nseComponents = mydata.indexcomponents[compDate]
    nseComponents = [mydata.DictBloomNSE[i] for i in nseComponents]
    tempMCap = mydata.Index.MarketCap.loc[:iStart].iloc[-1]
    #tempMCap = tempMCap.loc[tempMCap.index.isin(nseComponents)]
    #tempMCap = tempMCap.sort_values().loc[-StocksCount:]
    nseComponents.append(INDEXNAME)    

    stockFutsPrice = GetNSEBhavCopyFutsData(bhavconn, secNames = nseComponents, fieldName = 'Close', expiry = expiryDate, fromDate = iStart.date() - datetime.timedelta(days = 1), toDate = iEnd.date())
    try:
        mydata.Index.Close = pandas.merge(mydata.Index.Close, stockFutsPrice, how = 'outer')#pandas.concat([mydata.Index.Close, stockFutsPrice], axis = 0)
    except:
        mydata.Index.Close = stockFutsPrice
    
    #tickersList = stockFutsPrice.columns
    StrikesDiffDict = GetNSEBhavCopyStrikePointsDiff(conn = bhavconn, secNames = stockFutsPrice.columns, expiry = iEnd.strftime('%d%b%y').upper())
    StrikeDiffDF = pandas.DataFrame(StrikesDiffDict, index = ['StrikeDiff']).transpose()
    
    iDate = datetime.datetime(iStart.date().year, iStart.date().month, iStart.date().day)
    curPriceSer = stockFutsPrice.loc[iDate].dropna()
    df = pandas.concat([curPriceSer, StrikeDiffDF], axis = 1).rename(columns={iDate: 'Price'}).dropna()
    
    tickersDF = numpy.multiply(numpy.divide(df.Price, df.StrikeDiff).astype(int), df.StrikeDiff).astype(int)
    tickersDF = pandas.concat([pandas.DataFrame(tickersDF, columns = ['ATM']), StrikeDiffDF], axis = 1).dropna()
    tickersDF['UpOtm'] = (tickersDF.ATM + delta*tickersDF.StrikeDiff).astype(int)
    tickersDF['DownOtm'] = (tickersDF.ATM - delta*tickersDF.StrikeDiff).astype(int)
    
    tickers1 = [it+expiryDate+'CE'+str(tickersDF.loc[it, 'UpOtm']) for it in tickersDF.index]
    tickers2 = [it+expiryDate+'PE'+str(tickersDF.loc[it, 'DownOtm']) for it in tickersDF.index]    
    #tickers = [it+expiryDate+iR+str(tickersDF.loc[it]) for iR in ['CE', 'PE', 'CA', 'PA'] for it in tickersDF.index]
    mydata.Tickers.extend(tickers1)
    mydata.Tickers.extend(tickers2)
    print(iStart.date())
    
    
mydata.Close = GetNSEBhavCopyDatabyTicker(conn = bhavconn, tickers = mydata.Tickers, fieldName = 'Close')
#PriceData.columns = [it.replace(expiryDate+'CA', expiryDate+'CE').replace(expiryDate+'PA', expiryDate+'CE') for it in PriceData.columns]
#mydata.Close = PriceData
#     try:
#         mydata.Close = pandas.merge(mydata.Close, PriceData.loc[iDate:], how = 'outer')#pandas.concat([mydata.Close, PriceData.loc[iDate:]], axis = 0)
#     except:
#         mydata.Close = PriceData.loc[iDate:]
 
# #f = open('Z:/Pickles/Hist_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')

if os.path.exists('Z:/Pickles/'):
    f = open('Z:/Pickles/BankNiftyDispersionData_1StrikeAway'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
else:
    f = open('G:/Shared drives/BackTests/Pickles/BankNiftyDispersionData_1StrikeAway'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
    #pickleFile = 'G:/Shared drives/BackTests/Pickles/NiftyDispersionData17Mar-2023.pkl'

pickle.dump(mydata, f)
f.close()
t2 = time.time()
print('Data Building Completed:', round((t2-t1)/60, 1), 'Mins!')