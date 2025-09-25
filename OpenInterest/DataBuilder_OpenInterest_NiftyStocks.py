# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 14:43:00 2023
@author: Viren@InCred

Write the Straddle based on Open Interest from the Nifty 50, top 20 Selected Stocks Options.
1. Selecting Top 20 based on Mcap
2. Selecting  Top 20 based on Liquidity

"""


import datetime
#import sqlite3
import pandas
#from GetData import GetConn, QueryFutTickers
from GetData import *
import MyTechnicalLib
import time
#from FactorsDef import *
import pickle
import math
import re

import warnings
warnings.filterwarnings("ignore")

INDEXNAME = 'BANKNIFTY'# 'NIFTY'
StocksCount = 21
OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d+[A-Z]+\d+)(?P<option_type>[A-Z]+)(?P<strike>\d+(\.\d+)?)'

t1 = time.time()
priceconn = GetConn('PriceData', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
#analystconn = GetConn('AnalystData')
bhavconn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')

DataStartDate = datetime.date(2014, 4, 24)
DataEndDate = datetime.date(2023, 4, 27)# or latest date


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

mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['AF1 INDEX'], 'PX_LAST', DataStartDate)

mydata.Index.MarketCap = GetDataForTickersFromBloomDB(priceconn, mydata.allStocksinBloom, 'MCAP', DataStartDate)
mydata.Index.MarketCap.columns = [mydata.DictBloomNSE[it] for it in mydata.Index.MarketCap.columns]
indexData = GetDataForIndicesFromBloomDB(priceconn, ['NIFTY Index'], 'MCAP', DataStartDate)
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
    #nseComponents.append(INDEXNAME) 
    tempMCap = mydata.Index.MarketCap.loc[:iStart].iloc[-1]
    tempMCap = tempMCap.loc[tempMCap.index.isin(nseComponents)]
    tempMCap = tempMCap.sort_values().iloc[-StocksCount:]
    scrips = list(tempMCap.index)
    scrips.append(INDEXNAME)
    
    openIntDF = GetNSEBhavCopyAllTickersDailyData(conn = bhavconn, symbols= scrips, fieldName = 'OPEN_INT', expiry = expiryDate, fromDate = iStart.date() - datetime.timedelta(days = 1), toDate = iEnd.date(), options = True )
    lookupTable = pandas.DataFrame([re.match(OptionTickerRegEx, iTicker).groupdict() for iTicker in openIntDF.columns], index = openIntDF.columns)
    lookupTable['Ticker'] = lookupTable.symbol + lookupTable.option_type
    tickerLookupDict = {iTicker: lookupTable[lookupTable.Ticker==iTicker].index for iTicker in list(set(lookupTable.Ticker))}
    ok = [mydata.Tickers.extend(list(set(openIntDF.loc[:, tickerLookupDict[iTicker]].idxmax(axis = 1)))) for iTicker in tickerLookupDict.keys()]# Get the tickers with maximum Open Interest
    #print(iStart.date())
    stockFutsPrice = GetNSEBhavCopyFutsData(bhavconn, secNames = scrips, fieldName = 'Close', expiry = expiryDate, fromDate = iStart.date() - datetime.timedelta(days = 1), toDate = iEnd.date())
    try:
        mydata.Index.Close = pandas.merge(mydata.Index.Close, stockFutsPrice, how = 'outer')#pandas.concat([mydata.Index.Close, stockFutsPrice], axis = 0)
    except:
        mydata.Index.Close = stockFutsPrice
    print(iStart.date())
    #tickersList = stockFutsPrice.columns
    #StrikesDiffDict = GetNSEBhavCopyStrikePointsDiff(conn = bhavconn, secNames = stockFutsPrice.columns, expiry = iEnd.strftime('%d%b%y').upper())
    #StrikeDiffDF = pandas.DataFrame(StrikesDiffDict, index = ['StrikeDiff']).transpose()
    
    #iDate = datetime.datetime(iStart.date().year, iStart.date().month, iStart.date().day)
    #curPriceSer = stockFutsPrice.loc[iDate].dropna()
    #df = pandas.concat([curPriceSer, StrikeDiffDF], axis = 1).rename(columns={iDate: 'Price'}).dropna()
    #tickersDF = numpy.multiply(numpy.divide(df.Price, df.StrikeDiff).astype(int), df.StrikeDiff).astype(int)
    
    #tickers = [it+expiryDate+iR+str(tickersDF.loc[it]) for iR in ['CE', 'PE', 'CA', 'PA'] for it in tickersDF.index]
    #mydata.Tickers.extend(tickers)
    #print(iStart.date())
    

mydata.Close = GetNSEBhavCopyDatabyTicker(conn = bhavconn, tickers = mydata.Tickers, fieldName = 'Close')
#mydata.Close.columns = [it.replace(expiryDate+'CA', expiryDate+'CE').replace(expiryDate+'PA', expiryDate+'CE') for it in mydata.Close.columns]

mydata.OpenInterest = GetNSEBhavCopyDatabyTicker(conn = bhavconn, tickers = mydata.Tickers, fieldName = 'OPEN_INT')
#mydata.OpenInterest.columns = [it.replace(expiryDate+'CA', expiryDate+'CE').replace(expiryDate+'PA', expiryDate+'CE') for it in mydata.OpenInterest.columns]

if os.path.exists('Z:/Pickles/'):
    f = open('Z:/Pickles/NiftyStocksOIData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
else:
    f = open('G:/Shared drives/BackTests/Pickles/NiftyStocksOIData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
    #pickleFile = 'G:/Shared drives/BackTests/Pickles/NiftyDispersionData17Mar-2023.pkl'

pickle.dump(mydata, f)
f.close()
t2 = time.time()
print('Data Building Completed:', round((t2-t1)/60, 0), 'Mins!')
