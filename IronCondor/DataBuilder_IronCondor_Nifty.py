# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 14:14:29 2023


@author: Viren@InCred
Usage- Data Builder for Iron Condor, Takes the Positon for Weekly Expiry, Strikes
BNF Data From 2Jun16 Expiry
NF Data From 7 Mar 2019 Expiry
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

INDEXNAME = 'NIFTY'
strikeDiff = 100
Legs = [0.04, 0.10] # 4% % 10 % respectively

t1 = time.time()
priceconn = GetConn('PriceData', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
#analystconn = GetConn('AnalystData')
bhavconn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')

DataStartDate = datetime.date(2019, 3, 1)
DataEndDate = datetime.date(2022, 12, 29)# or latest date


mydata = MyBacktestData()
mydata.Index = Index()

mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Weekly')
mydata.ExpiryTimes = [datetime.datetime.strptime(it, '%Y-%m-%d') +datetime.timedelta(hours = 15, minutes=31) for it in mydata.ExpiryDates]     
mydata.ExpiryTimes = [it for it in mydata.ExpiryTimes if (it.date() >= DataStartDate and it.date() <= DataEndDate)]

# mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'NIFTY INDEX')# 'BSE100 INDEX'
# allstocks = []
# ok = [allstocks.extend(i) for i in mydata.indexcomponents.values()]
# mydata.allStocksinBloom = list(set(allstocks))
# mydata.DictBloomNSE = QueryScripMaster(priceconn, mydata.allStocksinBloom, 'Bloomberg', 'NSE')
# mydata.allStocksinNSE = [mydata.DictBloomNSE[it] for it in mydata.allStocksinBloom]
# mydata.allStocksinNSE.sort()
# mydata.allStocksinNSE.append(INDEXNAME)

#mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST', DataStartDate)
mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, ['NIFTY Index'], 'PX_LAST', DataStartDate)

#mydata.Index.MarketCap = GetDataForTickersFromBloomDB(priceconn, mydata.allStocksinBloom, 'MCAP', DataStartDate)
#mydata.Index.MarketCap.columns = [mydata.DictBloomNSE[it] for it in mydata.Index.MarketCap.columns]
#indexData = GetDataForIndicesFromBloomDB(priceconn, ['NIFTY Index'], 'MCAP', DataStartDate)
#mydata.Index.MarketCap = pandas.concat([mydata.Index.MarketCap, indexData], axis = 1)

print('Stage-1 Completed')
mydata.Tickers = []
for ind, item in enumerate(mydata.ExpiryTimes[:-1]):
    iStart = item#.date()# -datetime.timedelta(days = 1)
    iEnd = mydata.ExpiryTimes[ind+1]#.date()
    expiryDate = iEnd.strftime('%d%b%y').upper()
    iDate = datetime.datetime(iStart.date().year, iStart.date().month, iStart.date().day)
    
    indexPrice = mydata.indexprice.loc[iDate].values[0]
    atmStrike = int(indexPrice/strikeDiff)*strikeDiff
    iStrikes = [atmStrike]
    ok = [iStrikes.extend([atmStrike - int(it*atmStrike/strikeDiff)*strikeDiff, atmStrike + int(it*atmStrike/strikeDiff)*strikeDiff]) for it in Legs]
    
    mydata.Tickers.extend([INDEXNAME + expiryDate + 'CE' + str(it) for it in iStrikes if it >= atmStrike])
    mydata.Tickers.extend([INDEXNAME + expiryDate + 'PE' + str(it) for it in iStrikes if it <= atmStrike])
    
mydata.Close = GetNSEBhavCopyDatabyTicker(conn = bhavconn, tickers = mydata.Tickers, fieldName = 'Close')
 
# #f = open('Z:/Pickles/Hist_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')

if os.path.exists('Z:/Pickles/'):
    f = open('Z:/Pickles/NiftyIronCondorData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
else:
    f = open('G:/Shared drives/BackTests/Pickles/NiftyIronCondorData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
    #pickleFile = 'G:/Shared drives/BackTests/Pickles/NiftyDispersionData17Mar-2023.pkl'

pickle.dump(mydata, f)
f.close()
t2 = time.time()
print('Data Building Completed:', round((t2-t1)/60, 1), 'Mins!')