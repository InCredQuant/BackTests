# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 23:07:58 2023

@author: Viren@InCred
Usage- Data Builder for BankNifty Calendar Spread
1. Same Strikes// Straddle // Strangle Calendar Spread
2. Current Week 3% Strangle Short Next Week 6% Strangle Long
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
import pdb

import warnings
warnings.filterwarnings("ignore")

INDEXNAME = 'BANKNIFTY'#'BANKNIFTY'
nearOtm = 0.00/2# 3Percent
farOtm = 0.10/2# 6 Percent

strikeDiff = 100
IndexMoveLimit = 4*strikeDiff
oneSideLegs = 3

t1 = time.time()
priceconn = GetConn('PriceData', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
#analystconn = GetConn('AnalystData')
bhavconn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')

DataStartDate = datetime.date(2016, 5, 25)#datetime.date(2016, 5, 31)
DataEndDate = datetime.date(2023, 1, 5)# or latest date


mydata = MyBacktestData()
mydata.Index = Index()

mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Weekly')#'Weekly'
mydata.ExpiryTimes = [datetime.datetime.strptime(it, '%Y-%m-%d') +datetime.timedelta(hours = 15, minutes=31) for it in mydata.ExpiryDates]     
mydata.ExpiryTimes = [it for it in mydata.ExpiryTimes if it.date() <= DataEndDate]
#mydata.ExpiryTimes = [it for it in mydata.ExpiryTimes if (it.date() >= DataStartDate and it.date() <= DataEndDate)]

#mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST', DataStartDate)
mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, ['NSEBANK Index'], 'PX_LAST', DataStartDate)#['NSEBANK Index']

print('Stage-1 Completed')
mydata.Tickers = []
mydata.TradingDates = {}
for ind, item in enumerate(mydata.ExpiryTimes[:-2]):
    iStart = item#.date()# -datetime.timedelta(days = 1)
    nearExpiry = mydata.ExpiryTimes[ind+1]#.date()
    nextExpiry = mydata.ExpiryTimes[ind+2]
    nearExpiryDate = nearExpiry.strftime('%d%b%y').upper()
    nextExpiryDate = nextExpiry.strftime('%d%b%y').upper()
    iDate = datetime.datetime(iStart.date().year, iStart.date().month, iStart.date().day)
    priceSer = mydata.indexprice.loc[iDate:nearExpiry]
    # allDates = [iDate]
    # for iIndex in priceSer.index[1:-1]:
    #     indexChg = priceSer.loc[iIndex] - priceSer.loc[:allDates[-1]].iloc[-1]
    #     if numpy.abs(indexChg.values[0]) >= IndexMoveLimit:
    #         allDates.append(iIndex)
    
    #for activeDate in allDates:
    #iDate = priceSer.loc[:activeDate].index[-1]        
    indexPrice = priceSer.loc[iDate].values[0]
    atmStrike = int(indexPrice/strikeDiff)*strikeDiff
    curWeekStrikes = [atmStrike - int(nearOtm*atmStrike/strikeDiff)*strikeDiff, atmStrike + int(nearOtm*atmStrike/strikeDiff)*strikeDiff]
    nextWeekStrikes = [atmStrike - int(farOtm*atmStrike/strikeDiff)*strikeDiff, atmStrike + int(farOtm*atmStrike/strikeDiff)*strikeDiff]
    
    curWkTicker = [INDEXNAME+nearExpiryDate+opt+str(it) for opt in ['CE', 'PE'] for it in curWeekStrikes]
    #curWkTicker = [INDEXNAME+nearExpiryDate+('CE' if it >= atmStrike else 'PE')+str(it) for it in curWeekStrikes]
    nextWkTicker = [INDEXNAME+nextExpiryDate+('CE' if it >= atmStrike else 'PE')+str(it) for it in nextWeekStrikes]
    allTicker = set.union(set(curWkTicker), set(nextWkTicker))
    mydata.Tickers.extend(allTicker)
    mydata.TradingDates[iDate.date()] = {'Short': set(curWkTicker), 'Long': set(nextWkTicker)}
    print(iDate.date(), len(allTicker))

mydata.Close = GetNSEBhavCopyDatabyTicker(conn = bhavconn, tickers = list(set(mydata.Tickers)), fieldName = 'Close')
print('out of:', len(list(set(mydata.Tickers))), len(mydata.Close.columns), ' downloaded!', sep = ' ')

if os.path.exists('Z:/Pickles/'):
    f = open('Z:/Pickles/BankNiftyCalendarSpread_StraddleCurWk_10pctNextWk'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
else:
    f = open('G:/Shared drives/BackTests/Pickles/BankNiftyCalendarSpread_StraddleCurWk_10pctNextWk'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
    #pickleFile = 'G:/Shared drives/BackTests/Pickles/NiftyDispersionData17Mar-2023.pkl'

pickle.dump(mydata, f)
f.close()
t2 = time.time()
print('Data Building Completed:', round((t2-t1)/60, 1), 'Mins!')