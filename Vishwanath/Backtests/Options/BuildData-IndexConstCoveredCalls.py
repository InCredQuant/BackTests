# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 16:25:51 2022

@author: Viren@Incred
To Build the data Strucutres for backtesting and all
"""

import sqlite3
import pandas
from GetData import *
#import MyTechnicalLib
import time
#from combinenewdata_fromcapitaline_ import *
#from FactorsDef import *
from GetDataPostGres import DataBaseConnect
import warnings
warnings.filterwarnings("ignore")
import datetime
#resamplFreq = 'm'#'2QS-NOV'#'w-FRI'#'A-JUN'#'w-FRI' #'2QS-APR'#'A-Mar'#
#YearlyFreqCount = 52 if resamplFreq[0].lower() == 'w' else (12 if resamplFreq[0].lower() == 'm' else ( 4 if resamplFreq[0].lower() == 'q' else ( 1 if resamplFreq[0].lower() == 'a' else 252)))

t1 = time.time()

priceconn = GetConn('PriceData')
analystconn = GetConn('AnalystData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2000, 1, 1)
DataEndDate = datetime.date(2024, 3, 1)
indexName = 'NIFTY INDEX'
mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), indexName)

allstocks = []
[allstocks.extend(i) for i in mydata.indexcomponents.values()]
allstocks = set(allstocks)
mydata.allstocksinbloom = allstocks
#mydata.bloomtocapitalinemapping = QueryScripMaster(resconn, list(allstocks), 'Bloomberg', 'CompanyCode')
#mydata.allstocksincapitaline = [mydata.bloomtocapitalinemapping.get(i) for i in allstocks]

mydata.SpotPrice = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_LAST', DataStartDate)
mydata.SpotPrice = mydata.SpotPrice.sort_index()
mydata.SpotPrice = mydata.SpotPrice.loc[DataStartDate:]



mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, [indexName], 'PX_LAST')
mydata.indexprice = mydata.indexprice.sort_index()
mydata.indexprice = mydata.indexprice.loc[DataStartDate:]
#mydata.indexprice = mydata.indexpriceDaily.resample(resamplFreq, convention='end').last()
#mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]
### End##
mydata.NSEMapping = QueryScripMaster(priceconn, mydata.SpotPrice, 'Ticker', 'NSE')
mydata.NSEMapping = {key: value for key, value in mydata.NSEMapping.items() if value != None}

gd = DataBaseConnect()
gd.Connect()

mydata.ExpiryDates = gd.getExpiryDates('monthly')
mydata.ExpiryDates = [it for it in mydata.ExpiryDates if (it >= DataStartDate and it <= DataEndDate)]
mydata.ExpiryDates = [datetime.date.strftime(it, '%Y-%m-%d') for it in mydata.ExpiryDates]  # +datetime.timedelta(hours = 15, minutes=31)


mydata.Close = getNseBhavCopyDataSymbols(symbols = list(mydata.NSEMapping.values()), fromDate = mydata.ExpiryDates[-2], toDate = mydata.ExpiryDates[-1], expiryDate = '', fieldName = 'Close')
#replace_exp = {'23APR14':'24APR14'}
#for idx in range(1, len(mydata.ExpiryDates)-1):
#    start = mydata.ExpiryTimes[idx-1]
#    end = mydata.ExpiryTimes[idx]
#    exp = end.strftime('%d%b%y').upper()
#    if exp == '23APR14':
#        exp = replace_exp[exp]

mydata.Open = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_OPEN', DataStartDate)
mydata.Open = mydata.Open.resample(resamplFreq, convention = 'end').first()


mydata.High = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_HIGH', DataStartDate)
mydata.High = mydata.High.resample(resamplFreq, convention = 'end').max()

mydata.Low = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_LOW', DataStartDate)
mydata.Low = mydata.Open.resample(resamplFreq, convention = 'end').min()


#mydata.PriceVolatility = MyTechnicalLib.GetMovingVolatility(mydata.CloseDaily, 60).resample(resamplFreq, convention = 'end').last()[DataStartDate:]
#mydata.LongPriceVolatility = MyTechnicalLib.GetMovingVolatility(mydata.CloseDaily, 500).resample(resamplFreq, convention = 'end').last()[DataStartDate:]
mydata.Ret = mydata.CloseDaily.pct_change().resample(resamplFreq, convention = 'end').mean()[DataStartDate:]#.fillna(method = 'bfill')
#mydata.RetVolatility = MyTechnicalLib.GetMovingVolatility(mydata.CloseDaily.pct_change(), 60).resample(resamplFreq, convention = 'end').last()[DataStartDate:]
mydata.fifty2weeklow = mydata.CloseDaily.rolling(window = 250, min_periods = 250).min().resample(resamplFreq, convention = 'end').last()[DataStartDate:]#pandas.rolling_apply(mydata.CloseDaily, 250, min).resample(resamplFreq, how = 'last')[DataStartDate:]
mydata.fifty2weekhigh = mydata.CloseDaily.rolling(window = 250, min_periods = 250).max().resample(resamplFreq, convention = 'end').last()[DataStartDate:]#pandas.rolling_apply(mydata.CloseDaily, 250, max).resample(resamplFreq, how = 'last')[DataStartDate:]
#mydata.DrawDown = (mydata.CloseDaily.rolling(window = 250, min_periods = 250).apply(MyTechnicalLib.MeanDrawDowm)/mydata.CloseDaily).resample(resamplFreq, convention = 'end').last()[DataStartDate:]
#mydata.MaxDrawDown = (mydata.CloseDaily.rolling(window = 250, min_periods = 250).apply(MyTechnicalLib.Max_DrawDown)/mydata.CloseDaily).resample(resamplFreq, convention = 'end').last()[DataStartDate:]
    
mydata.BEST_EPS = GetDataForBESTFromBloomDB(analystconn, mydata.allstocksinbloom, 'BEST_EPS', freq = resamplFreq)
mydata.BEST_EPS = mydata.BEST_EPS[mydata.BEST_EPS.index.isin(mydata.Close.index)]
#mydata.BEST_EPS_LO = GetDataForBESTFromBloomDB(tickerpriceconn, mydata.allstocksinbloom, 'BEST_EPS_LO', freq = 'w')
#mydata.BEST_EPS_LO = mydata.BEST_EPS_LO.resample('w', convention = 'end').min()
#mydata.BEST_EPS_HI = GetDataForBESTFromBloomDB(tickerpriceconn, mydata.allstocksinbloom, 'BEST_EPS_HI', freq = 'w')
#mydata.BEST_EPS_HI = mydata.BEST_EPS_HI.resample('w', convention = 'end').max()
mydata.BEST_EPS_STDDEV = GetDataForBESTFromBloomDB(analystconn, mydata.allstocksinbloom, 'BEST_EPS_STDDEV', freq = resamplFreq)
mydata.BEST_EPS_STDDEV = mydata.BEST_EPS_STDDEV.resample(resamplFreq, convention = 'end').mean()
mydata.BEST_EPS_STDDEV = mydata.BEST_EPS_STDDEV[mydata.BEST_EPS_STDDEV.index.isin(mydata.Close.index)]
#mydata.BEST_LTG_EPS = GetDataForBESTFromBloomDB(tickerpriceconn, mydata.allstocksinbloom, 'BEST_LTG_EPS', freq = 'w')
#mydata.EPS_Dispersion = mydata.BEST_EPS_STDDEV.resample('w', convention = 'end').last()