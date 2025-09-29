# -*- coding: utf-8 -*-
"""
Created on 13 Sep 2022

@author: Viren@InCred
Data Builder
"""

import sqlite3
import pandas
from GetData import GetConn, QueryFutTickers
from GetData import *#GetComponentsForIndexForDateRange, GetDataForIndicesFromBloomDB, GetDataForFutTickersFromBloomDB, GetConn, QueryFutTickers
import MyTechnicalLib
import time
#from FactorsDef import *
import pickle

import warnings
warnings.filterwarnings("ignore")


resamplFreq = 'w-FRI'#'2QS-NOV'#'w-FRI'#'w-FRI'#'A-JUN'#'w-FRI' #'2QS-APR'#'A-Mar'#
#YearlyFreqCount = 52 if resamplFreq[0].lower() == 'w' else (12 if resamplFreq[0].lower() == 'm' else ( 4 if resamplFreq[0].lower() == 'q' else ( 1 if resamplFreq[0].lower() == 'a' else 252)))


priceconn = GetConn('PriceData')
analystconn = GetConn('AnalystData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2005, 1, 1)

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE200 INDEX')
mydata.indexpriceDaily = GetDataForIndicesFromBloomDB(priceconn, ['BSE200 INDEX'], 'PX_LAST')

allstocks = []

[allstocks.extend(i) for i in mydata.indexcomponents.values()]
allstocks = set(allstocks)
mydata.allstocksinbloom = allstocks



mydata.CloseDaily = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_LAST', DataStartDate)
mydata.CloseDaily = mydata.CloseDaily.sort_index()
mydata.Close = mydata.CloseDaily

mydata.CloseNifty = GetDataForFutTickersFromBloomDB(priceconn, 'NZ1 Index', 'PX_LAST', DataStartDate)
mydata.Close = pandas.concat([mydata.Close, mydata.CloseNifty], axis = 1).resample(resamplFreq, convention = 'end').last()

mydata.Open = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_OPEN', DataStartDate)
mydata.Open = mydata.Open.sort_index()

mydata.OpenNifty = GetDataForFutTickersFromBloomDB(priceconn, 'NZ1 Index', 'PX_OPEN', DataStartDate)
mydata.Open = pandas.concat([mydata.Open, mydata.OpenNifty], axis = 1).resample(resamplFreq, convention = 'start').first()

mydata.High = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_HIGH', DataStartDate)
mydata.High = mydata.High.sort_index()

mydata.HighNifty = GetDataForFutTickersFromBloomDB(priceconn, 'NZ1 Index', 'PX_HIGH', DataStartDate)
mydata.High = pandas.concat([mydata.High, mydata.HighNifty], axis = 1).resample(resamplFreq, convention = 'end').max()


mydata.Low = GetDataForTickersFromBloomDB(priceconn, mydata.allstocksinbloom, 'PX_LOW', DataStartDate)
mydata.Low = mydata.Low.sort_index()

mydata.LowNifty = GetDataForFutTickersFromBloomDB(priceconn, 'NZ1 Index', 'PX_LOW', DataStartDate)
mydata.Low = pandas.concat([mydata.Low, mydata.LowNifty], axis = 1).resample(resamplFreq, convention = 'end').min()

mydata.Close['Cash'] = [1.00]*mydata.Close.shape[0]
mydata.Open['Cash'] = [1.00]*mydata.Open.shape[0]
mydata.High['Cash'] = [1.00]*mydata.High.shape[0]
mydata.Low['Cash'] = [1.00]*mydata.Low.shape[0]

mydata.indexpriceDaily = mydata.indexpriceDaily.sort_index()
mydata.indexprice = mydata.indexpriceDaily.resample(resamplFreq, convention='end').last()
# mydata.indexprice = pandas.concat([mydata.indexpriceDaily, mydata.indexprice], axis = 0)
# mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]
# mydata.indexprice = mydata.indexprice[~mydata.indexprice.index.duplicated(keep = 'last')]
# mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]



mydata.RSI14 = MyTechnicalLib.GetRSI(mydata.Close, 14)
mydata.RSI5 =  MyTechnicalLib.GetRSI(mydata.Close, 5)
mydata.RSI2 =  MyTechnicalLib.GetRSI(mydata.Close, 2)

mydata.SMA10 = MyTechnicalLib.MovingAverage(mydata.Close,10)
mydata.SMA20 = MyTechnicalLib.MovingAverage(mydata.Close,20)
mydata.SMA21 = MyTechnicalLib.MovingAverage(mydata.Close,21)
mydata.SMA24 = MyTechnicalLib.MovingAverage(mydata.Close,24)
mydata.SMA50 = MyTechnicalLib.MovingAverage(mydata.Close,50)
mydata.SMA100 = MyTechnicalLib.MovingAverage(mydata.Close,100)


mydata.EMA21 = MyTechnicalLib.GetEMA(mydata.Close, 21 , ShiftDays=0)
mydata.WMA20 = MyTechnicalLib.GetWMA(mydata.Close, 20)
mydata.MACD, mydata.MACDSignal = MyTechnicalLib.MACD(mydata.Close)

mydata.ATR24 = MyTechnicalLib.GetATR(mydata, 24)

mydata.ROC27D = MyTechnicalLib.GetROC(mydata, 27)
mydata.ROCMA18D = MyTechnicalLib.MovingAverage(mydata.ROC27D, 18)

mydata.NVI, mydata.PVI = MyTechnicalLib.VortexOscillator(mydata, 14)
mydata.LS, mydata.LR = MyTechnicalLib.RegressionCrossOverSignal(mydata.Close, LSDays = 5, LRDays = 50)

mydata.IndexInclusionFactor = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')


f = open('Z:/Pickles/BSE200_WeeklyStocksData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
