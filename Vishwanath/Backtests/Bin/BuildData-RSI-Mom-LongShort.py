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


resamplFreq = 'w-THU'#'2QS-NOV'#'w-FRI'#'w-FRI'#'A-JUN'#'w-FRI' #'2QS-APR'#'A-Mar'#
#YearlyFreqCount = 52 if resamplFreq[0].lower() == 'w' else (12 if resamplFreq[0].lower() == 'm' else ( 4 if resamplFreq[0].lower() == 'q' else ( 1 if resamplFreq[0].lower() == 'a' else 252)))


priceconn = GetConn('PriceData')
analystconn = GetConn('AnalystData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2005, 1, 1)

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE100 INDEX')
mydata.indexpriceDaily = GetDataForIndicesFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST')

allstocks = ['ESCORTS IN', 'IDFC IN',  'MSIL IN',  'RBK IN',  'GPL IN', 'LTI IN', 'MTCL IN', 'ADE IN', 'INFOE IN', 'ABB IN', 'TATA IN', 'CROMPTON IN', 'TECHM IN', 'VEDL IN', 'BRCM IN',
  'IRCTC IN', 'HUVR IN', 'TRENT IN', 'AUBANK IN', 'GRASIM IN', 'HNDL IN', 'JSTL IN', 'SRF IN', 'TTAN IN', 'TTCH IN', 'ZYDUSLIF IN', 'ARBP IN', 'CCRI IN', 'DIVI IN', 'HCLT IN',
   'IGL IN', 'JSP IN', 'LPC IN', 'NMDC IN', 'SBILIFE IN', 'TPW IN', 'VOLT IN', 'BHEL IN', 'BOB IN', 'DRRD IN', 'IDEA IN', 'IHFL IN', 'INDUSTOW IN', 'IOCL IN', 'KKC IN', 'MPHL IN',
   'ONGC IN', 'PIEL IN', 'PNB IN', 'Z IN', 'IH IN', 'BAF IN', 'BJFIN IN', 'WPRO IN', 'IDFCFB IN', 'SBICARD IN', 'TATACONS IN', 'IIB IN', 'MRF IN', 'NACL IN', 'UPLL IN']


[allstocks.extend(i) for i in mydata.indexcomponents.values()]
allstocks = set(allstocks)
mydata.allstocksinbloom = allstocks

basesqlquery = 'select distinct(Ticker) as Ticker from FutLookUpTable;'
curs = priceconn.cursor()
curs.execute(basesqlquery)
qw = curs.fetchall()
allstocks = [i[0].replace(' IS', ' IN') for i in qw]

futDict = QueryFutTickers(priceconn, allstocks)
futDict['NZ1 INDEX'] = 'NIFTY INDEX'
futDictInv = dict([(futDict[k], k) for k in futDict.keys()])

mydata.CloseDaily = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_LAST', DataStartDate)
mydata.CloseDaily.columns = [futDict[i] for i in mydata.CloseDaily.columns]
mydata.CloseDaily = mydata.CloseDaily.sort_index()
mydata.Close = mydata.CloseDaily

#mydata.Close = mydata.CloseDaily.resample(resamplFreq, convention='end').last()

#mydata.Close = pandas.concat([mydata.CloseDaily, mydata.Close], axis = 0)
#mydata.Close = mydata.Close.sort_index()
#mydata.Close = mydata.Close[~mydata.Close.index.duplicated(keep = 'last')]


mydata.Open = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_OPEN', DataStartDate)
mydata.Open.columns = [futDict[i] for i in mydata.Open.columns]
mydata.Open = mydata.Open.sort_index()

mydata.High = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_HIGH', DataStartDate)
mydata.High.columns = [futDict[i] for i in mydata.High.columns]
mydata.High = mydata.High.sort_index()

mydata.Low = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_LOW', DataStartDate)
mydata.Low.columns = [futDict[i] for i in mydata.Low.columns]
mydata.Low = mydata.Low.sort_index()

mydata.Close['Cash'] = [1.00]*mydata.Close.shape[0]
mydata.Open['Cash'] = [1.00]*mydata.Open.shape[0]
mydata.High['Cash'] = [1.00]*mydata.High.shape[0]
mydata.Low['Cash'] = [1.00]*mydata.Low.shape[0]

mydata.indexpriceDaily = mydata.indexpriceDaily.sort_index()
mydata.indexprice = mydata.indexpriceDaily.resample(resamplFreq, convention='end').last()
mydata.indexprice = pandas.concat([mydata.indexpriceDaily, mydata.indexprice], axis = 0)
mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]
mydata.indexprice = mydata.indexprice[~mydata.indexprice.index.duplicated(keep = 'last')]
mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]



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


f = open('Z:/Pickles/All_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
