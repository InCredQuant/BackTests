# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 16:44:59 2022

@author: Viren@Incred
Builds the data for Index Futs Trading Strategy
"""


import sqlite3
import pandas
from GetData import GetConn, QueryFutTickers
from GetData import *#GetComponentsForIndexForDateRange, GetDataForIndicesFromBloomDB, GetDataForFutTickersFromBloomDB, GetConn, QueryFutTickers
import MyTechnicalLib
import time
import pickle
import warnings
warnings.filterwarnings("ignore")
priceconn = GetConn('PriceData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2001, 1, 1)#datetime.date(2020, 12, 31)#datetime.date(2006, 6, 25)#datetime.date(2008, 12, 29)#
#reSampleFreq = 'w-FRI'#'w-FRI'

mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, ['NIFTY INDEX'], 'PX_LAST', DataStartDate)#NZ1
#mydata.indexprice = mydata.indexprice.resample('w-FRI', convention = 'end').last()


#allIndices = ['NIFTY INDEX', 'NSEBANK INDEX', 'NMIDSELP INDEX']##, 'NZ2 Index', 'AF2 Index']# NMIDSELPis Nifty mid Cap Select Index Spot
allIndices = ['NZ1 Index', 'AF1 Index', 'RNS1 Index']
mydata.Close = GetDataForIndicesFromBloomDB(priceconn, allIndices, 'PX_LAST', DataStartDate)#GetDataForFutTickersFromBloomDB

mydata.Close = mydata.Close.sort_index()
#mydata.Close = mydata.Close.resample(reSampleFreq, convention = 'end').last()

mydata.Open = GetDataForIndicesFromBloomDB(priceconn, allIndices, 'PX_OPEN', DataStartDate)#GetDataForFutTickersFromBloomDB
mydata.Open = mydata.Open.sort_index()
#mydata.Open = mydata.Open.resample(reSampleFreq, convention = 'end').first()

mydata.High = GetDataForIndicesFromBloomDB(priceconn, allIndices, 'PX_HIGH', DataStartDate)#GetDataForFutTickersFromBloomDB
mydata.High = mydata.High.sort_index()
#mydata.High = mydata.High.resample(reSampleFreq, convention = 'end').max()

mydata.Low = GetDataForIndicesFromBloomDB(priceconn, allIndices, 'PX_LOW', DataStartDate)#GetDataForFutTickersFromBloomDB
mydata.Low = mydata.Low.sort_index()
#mydata.Low = mydata.Low.resample(reSampleFreq, convention = 'end').min()

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

mydata.CloseWeekly = mydata.Close.resample('w-FRI', convention = 'end').last()
mydata.SMA10Weekly = MyTechnicalLib.MovingAverage(mydata.CloseWeekly, 10)
mydata.SMA20Weekly = MyTechnicalLib.MovingAverage(mydata.CloseWeekly, 20)

mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')


f = open('G:/Shared drives/QuantFunds/Liquid1/DataPickles/IndexFutsData_Daily'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
