# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 16:14:32 2024

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
from GetDataPostGres import DataBaseConnect
import datetime
warnings.filterwarnings("ignore")
priceconn = GetConn('PriceData')
dbObj = DataBaseConnect()
dbObj.Connect()

mydata = MyBacktestData()
DataStartDate = datetime.date(2010, 9, 1)#(2021, 12, 31)#datetime.date(2022, 12, 31)#datetime.date(2008, 12, 29)#
DatEndDate = datetime.date(2025, 7, 31)

allIndices = ['MIDCPNIFTY-I.NFO', 'NIFTY-I.NFO','BANKNIFTY-I.NFO']

timeList = []
start_time = datetime.datetime.strptime('11:14:59', '%H:%M:%S')
current_time = start_time
marketOpen = datetime.datetime.strptime('09:15:59', '%H:%M:%S')
marketClose = datetime.datetime.strptime('15:29:59', '%H:%M:%S')

while current_time > marketOpen and current_time < marketClose:  # Assuming you want to generate times until 23:59:59
    timeList.append(current_time.strftime('%H:%M:%S'))
    current_time += datetime.timedelta(minutes=120)

#timeList = ['09:29:59', '09:59:59', '10:29:59', '10:59:59', '11:29:59', '11:59:59', '12:29:59', '12:59:59', '13:29:59', '13:59:59', '14:29:59', '14:59:59']
mydata.Close = dbObj.getMultiTickersMultiTimeMinData(DataStartDate.strftime('%Y-%m-%d'), DatEndDate.strftime('%Y-%m-%d'), timeList, allIndices, fieldName = 'Close')


mydata.indexprice = mydata.Close.loc[:, 'NIFTY-I']

mydata.Open = dbObj.getMultiTickersMultiTimeMinData(DataStartDate.strftime('%Y-%m-%d'), DatEndDate.strftime('%Y-%m-%d'), timeList, allIndices, fieldName = 'Open')
mydata.High = dbObj.getMultiTickersMultiTimeMinData(DataStartDate.strftime('%Y-%m-%d'), DatEndDate.strftime('%Y-%m-%d'), timeList, allIndices, fieldName = 'High')
mydata.Low = dbObj.getMultiTickersMultiTimeMinData(DataStartDate.strftime('%Y-%m-%d'), DatEndDate.strftime('%Y-%m-%d'), timeList, allIndices, fieldName = 'Low')

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

#mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')


f = open('G:/Shared drives/QuantFunds/Liquid1/DataPickles/IndexFutsData_2Hourly'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
