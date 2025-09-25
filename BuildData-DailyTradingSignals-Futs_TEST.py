
# -*- coding: utf-8 -*-
"""
Created on 13 Sep 2022

@author: Viren@InCred
Data Builder
"""
import datetime
#import sqlite3
#import pandas
import sys
sys.path.insert(1, 'G:\Shared drives\BackTests\pycode\MainLibs')
from MainLibs.GetData import GetConn, QueryFutTickers
from MainLibs.GetData import *#GetComponentsForIndexForDateRange, GetDataForIndicesFromBloomDB, GetDataForFutTickersFromBloomDB, GetConn, QueryFutTickers
import MainLibs.MyTechnicalLib as MyTechnicalLib
#import time
#from FactorsDef import *
# import pickle

import warnings
warnings.filterwarnings("ignore")

priceconn = GetConn('PriceData')
analystconn = GetConn('AnalystData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2022, 12, 29)#(2009, 12, 25)#
#DataStartDate = datetime.date(2020, 1, 1)

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE200 INDEX')# 'BSE100 INDEX'
mydata.indexpriceDaily = GetDataForIndicesFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST')

allstocks = ['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'ADE IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN', 'HDFC IN', 'TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'ADSEZ IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'NFIL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'GUJGA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN', 'INMART IN', 'IHFL IN', 'ICEM IN', 'CLGT IN', 'PLNG IN', 'UBBL IN', 'CANF IN', 'ARTO IN', 'CROMPTON IN', 'CUBK IN', 'ALKEM IN', 'DLPL IN', 'PVRINOX IN', 'TRP IN', 'INDA IN', 'IDEA IN', 'SUNTV IN', 'CHMB IN', 'MCX IN', 'METROHL IN', 'HCP IN', 'JKCE IN', 'SYNG IN', 'GRAN IN', 'RINDL IN', 'CRIN IN', 'IPCA IN', 'BOOT IN', 'ATLP IN', 'OFSS IN']

[allstocks.extend(i) for i in mydata.indexcomponents.values()]
allstocks = set(allstocks)
mydata.allstocksinbloom = allstocks

basesqlquery = 'select distinct(Ticker) as Ticker from FutLookUpTable;'
curs = priceconn.cursor()
curs.execute(basesqlquery)
qw = curs.fetchall()
AllList = [i[0].replace(' IS', ' IN') for i in qw]
allstocks = list(set.union(set(allstocks), set(AllList)))

futDict = QueryFutTickers(priceconn, allstocks)
futDict['NZ1 INDEX'] = 'NIFTY INDEX'
futDict['AF1 INDEX'] = 'NSEBANK INDEX'
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
mydata.indexprice = mydata.indexpriceDaily
mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]#.resample(resamplFreq, convention='end').last()
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

mydata.IndexInclusionFactorBSE200 = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')
mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE100 INDEX')# 'BSE100 INDEX'
mydata.IndexInclusionFactorBSE100 = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')
mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')

basePath = 'G:/Shared drives/QuantFunds/Liquid1/'

with open(basePath+'STFDMOM_'+ datetime.datetime.now().strftime('%Y%m%d%M%H%S') +'.pkl', 'wb') as file_obj:
    pickle.dump(mydata, file_obj)

# f = open(basePath+'STFDMOM_'+ datetime.datetime.now().strftime('%Y%m%d%M%H%S') +'.pkl', 'wb')
## f = open('Z:/Pickles/Hist_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
## f = open('Z:/Pickles/Live_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
# pickle.dump(mydata, f)
# f.close()
