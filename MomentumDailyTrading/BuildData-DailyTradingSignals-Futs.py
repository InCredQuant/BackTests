
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
#sys.path.insert(1, 'G:\Shared drives\BackTests\pycode\MainLibs')
#from MainLibs.GetData import GetConn, QueryFutTickers
from GetData import GetConn, QueryFutTickers
#from MainLibs.GetData import *#GetComponentsForIndexForDateRange, GetDataForIndicesFromBloomDB, GetDataForFutTickersFromBloomDB, GetConn, QueryFutTickers
from GetData import *
#import MainLibs.MyTechnicalLib as MyTechnicalLib
import MyTechnicalLib
#import time
#from FactorsDef import *
import pickle
import numpy as np
import warnings
warnings.filterwarnings("ignore")

priceconn = GetConn('PriceData')
analystconn = GetConn('AnalystData')

mydata = MyBacktestData()
#DataStartDate = datetime.date(2023, 12, 30)#(2012, 12, 27)#(2022, 12, 29)#(2012, 12, 27)#Changed to Model#(2009, 12, 31)##(2023, 12, 29)#(2020, 12, 29)#
DataStartDate = datetime.date(2010, 12, 27)#datetime.date(2020, 1, 1)

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE200 INDEX')# 'BSE100 INDEX'
mydata.indexpriceDaily = GetDataForIndicesFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST')

#LiqAllstocks = ['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN', 'TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'NFIL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'GUJGA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN']
#allstocks = ['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'ADE IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN','TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'ADSEZ IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'NFIL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'GUJGA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN', 'INMART IN', 'IHFL IN', 'ICEM IN', 'CLGT IN', 'PLNG IN', 'UBBL IN', 'CANF IN', 'ARTO IN', 'CROMPTON IN', 'CUBK IN', 'ALKEM IN', 'DLPL IN', 'PVRINOX IN', 'TRP IN', 'INDA IN', 'IDEA IN', 'SUNTV IN', 'CHMB IN', 'MCX IN', 'METROHL IN', 'HCP IN', 'JKCE IN', 'SYNG IN', 'GRAN IN', 'RINDL IN', 'CRIN IN', 'IPCA IN', 'BOOT IN', 'ATLP IN', 'OFSS IN']


allstocks = ['HDFCB IN', 'RELIANCE IN', 'ICICIBC IN', 'BAF IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BHARTI IN', 'TTMT IN', 'TCS IN', 'MM IN', 'LT IN', 'HNAL IN', 'TRENT IN', 'BHE IN', 'ZOMATO IN', 'TATA IN', 'MSIL IN', 'DIXON IN', 'HNDL IN', 'BSE IN', 'ITC IN', 'VEDL IN', 'BJFIN IN', 'INDIGO IN', 'COFORGE IN', 'NTPC IN', 'RECL IN', 'UTCEM IN', 'HUVR IN', 'POWF IN', 'HCLT IN', 'BOB IN', 'DLFU IN', 'APNT IN', 'WPRO IN', 'IH IN', 'TECHM IN', 'EIM IN', 'SHFL IN', 'JSTL IN', 'TTAN IN', 'PNB IN', 'TPWR IN', 'SRF IN', 'PSYS IN', 'LAURUS IN', 'BJAUT IN', 'CIFC IN', 'FB IN', 'SUNP IN', 'ONGC IN', 'INDUSTOW IN', 'BHEL IN', 'LPC IN', 'VOLT IN', 'AUBANK IN', 'HPCL IN', 'CBK IN', 'IDFCFB IN', 'COAL IN', 'JIOFIN IN', 'HMCL IN', 'TVSL IN', 'PWGR IN', 'UPLL IN', 'ABB IN', 'JSP IN', 'SBICARD IN', 'GRASIM IN', 'NMDC IN', 'BHFC IN', 'AL IN', 'HDFCLIFE IN', 'JUBI IN', 'GPL IN', 'DRRD IN', 'BPCL IN', 'DIVI IN', 'DMART IN', 'MCX IN', 'NACL IN', 'CDSL IN', 'HAVL IN', 'BIOS IN', 'APHS IN', 'SAIL IN', 'NEST IN', 'INFOE IN', 'GAIL IN', 'MOTHERSO IN', 'ARBP IN', 'SIEM IN', 'BRIT IN', 'OBER IN', 'HDFCAMC IN', 'SBILIFE IN', 'LTIM IN', 'ANGELONE IN', 'VBL IN', 'MPHL IN', 'CIPLA IN', 'KKC IN', 'IOCL IN', 'ACEM IN', 'TATACONS IN', 'ZYDUSLIF IN', 'IGL IN', 'MGFL IN', 'CHMB IN', 'POLICYBZ IN', 'CCRI IN', 'LICHF IN', 'JSW IN', 'IREDA IN', 'UNITDSPR IN', 'GCPL IN', 'MUTH IN', 'PAG IN', 'IRCTC IN', 'BANDHAN IN', 'PIEL IN', 'LTF IN', 'CLGT IN', 'DABUR IN', 'OFSS IN', 'ABCAP IN', 'EXID IN', 'MRCO IN', 'ABFRL IN', 'RBK IN', 'LODHA IN', 'HUDCO IN', 'SRCM IN', 'MAHGL IN', 'MAXHEALT IN', 'TTCH IN', 'MMFS IN', 'IEX IN', 'MAXF IN', 'PIDI IN', 'CAMS IN', 'PI IN', 'ACC IN', 'ICICIGI IN', 'UNBK IN', 'TITAGARH IN', 'ARTO IN', 'NYKAA IN', 'PLNG IN', 'CGPOWER IN', 'KEII IN', 'TCOM IN', 'TELX IN', 'DALBHARA IN', 'APAT IN', 'IPRU IN', 'GRAN IN', 'KPITTECH IN', 'BIL IN', 'TATATECH IN', 'HCP IN', 'SOIL IN', 'BOS IN', 'ASTRA IN', 'CYL IN', 'BSOFT IN']
#[allstocks.extend(i) for i in mydata.indexcomponents.values()]
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



print(1)
# duplicates = list(mydata.Close.columns[mydata.Close.columns.duplicated()])
# for it in duplicates:
#     colNums = np.argwhere(mydata.Close.columns.isin([it]).ravel())
#     colNums = [ik[0] for ik in colNums]
#     mydata.Close.iloc[:, colNums[0]] = np.where(mydata.Close.iloc[:, colNums[0]].isna(), mydata.Close.iloc[:, colNums[1]], mydata.Close.iloc[:, colNums[0]])#, index = mydata.Close.index)
#     mydata.Close = mydata.Close.drop(mydata.Close.columns[colNums[1:]], axis=1)
#mydata.Close = mydata.Close.T.drop_duplicates().T#.loc[:, list(set(mydata.Close.columns))]

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

mydata.Close = mydata.Close.loc[:, ~mydata.Close.columns.duplicated()]
mydata.Open = mydata.Open.loc[:, ~mydata.Open.columns.duplicated()]
mydata.High = mydata.High.loc[:, ~mydata.High.columns.duplicated()]
mydata.Low = mydata.Low.loc[:, ~mydata.Low.columns.duplicated()]

mydata.indexpriceDaily = mydata.indexpriceDaily.sort_index()
mydata.indexprice = mydata.indexpriceDaily
mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]#.resample(resamplFreq, convention='end').last()
# mydata.indexprice = pandas.concat([mydata.indexpriceDaily, mydata.indexprice], axis = 0)
# mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]
# mydata.indexprice = mydata.indexprice[~mydata.indexprice.index.duplicated(keep = 'last')]
# mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]


print(2)
mydata.RSI14 = MyTechnicalLib.GetRSI_talib(mydata.Close, 14)# dependency on talib
mydata.RSI5 =  MyTechnicalLib.GetRSI_talib(mydata.Close, 5)
mydata.RSI2 =  MyTechnicalLib.GetRSI_talib(mydata.Close, 2)

mydata.SMA10 = MyTechnicalLib.MovingAverage(mydata.Close,10)
mydata.SMA20 = MyTechnicalLib.MovingAverage(mydata.Close,20)
mydata.SMA21 = MyTechnicalLib.MovingAverage(mydata.Close,21)
mydata.SMA24 = MyTechnicalLib.MovingAverage(mydata.Close,24)
mydata.SMA50 = MyTechnicalLib.MovingAverage(mydata.Close,50)
mydata.SMA100 = MyTechnicalLib.MovingAverage(mydata.Close,100)


mydata.EMA21 = MyTechnicalLib.GetEMA(mydata.Close, 21 , ShiftDays=0)
mydata.WMA20 = MyTechnicalLib.GetWMA(mydata.Close, 20)
mydata.MACD, mydata.MACDSignal = MyTechnicalLib.MACD(mydata.Close)

print(3)
mydata.ATR24 = MyTechnicalLib.GetATR(mydata, 24)

mydata.ROC27D = MyTechnicalLib.GetROC(mydata, 27)
mydata.ROCMA18D = MyTechnicalLib.MovingAverage(mydata.ROC27D, 18)

mydata.NVI, mydata.PVI = MyTechnicalLib.VortexOscillator(mydata, 14)
mydata.LS, mydata.LR = MyTechnicalLib.RegressionCrossOverSignal(mydata.Close, LSDays = 5, LRDays = 50)

mydata.IndexInclusionFactorBSE200 = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')
#mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE100 INDEX')# 'BSE100 INDEX'
#mydata.IndexInclusionFactorBSE100 = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')
mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')

print(4)
basePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
f = open(basePath+'STFDMOM_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
#f = open(basePath+'STFDMOM_All'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
#f = open('Z:/Pickles/Hist_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
#f = open('Z:/Pickles/Live_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
