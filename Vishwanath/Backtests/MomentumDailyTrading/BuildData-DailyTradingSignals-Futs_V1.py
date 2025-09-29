# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 10:51:46 2023

@author: Viren@InCred
Data Builder, for LiveModel of Daily Momentum.
It deals different expiries for futs data. Takes =1 as current Expiry and =2 as next expiry
On the expiry day we have to do buying in =2, which will become =1 on next day, and selling is always on =1.
We are dealing it with introducing, expiry dates in tickers.
"""
import datetime
import pandas
import MyTechnicalLib as mylib
import GetData
import pickle
import time
import warnings
warnings.filterwarnings("ignore")
import pdb

t1 = time.time()
priceconn = GetData.GetConn('PriceData')
analystconn = GetData.GetConn('AnalystData')

mydata = GetData.MyBacktestData()
mydata.Index = GetData.Index()# it is called to store Spots Data

DataStartDate = datetime.date(2009, 12, 25)#(2022, 12, 29)#2009
#DataStartDate = datetime.date(2020, 1, 1)

mydata.Index.indexcomponents = GetData.GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE200 INDEX')# 'BSE100 INDEX'
allstocks = []#['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'ADE IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN', 'HDFC IN', 'TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'ADSEZ IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'NFIL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'GUJGA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN', 'INMART IN', 'IHFL IN', 'ICEM IN', 'CLGT IN', 'PLNG IN', 'UBBL IN', 'CANF IN', 'ARTO IN', 'CROMPTON IN', 'CUBK IN', 'ALKEM IN', 'DLPL IN', 'PVRINOX IN', 'TRP IN', 'INDA IN', 'IDEA IN', 'SUNTV IN', 'CHMB IN', 'MCX IN', 'METROHL IN', 'HCP IN', 'JKCE IN', 'SYNG IN', 'GRAN IN', 'RINDL IN', 'CRIN IN', 'IPCA IN', 'BOOT IN', 'ATLP IN', 'OFSS IN']

[allstocks.extend(i) for i in mydata.Index.indexcomponents.values()]
allstocks = set(allstocks)
mydata.Index.allstocksinbloom = allstocks

basesqlquery = 'select distinct(Ticker) as Ticker from ScripM where FutTicker is not NULL;'
curs = priceconn.cursor()
curs.execute(basesqlquery)
qw = curs.fetchall()
AllList = [i[0]+' IN' for i in qw]
allstocks = list(set.union(set(allstocks), set(AllList)))

mydata.FutDict = GetData.QueryScripMaster(priceconn, allstocks)# from Eq Ticker to Futs Ticker
filtered = {k: v for k, v in mydata.FutDict.items() if v is not None}
mydata.FutDict.clear()
mydata.FutDict.update(filtered)
mydata.FutDictInv = dict([(mydata.FutDict[k], k) for k in mydata.FutDict.keys()])# from Futs Ticker to Eq Ticker

mydata.NSEDict = GetData.QueryScripMaster(priceconn, allstocks, FromWhat = 'Ticker', ToWhat = 'NSE')# from Eq Ticker to NSE Scrip
#mydata.NSEDict['NIFTY'] = 'NIFTY INDEX'
#mydata.NSEDict['NSEBANK'] = 'NSEBANK INDEX'
mydata.NSEDictInv = dict([(mydata.NSEDict[k], k) for k in mydata.NSEDict.keys()])

allstocks = list(mydata.FutDict.keys())
allstocks = [it+' IN' for it in allstocks]
mydata.Index.Close = GetData.GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_LAST', DataStartDate)
mydata.Index.Close.columns = [mydata.NSEDict[it.replace(' IN', '')] for it in mydata.Index.Close.columns]
mydata.Index.High = GetData.GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_HIGH', DataStartDate)
mydata.Index.High.columns = [mydata.NSEDict[it.replace(' IN', '')] for it in mydata.Index.High.columns]
mydata.Index.Low = GetData.GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_LOW', DataStartDate)
mydata.Index.Low.columns = [mydata.NSEDict[it.replace(' IN', '')] for it in mydata.Index.Low.columns]

mydata.ExpiryDates = GetData.QueryExpiryDates(priceconn, expirytype = 'Monthly')
mydata.ExpiryDates = [it for it in mydata.ExpiryDates if datetime.datetime.strptime(it, '%Y-%m-%d').date() >= DataStartDate]

indicesList = ['AF', 'NZ', 'FIE']
allFutsTicks = list(mydata.FutDict.values())
nearMonth = [it+'=1 IS' if it not in indicesList else it+'1 Index' for it in allFutsTicks]
nextMonth = [it+'=2 IS' if it not in indicesList else it+'2 Index' for it in allFutsTicks]
allfuts = list(set.union(set(nearMonth), set(nextMonth)))

t2 = time.time()
print('Step1 Completed!', round((t2-t1)/60, 1))

def MakeExpiryRollOverData(data: pandas.DataFrame, prevExpiry, iExp):
    expiry = datetime.datetime.strptime(iExp, '%Y-%m-%d').strftime('%d%b%y').upper()# iExp
    Series1 = data.loc[prevExpiry:prevExpiry, set.intersection(set(nextMonth), set(data.columns))]# this is expiry day price for next expiry Tickers
    Series1.columns = [mydata.NSEDict[mydata.FutDictInv[it.replace('=2 IS', '')]] for it in Series1.columns]
    
    Series2 = data.loc[prevExpiry:iExp, set.intersection(set(nearMonth), set(data.columns))].iloc[1:]
    Series2.columns = [mydata.NSEDict[mydata.FutDictInv[it.replace('=1 IS', '')]] for it in Series2.columns]
    Series = pandas.concat([Series1, Series2], axis = 0)
    Series.columns = [it+expiry+'XX0' for it in Series.columns]
    Series = Series.backfill(limit=1)
    Series = Series.loc[:, Series.isna().sum() != len(Series)]
    return Series



mydata.Index.RSI14 = mylib.GetRSI_talib(mydata.Index.Close, 14)
mydata.Index.RSI5 =  mylib.GetRSI_talib(mydata.Index.Close, 5)
mydata.Index.RSI2 =  mylib.GetRSI_talib(mydata.Index.Close, 2)

mydata.Index.SMA10 = mylib.MovingAverage(mydata.Index.Close,10)
mydata.Index.SMA20 = mylib.MovingAverage(mydata.Index.Close,20)
mydata.Index.SMA21 = mylib.MovingAverage(mydata.Index.Close,21)
mydata.Index.SMA24 = mylib.MovingAverage(mydata.Index.Close,24)
mydata.Index.SMA50 = mylib.MovingAverage(mydata.Index.Close,50)
mydata.Index.SMA100 = mylib.MovingAverage(mydata.Index.Close,100)

mydata.Index.EMA21 = mylib.GetEMA(mydata.Index.Close, 21 , ShiftDays=0)
mydata.Index.WMA20 = mylib.GetWMA(mydata.Index.Close, 20)
mydata.Index.MACD, mydata.Index.MACDSignal = mylib.MACD(mydata.Index.Close)

mydata.Index.ATR24 = mylib.GetATR(mydata.Index, 24)

mydata.Index.ROC27D = mylib.GetROC(mydata.Index, 27)
mydata.Index.ROCMA18D = mylib.MovingAverage(mydata.Index.ROC27D, 18)

mydata.Index.NVI, mydata.Index.PVI = mylib.VortexOscillator(mydata.Index, 14)
mydata.Index.LS, mydata.Index.LR = mylib.RegressionCrossOverSignal(mydata.Index.Close, LSDays = 5, LRDays = 50)

#mydata.Index.InclusionFactor = mylib.IndexComponents2(mydata.Index, freq = 'MS')

t3 = time.time()
print('Step2 Completed!', round((t3-t2)/60, 1))

OpenDaily = GetData.GetDataForFutTickersFromBloomDB(priceconn, allfuts, 'PX_OPEN', DataStartDate)
OpenDaily = OpenDaily.sort_index()

HighDaily = GetData.GetDataForFutTickersFromBloomDB(priceconn, allfuts, 'PX_HIGH', DataStartDate)
HighDaily = HighDaily.sort_index()

LowDaily = GetData.GetDataForFutTickersFromBloomDB(priceconn, allfuts, 'PX_LOW', DataStartDate)
LowDaily = LowDaily.sort_index()

CloseDaily = GetData.GetDataForFutTickersFromBloomDB(priceconn, allfuts, 'PX_LAST', DataStartDate)
CloseDaily = CloseDaily.sort_index()

mydata.Close = pandas.DataFrame()
mydata.High = pandas.DataFrame()
mydata.Low = pandas.DataFrame()
mydata.Open = pandas.DataFrame()

for iT, iExp in enumerate(mydata.ExpiryDates):
    if iT<=0:
        continue    
    prevExpiry = mydata.ExpiryDates[iT -1]    
    mydata.Open = pandas.concat([mydata.Open, MakeExpiryRollOverData(OpenDaily, prevExpiry, iExp)], axis = 1)
    mydata.High = pandas.concat([mydata.High, MakeExpiryRollOverData(HighDaily, prevExpiry, iExp)], axis = 1)
    mydata.Low = pandas.concat([mydata.Low, MakeExpiryRollOverData(LowDaily, prevExpiry, iExp)], axis = 1)
    mydata.Close = pandas.concat([mydata.Close, MakeExpiryRollOverData(CloseDaily, prevExpiry, iExp)], axis = 1)

mydata.Close['Cash'] = [1.00]*mydata.Close.shape[0]
mydata.Open['Cash'] = [1.00]*mydata.Open.shape[0]
mydata.High['Cash'] = [1.00]*mydata.High.shape[0]
mydata.Low['Cash'] = [1.00]*mydata.Low.shape[0]

mydata.indexpriceDaily = GetData.GetDataForIndicesFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST')
mydata.indexpriceDaily = mydata.indexpriceDaily.sort_index()
mydata.indexprice = mydata.indexpriceDaily
mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]

t4 = time.time()
print('Step3 Completed!', round((t4-t3)/60, 1))
#mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE100 INDEX')# 'BSE100 INDEX'
#mydata.IndexInclusionFactorBSE100 = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')

basePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
f = open(basePath+'STFDMOM_V2_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
#f = open('Z:/Pickles/Hist_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
#f = open('Z:/Pickles/Live_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()



t5 = time.time()
print('Total Time!', round((t5-t1)/60, 1))
