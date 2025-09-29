# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 11:02:40 2022

@author: Viren@InCred
Data Builder for Simple Momentum Long Short Trading, gets the data for all Futs Tickers
"""

import sqlite3
import pandas
from GetData import GetConn, QueryFutTickers
from GetData import *
import MyTechnicalLib
import time
import pickle

from intraday_db_postgres import DataBaseConnect


import warnings
warnings.filterwarnings("ignore")

engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"10.147.0.69"}:{"5432"}/{"data"}'
db_obj = DataBaseConnect(engine_url)
db_obj.connect()


priceconn = GetConn('PriceData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2022, 1, 1)#datetime.date(2022, 1, 1)

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE200 INDEX')
#mydata.indexpriceDaily = GetDataForIndicesFromBloomDB(priceconn, ['BSE200 INDEX'], 'PX_LAST')
#allstocks = ['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'ADE IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN', 'TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'ADSEZ IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN', 'INMART IN', 'IHFL IN', 'ICEM IN', 'CLGT IN', 'PLNG IN', 'UBBL IN', 'CANF IN', 'ARTO IN', 'CROMPTON IN', 'CUBK IN', 'ALKEM IN', 'DLPL IN', 'PVRINOX IN', 'TRP IN', 'INDA IN', 'IDEA IN', 'SUNTV IN', 'CHMB IN', 'MCX IN', 'METROHL IN', 'HCP IN', 'JKCE IN', 'SYNG IN', 'GRAN IN', 'RINDL IN', 'CRIN IN', 'IPCA IN', 'BOOT IN', 'ATLP IN', 'OFSS IN']
allstocks = ['HDFCB IN', 'RELIANCE IN', 'ICICIBC IN', 'BAF IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BHARTI IN', 'TTMT IN', 'TCS IN', 'MM IN', 'LT IN', 'HNAL IN', 'TRENT IN', 'BHE IN', 'ZOMATO IN', 'TATA IN', 'MSIL IN', 'DIXON IN', 'HNDL IN', 'BSE IN', 'ITC IN', 'VEDL IN', 'BJFIN IN', 'INDIGO IN', 'COFORGE IN', 'NTPC IN', 'RECL IN', 'UTCEM IN', 'HUVR IN', 'POWF IN', 'HCLT IN', 'BOB IN', 'DLFU IN', 'APNT IN', 'WPRO IN', 'IH IN', 'TECHM IN', 'EIM IN', 'SHFL IN', 'JSTL IN', 'TTAN IN', 'PNB IN', 'TPWR IN', 'SRF IN', 'PSYS IN', 'LAURUS IN', 'BJAUT IN', 'CIFC IN', 'FB IN', 'SUNP IN', 'ONGC IN', 'INDUSTOW IN', 'BHEL IN', 'LPC IN', 'VOLT IN', 'AUBANK IN', 'HPCL IN', 'CBK IN', 'IDFCFB IN', 'COAL IN', 'JIOFIN IN', 'HMCL IN', 'TVSL IN', 'PWGR IN', 'UPLL IN', 'ABB IN', 'JSP IN', 'SBICARD IN', 'GRASIM IN', 'NMDC IN', 'BHFC IN', 'AL IN', 'HDFCLIFE IN', 'JUBI IN', 'GPL IN', 'DRRD IN', 'BPCL IN', 'DIVI IN', 'DMART IN', 'MCX IN', 'NACL IN', 'CDSL IN', 'HAVL IN', 'BIOS IN', 'APHS IN', 'SAIL IN', 'NEST IN', 'INFOE IN', 'GAIL IN', 'MOTHERSO IN', 'ARBP IN', 'SIEM IN', 'BRIT IN', 'OBER IN', 'HDFCAMC IN', 'SBILIFE IN', 'LTIM IN', 'ANGELONE IN', 'VBL IN', 'MPHL IN', 'CIPLA IN', 'KKC IN', 'IOCL IN', 'ACEM IN', 'TATACONS IN', 'ZYDUSLIF IN', 'IGL IN', 'MGFL IN', 'CHMB IN', 'POLICYBZ IN', 'CCRI IN', 'LICHF IN', 'JSW IN', 'IREDA IN', 'UNITDSPR IN', 'GCPL IN', 'MUTH IN', 'PAG IN', 'IRCTC IN', 'BANDHAN IN', 'PIEL IN', 'LTF IN', 'CLGT IN', 'DABUR IN', 'OFSS IN', 'ABCAP IN', 'EXID IN', 'MRCO IN', 'ABFRL IN', 'RBK IN', 'LODHA IN', 'HUDCO IN', 'SRCM IN', 'MAHGL IN', 'MAXHEALT IN', 'TTCH IN', 'MMFS IN', 'IEX IN', 'MAXF IN', 'PIDI IN', 'CAMS IN', 'PI IN', 'ACC IN', 'ICICIGI IN', 'UNBK IN', 'TITAGARH IN', 'ARTO IN', 'NYKAA IN', 'PLNG IN', 'CGPOWER IN', 'KEII IN', 'TCOM IN', 'TELX IN', 'DALBHARA IN', 'APAT IN', 'IPRU IN', 'GRAN IN', 'KPITTECH IN', 'BIL IN', 'TATATECH IN', 'HCP IN', 'SOIL IN', 'BOS IN', 'ASTRA IN', 'CYL IN', 'BSOFT IN']

#allstocks = []
kk = [allstocks.extend(i) for i in mydata.indexcomponents.values()]
allstocks = set(allstocks)
mydata.allstocksinbloom = allstocks


futDict = QueryFutTickers(priceconn, allstocks)
futDict['NZ1 INDEX'] = 'NIFTY INDEX'
futDict['AF1 INDEX'] = 'NSEBANK INDEX'
futDictInv = dict([(futDict[k], k) for k in futDict.keys()])

mydata.CloseDaily = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_LAST', DataStartDate)
mydata.CloseDaily.columns = [futDict[i] for i in mydata.CloseDaily.columns]
mydata.CloseDaily = mydata.CloseDaily.sort_index()
mydata.Close = mydata.CloseDaily
mydata.Close['Cash'] = [1.00]*mydata.Close.shape[0]
mydata.Close = mydata.Close#.ffill(limit = 2)

mydata.High = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_HIGH', DataStartDate)
mydata.High.columns = [futDict[i] for i in mydata.High.columns]
mydata.High = mydata.High.sort_index()
mydata.High['Cash'] = [1.00]*mydata.High.shape[0]
mydata.High = mydata.High.ffill(limit = 2)

mydata.Low = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_LOW', DataStartDate)
mydata.Low.columns = [futDict[i] for i in mydata.Low.columns]
mydata.Low = mydata.Low.sort_index()
mydata.Low['Cash'] = [1.00]*mydata.Low.shape[0]
mydata.Low = mydata.Low.ffill(limit = 2)

#mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')

mydata.ExpiryDates = db_obj.getExpiryDates(expiryType = 'm',  symbol = 'BANKNIFTY')
removeDates = ['2014-02-27', '2023-03-30', '2024-03-28', '2024-04-25', '2024-06-27', '2024-09-26', '2024-12-24', '2024-12-26', '2025-01-29']
removeDates = [datetime.datetime.strptime(it, '%Y-%m-%d').date() for it in removeDates]
#mydata.ExpiryDates = [datetime.datetime.strptime(it, '%Y-%m-%d') for it in mydata.ExpiryDates]
mydata.ExpiryDates = [iDate for iDate in mydata.ExpiryDates if iDate >= DataStartDate and iDate not in removeDates]
mydata.ExpiryDates = [it for it in mydata.ExpiryDates if (it >= DataStartDate and it <= datetime.datetime.today().date())]

mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST', DataStartDate)
mydata.IndexInclusionFactor = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')

SpecialExpiryDates = {'2023-06-29': '2023-06-28'}#
for key in SpecialExpiryDates.keys():
    if key in mydata.ExpiryDates:
        mydata.ExpiryDates[mydata.ExpiryDates.index(key)] = SpecialExpiryDates[key]
picklePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
f = open(picklePath+'STFM_Stocks_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
priceconn.close()