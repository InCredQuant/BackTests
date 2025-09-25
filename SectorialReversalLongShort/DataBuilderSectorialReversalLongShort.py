# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 14:20:14 2022

@author: Viren@InCred
Data Builder for Sectorial Reversal Long Short
"""

import sqlite3
import pandas
from GetData import GetConn, QueryFutTickers
from GetData import *
import MyTechnicalLib
import time
import pickle

import warnings
warnings.filterwarnings("ignore")

priceconn = GetConn('PriceData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2020, 12, 30)#datetime.date(2020, 1, 1)

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'BSE200 INDEX')
#mydata.indexpriceDaily = GetDataForIndicesFromBloomDB(priceconn, ['BSE200 INDEX'], 'PX_LAST')
allstocks = ['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'ADE IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN', 'TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'ADSEZ IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'NFIL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'GUJGA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN', 'INMART IN', 'IHFL IN', 'ICEM IN', 'CLGT IN', 'PLNG IN', 'UBBL IN', 'CANF IN', 'ARTO IN', 'CROMPTON IN', 'CUBK IN', 'ALKEM IN', 'DLPL IN', 'PVRINOX IN', 'TRP IN', 'INDA IN', 'IDEA IN', 'SUNTV IN', 'CHMB IN', 'MCX IN', 'METROHL IN', 'HCP IN', 'JKCE IN', 'SYNG IN', 'GRAN IN', 'RINDL IN', 'CRIN IN', 'IPCA IN', 'BOOT IN', 'ATLP IN', 'OFSS IN']

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
mydata.Close = mydata.Close.ffill(limit = 2)

mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')

mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST', DataStartDate)
mydata.IndexInclusionFactor = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')

mydata.GICS, mydata.InvGICS =  GetGICS(priceconn, allstocks, sectorName = 'GICS_SECTOR_NAME')

SpecialExpiryDates = {'2023-06-29': '2023-06-28'}#
for key in SpecialExpiryDates.keys():
    if key in mydata.ExpiryDates:
        mydata.ExpiryDates[mydata.ExpiryDates.index(key)] = SpecialExpiryDates[key]

picklePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
f = open(picklePath+'STFMSEC_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
