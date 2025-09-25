# -*- coding: utf-8 -*-
"""
Created on Mon May 26 13:48:09 2025

@author: Viren@Incred
Builds the data for Stocks Momentum sTOCKS Trading Strategy for Long Only Models
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

# indices are: BSE100, BSE200, BSE500, NSESMCAP, NSESMCPW
# Data Points: Open, High, Low, Close, 


mydata = MyBacktestData()
DataStartDate = datetime.date(2014, 1, 1)#datetime.date(2002, 12, 25)#datetime.date(2022, 1, 1)#datetime.date(2022, 1, 1)#

indices = ['BSE100 INDEX', 'BSE200 INDEX']#, 'BSE500 INDEX']#, 'BSEALL INDEX']#['NIFTY INDEX', 'BSE100 INDEX', 'BSE200 INDEX', 'BSE500 INDEX', 'NSEMCAP INDEX', 'NSESMCP INDEX','SPBSAIP INDEX']
mydata.IndexCompDict = {}
allstocks = []

for ind in indices:
    indexComp = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), ind)    
    mydata.IndexCompDict[ind] = indexComp    
    kk = [allstocks.extend(i) for i in indexComp.values()]

allstocks = set(allstocks)    
mydata.allstocksinbloom = allstocks


mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, indices, 'PX_LAST', FromDate = DataStartDate, ToDate = datetime.date(2025, 4, 30))
mydata.indexprice = mydata.indexprice.sort_index()
mydata.indexprice = mydata.indexprice.resample('m').last()

mydata.CloseDaily = GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_LAST', fromDate = DataStartDate)
mydata.CloseDaily = mydata.CloseDaily.sort_index()
mydata.Close = mydata.CloseDaily.resample('m').last().loc[mydata.indexprice.index]

#mydata.High = GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_HIGH', DataStartDate)
#mydata.High = mydata.High.sort_index()
#mydata.High = mydata.High.ffill(limit = 2)

#mydata.Low = GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_LOW', DataStartDate)
#mydata.Low = mydata.Low.sort_index()

#mydata.TurnOver = GetDataForTickersFromBloomDB(priceconn, allstocks, 'TURNOVER', DataStartDate)
#mydata.TurnOver = mydata.TurnOver.sort_index()

mydata.MCap = GetDataForTickersFromBloomDB(priceconn, allstocks, 'MCAP', DataStartDate)
mydata.MCap = mydata.MCap.sort_index()
#mydata.Low = mydata.Low.ffill(limit = 2)

#mydata.Close.Cash = (1 + 0.065/252).cumprod()
#mydata.GICS, mydata.InvGICS =  GetGICS(priceconn, allstocks, sectorName = 'GICS_SECTOR_NAME')

scoreData = pandas.read_csv('G:/Shared drives/BackTests/BackTestsResults/ValQuantData/ModelRankingData.csv', sep = ',',  header = 0)
scoreData.index = pandas.to_datetime(scoreData.EvaluationDate)
gg = scoreData.groupby('NSECode')
temp_dfs = [pandas.DataFrame(gg.get_group(grp)['Score'].rename(grp)) for grp in gg.groups.keys()]
mydata.ValQuantScore = pandas.concat(temp_dfs, axis=1)
mydata.ValQuantScore = mydata.ValQuantScore.sort_index()

futDict = QueryScripMaster(priceconn, mydata.ValQuantScore.columns, FromWhat = 'NSE', ToWhat = 'Ticker')
futDictInv = dict([(futDict[k], k) for k in futDict.keys()])

ReqNames = [it for it in mydata.ValQuantScore.columns if it in futDict.keys()]
mydata.ValQuantScore = mydata.ValQuantScore.loc[:, ReqNames]
mydata.ValQuantScore.loc[pandas.datetime(2025, 3, 31)] = numpy.nan

mydata.ValQuantScore.columns = [futDict[it] + ' IN' for it in mydata.ValQuantScore.columns]
mydata.ValQuantScore = mydata.ValQuantScore.resample('m').last().ffill(limit = 2)
mydata.ValQuantScore = mydata.ValQuantScore.shift(1)

#mydata.Close = pandas.concat([mydata.Close, liqfund], axis = 1)
#mydata.Close['Liquid'] = mydata.Close['Liquid'].ffill()

#mydata.High['Liquid'] = mydata.Close['Liquid']
#mydata.Low['Liquid'] = mydata.Close['Liquid']

picklePath = 'G:/Shared drives/QuantFunds/EquityPlus/DataPickles/'
f = open(picklePath+'ValQuantModel'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
priceconn.close()