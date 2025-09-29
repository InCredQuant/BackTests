# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 15:23:33 2023

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
DataStartDate = datetime.date(2022, 12, 25)#datetime.date(2002, 12, 25)#datetime.date(2022, 1, 1)#datetime.date(2022, 1, 1)#

indices = ['BSE100 INDEX', 'BSE200 INDEX', 'BSE500 INDEX']#, 'BSEALL INDEX']#['NIFTY INDEX', 'BSE100 INDEX', 'BSE200 INDEX', 'BSE500 INDEX', 'NSEMCAP INDEX', 'NSESMCP INDEX','SPBSAIP INDEX']
mydata.IndexCompDict = {}
allstocks = []

for ind in indices:
    indexComp = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), ind)    
    mydata.IndexCompDict[ind] = indexComp    
    kk = [allstocks.extend(i) for i in indexComp.values()]

allstocks = set(allstocks)    
mydata.allstocksinbloom = allstocks
mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, indices, 'PX_LAST')
mydata.indexprice = mydata.indexprice.sort_index()

mydata.CloseDaily = GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_LAST', DataStartDate)
mydata.CloseDaily = mydata.CloseDaily.sort_index()
mydata.Close = mydata.CloseDaily

mydata.High = GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_HIGH', DataStartDate)
mydata.High = mydata.High.sort_index()
#mydata.High = mydata.High.ffill(limit = 2)

mydata.Low = GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_LOW', DataStartDate)
mydata.Low = mydata.Low.sort_index()

mydata.TurnOver = GetDataForTickersFromBloomDB(priceconn, allstocks, 'TURNOVER', DataStartDate)
mydata.TurnOver = mydata.TurnOver.sort_index()

mydata.MCap = GetDataForTickersFromBloomDB(priceconn, allstocks, 'MCAP', DataStartDate)
mydata.MCap = mydata.MCap.sort_index()
#mydata.Low = mydata.Low.ffill(limit = 2)

#mydata.Close.Cash = (1 + 0.065/252).cumprod()

mydata.GICS, mydata.InvGICS =  GetGICS(priceconn, allstocks, sectorName = 'GICS_SECTOR_NAME')

liqfund = pandas.read_excel('G:/Shared drives/QuantFunds/Liquid1/LiveModels/HDFCLiqFund.xlsx', sheet_name = 'LiqFund', index_col = 0, header = 0)
liqfund = liqfund.loc[liqfund.index.isin(mydata.Close.index)]

mydata.Close = pandas.concat([mydata.Close, liqfund], axis = 1)
mydata.Close['Liquid'] = mydata.Close['Liquid'].ffill()

mydata.High['Liquid'] = mydata.Close['Liquid']
mydata.Low['Liquid'] = mydata.Close['Liquid']

picklePath = 'G:/Shared drives/QuantFunds/EquityPlus/DataPickles/'
f = open(picklePath+'MomentumModels'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()
priceconn.close()