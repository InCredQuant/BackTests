# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 17:37:19 2024

@author: Viren
"""

#import sqlite3
import pandas as pd
import datetime as dt
#from GetData import GetConn#, QueryFutTickers
from GetData import GetComponentsForIndexForDateRange, GetDataForTickersFromBloomDB,  GetDataForIndicesFromBloomDB, GetConn, MyBacktestData

#import MyTechnicalLib
#import time
import pickle
import warnings
warnings.filterwarnings("ignore")
priceconn = GetConn('PriceData')

mydata = MyBacktestData()
DataStartDate = dt.date(1999, 12, 25)
EndDate = dt.date(2024, 12, 31)

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, EndDate, 'BSE200 INDEX')# 'BSE100 INDEX'
allstocks = [] 
kk = [allstocks.extend(i) for i in mydata.indexcomponents.values()]
allstocks = set(allstocks)    

mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, ['BSE200 INDEX'], 'PX_LAST')
mydata.indexprice = mydata.indexprice.sort_index()
mydata.indexprice = mydata.indexprice.dropna()
mydata.indexprice = mydata.indexprice.loc[:EndDate, :]

mydata.Close = GetDataForTickersFromBloomDB(priceconn, allstocks, 'PX_LAST', DataStartDate)
mydata.Close = mydata.Close.sort_index()
mydata.Close = mydata.Close.loc[mydata.Close.index.isin(mydata.indexprice.index), :]

mydata.indexprice = mydata.indexprice.loc[mydata.indexprice.index.isin(mydata.Close.index), :]
print(len(mydata.Close.columns), ',Got from Total: ', len(allstocks))## 512/544

mydata.MarketCap = GetDataForTickersFromBloomDB(priceconn, allstocks, 'MCAP', DataStartDate)# market cap is in Millions
mydata.MarketCap = mydata.MarketCap.sort_index()
mydata.MarketCap = mydata.MarketCap.loc[mydata.MarketCap.index.isin(mydata.indexprice.index), :]


mydata.Assets = pd.read_excel('G:/Shared drives/QuantFunds/Liquid1/LiveModels/HDFCLiqFund.xlsx', sheet_name = 'LiqFund', index_col = 0, header = 0)
mydata.Assets = mydata.Assets.loc[mydata.Assets.index.isin(mydata.Close.index)]

AssetIndices = ['NIFTY INDEX', 'NSEMCAP INDEX', 'NSESMCP INDEX', 'NSEAUTO INDEX', 'NSEBANK INDEX', 'NSEFMCG INDEX', 'NSEIT INDEX', 'NSEMET INDEX', 'NSEPHRM INDEX', 'NSEINFR INDEX']
tempdata = GetDataForIndicesFromBloomDB(priceconn, AssetIndices, 'PX_LAST')
tempdata = tempdata.sort_index()
tempdata = tempdata.loc[tempdata.index.isin(mydata.indexprice.index), :]

mydata.Assets = pd.concat([mydata.Assets, tempdata], axis= 1)
mydata.Assets = mydata.Assets.interpolate(method = 'linear')
mydata.Assets.index = pd.to_datetime(mydata.Assets.index).normalize()
mydata.Assets = mydata.Assets.sort_index()


fileName = 'Data.xlsx'
mydata.PE = pd.read_excel(fileName, sheet_name= 'PE', index_col=0, header = 1)

mydata.PB = pd.read_excel(fileName, sheet_name= 'PB', index_col=0, header = 1)
mydata.ROE = pd.read_excel(fileName, sheet_name= 'ROE', index_col=0, header = 1)
mydata.ROA = pd.read_excel(fileName, sheet_name= 'ROA', index_col=0, header = 1)

hedgeData = pd.read_excel(fileName, sheet_name= 'Hedge', index_col=0, header = 0)
hedgeData.index = pd.to_datetime(hedgeData.index).normalize()

#hedgeData.columns = ['Hedge']
hedgeData = hedgeData.sort_index()

targetPrices = pd.read_excel(fileName, sheet_name= 'Target', index_col=0, header = 0)
targetPrices.index = pd.to_datetime(targetPrices.index).normalize()
mydata.TargetPrice = targetPrices.sort_index()

mydata.Assets = pd.concat([mydata.Assets, hedgeData], axis = 1)
mydata.Assets = mydata.Assets.loc[mydata.Assets.index.isin(mydata.Close.index)]

f = open('ProjectData'+'.pkl', 'wb')#dt.datetime.today().date().strftime('%Y%m%d')

pickle.dump(mydata, f)
f.close()
priceconn.close()


# Copy file to Server
#scp ProjectData.pkl incredquant@192.168.44.4:JupyterNotebook/FinalProject/