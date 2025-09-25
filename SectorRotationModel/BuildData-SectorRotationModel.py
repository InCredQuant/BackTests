# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 10:33:26 2022

@author: Viren@InCred
"""

import sqlite3
import pandas
from GetData import GetConn, QueryFutTickers
from GetData import *
import MyTechnicalLib
import time
#from FactorsDef import *
import pickle

import warnings
warnings.filterwarnings("ignore")

priceconn = GetConn('PriceData')

mydata = MyBacktestData()
DataStartDate = datetime.date(2000, 1, 1)

allIndices = ['NIFTY INDEX', 'NSEINFR INDEX', 'NSECON INDEX', 'NSEFIN INDEX', 'NSECMD INDEX', 'NSENRG INDEX', 'NSEBANK INDEX', 'NSEIT INDEX', 'NSEFMCG INDEX', 'NSEPSE INDEX', 'NSEAUTO INDEX', 'NSEMET INDEX', 'NSEPHRM INDEX', 'NSEPSBK INDEX']
allStocks = []#['NIFTY INDEX']
mydata.IndexInclusionFactor = {}
mydata.Close = pandas.DataFrame()

for ind in allIndices:
    mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), ind)
    stocksList = []
    k = [stocksList.extend(i) for i in mydata.indexcomponents.values()]
    stocksList = set(stocksList)
    mydata.CloseDaily = GetDataForTickersFromBloomDB(priceconn, stocksList, 'PX_LAST', DataStartDate)
    mydata.CloseDaily = mydata.CloseDaily.sort_index()
    mydata.IndexInclusionFactor[ind] = MyTechnicalLib.IndexComponents(mydata, freq = 'MS')
    allStocks.extend(stocksList)
    #mydata.Close = pandas.concat([mydata.Close, mydata.CloseDaily], axis = 1)

mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST', DataStartDate)
mydata.indexprice = mydata.indexprice.sort_index()
mydata.indexprice = mydata.indexprice.fillna(method = 'ffill', limit = 2)

futDict = QueryFutTickers(priceconn, allStocks)
futDict['NZ1 INDEX'] = 'NIFTY INDEX'
futDictInv = dict([(futDict[k], k) for k in futDict.keys()])

mydata.Close = GetDataForFutTickersFromBloomDB(priceconn, futDict.keys(), 'PX_LAST', DataStartDate)
mydata.Close.columns = [futDict[i] for i in mydata.Close.columns]
#mydata.Close = GetDataForTickersFromBloomDB(priceconn, allStocks, 'PX_LAST', DataStartDate)
mydata.Close = mydata.Close.sort_index()
#mydata.Close = pandas.concat([mydata.Close, mydata.indexprice], axis = 1)
mydata.Close = mydata.Close.fillna(method = 'ffill', limit = 2)

mydata.CloseIndices = GetDataForIndicesFromBloomDB(priceconn, allIndices, 'PX_LAST', DataStartDate)
mydata.CloseIndices = mydata.CloseIndices.sort_index()
mydata.CloseIndices = mydata.CloseIndices.fillna(method = 'ffill', limit = 2)
mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Monthly')

f = open('Z:/Pickles/SectorRotation_Data'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
pickle.dump(mydata, f)
f.close()