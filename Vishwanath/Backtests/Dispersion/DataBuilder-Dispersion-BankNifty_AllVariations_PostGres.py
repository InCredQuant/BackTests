# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 10:59:07 2023

@author: Viren@InCred
Usage- Data Builder for Disperison backTest/ Live Model
# Take the position on previous Expiry, if Index Moves by more than certain Percentage then 
1. Exit Full Straddle and take new positions
2. Shift Only the Index Strikes to the new levels
3. Take IV spread based positions

4 Strategies
1. Simple Expiry to Expiry, straddle for Stocks and 1.5% away Bank Nifty Index
2. Take Position at 3 weeks to expiry, straddle for Stocks and 1.5% away Bank Nifty Index
3. Strangle 3 Weeks to Expiry, Strange for Stocks and 1.5% away bank Nifty Index // 1 Strike Away strangle
4. Shift Entire Straddle at 3% bank NiftyMove

"""

import datetime
#import sqlite3
#import pandas as pd
from GetData import GetConn, QueryFutTickers
from GetData import *
#import MyTechnicalLib
import time
#from FactorsDef import *
import pickle
#import math
#import re
import os


#import psycopg2
from intraday_db_postgres import DataBaseConnect


import warnings
warnings.filterwarnings("ignore")

INDEXNAME = 'BANKNIFTY'
strategyNum = 4

strategy_params = {1 : [6, 0, 0, 0.0, 0.015, False], 2 : [6, 3, 0, 0.0, 0.015, False], 3 : [6, 3, 1, 0.0, 0.015, False], 4 : [6, 0, 0, 0.03, 0.015, False]}
strategy_names = {1: 'ExpiryToExpiry', 2: '3WksToExpiry', 3: 'Strangel3WksToExpiry', 4: 'ShiftStraddle3pctBNFMove'}

StocksCount = strategy_params[strategyNum][0]
NumWeekAway = strategy_params[strategyNum][1]
StrikeAway = strategy_params[strategyNum][2]
IndexMoveLimit = strategy_params[strategyNum][3]
IndexStrikeAway = strategy_params[strategyNum][4]
onlyIndexMove = strategy_params[strategyNum][5]
'''
StocksCount = 6
NumWeekAway = 0
StrikeAway = 0
IndexMoveLimit = 0.03#.03#0.03 # if it is 0, then no shifting in the index or over all positions// For shifting Change it to Non Zero
IndexStrikeAway = 0.015 # by Default we are taking 1.5% Away Strikes for Bank Nifty
onlyIndexMove = False # True/False -> True means Only index Strangle position will move/ False - All the Positions will move
# if Index Movelimit is 0 -> then there will be no movement, it will works as simple Dispersion Model
'''
engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'
db_obj = DataBaseConnect(engine_url)
db_obj.connect()

t1 = time.time()
priceconn = GetConn('PriceData', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
#analystconn = GetConn('AnalystData')
#bhavconn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')

mydata = MyBacktestData()
mydata.Index = Index()

DataStartDate = datetime.date(2023, 12, 27)#(2011, 11, 25)#datetime.date(2014, 4, 24)
DataEndDate = datetime.date(2025, 1, 29)# or latest date

mydata.ExpiryDatesIndex = db_obj.getExpiryDates(expiryType = 'm',  symbol = 'BANKNIFTY')
mydata.ExpiryDates = db_obj.getExpiryDates(expiryType = 'm',  instrument = 'OPTSTK', symbol = '')

removeDates = ['2014-02-27', '2023-03-30', '2024-03-28', '2024-04-25', '2024-06-27', '2024-09-26', '2024-12-26']
removeDates = [datetime.datetime.strptime(it, '%Y-%m-%d').date() for it in removeDates]
#mydata.ExpiryDates = [datetime.datetime.strptime(it, '%Y-%m-%d') for it in mydata.ExpiryDates]
#mydata.ExpiryDates = [iDate for iDate in mydata.ExpiryDates if iDate >= DataStartDate and iDate not in removeDates]
mydata.ExpiryDatesIndex = [iDate for iDate in mydata.ExpiryDatesIndex if iDate >= DataStartDate and iDate not in removeDates]

#mydata.ExpiryDates = list(QueryExpiry([INDEXNAME], DataStartDate, DataEndDate, expirytype = "WEEKLY").DATE)
#mydata.ExpiryTimes = [datetime.datetime.strptime(it, '%Y-%m-%d') for it in mydata.ExpiryDates]#+datetime.timedelta(hours = 15, minutes=31)
mydata.ExpiryTimes = [it for it in mydata.ExpiryDates if (it >= DataStartDate and it <= DataEndDate)]

mydata.indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), 'NSEBANK INDEX')# 'BSE100 INDEX'
allstocks = []
ok = [allstocks.extend(i) for i in mydata.indexcomponents.values()]
mydata.allStocksinBloom = list(set(allstocks))
mydata.DictBloomNSE = QueryScripMaster(priceconn, mydata.allStocksinBloom, 'Ticker', 'NSE')
mydata.DictBloomNSE = {key+" IN":value for (key, value) in mydata.DictBloomNSE.items()}
mydata.allStocksinNSE = [mydata.DictBloomNSE[it] for it in mydata.allStocksinBloom]
mydata.allStocksinNSE.sort()
mydata.allStocksinNSE.append(INDEXNAME)

#mydata.indexprice = GetDataForFutTickersFromBloomDB(priceconn, ['NZ1 INDEX'], 'PX_LAST', DataStartDate)
mydata.indexprice = GetDataForIndicesFromBloomDB(priceconn, ['NSEBANK Index'], 'PX_LAST', DataStartDate).dropna()

mydata.Index.MarketCap = GetDataForTickersFromBloomDB(priceconn, mydata.allStocksinBloom, 'MCAP', DataStartDate)
mydata.Index.MarketCap.columns = [mydata.DictBloomNSE[it] for it in mydata.Index.MarketCap.columns]
indexData = GetDataForIndicesFromBloomDB(priceconn, ['NSEBANK Index'], 'MCAP', DataStartDate)
mydata.Index.MarketCap = pandas.concat([mydata.Index.MarketCap, indexData], axis = 1)

StocksExpiry = {'2024-03-27' : '2024-03-28', '2024-04-24': '2024-04-25', '2024-05-29' : '2024-05-30', '2024-06-26' : '2024-06-27','2024-07-31' : '2024-07-25', '2024-08-28' : '2024-08-29'}
StocksExpiry = {datetime.datetime.strptime(k, '%Y-%m-%d').date().strftime('%d%b%y').upper(): datetime.datetime.strptime(v, '%Y-%m-%d').date().strftime('%d%b%y').upper() for k, v in StocksExpiry.items()}
print('Stage-1 Completed')
mydata.Tickers = []
mydata.TradingDates = {}
ExpiryCorrectionDict = {'23APR14' : '24APR14'}

import itertools

for ind, item in enumerate(mydata.ExpiryTimes[:-1]):
    iStart = item#.date()# -datetime.timedelta(days = 1)
    if item == datetime.date(2023, 6, 29):
        iStart = datetime.date(2023, 6, 28)
    iEnd = mydata.ExpiryTimes[ind+1]#.date()
    expiryDate = iEnd.strftime('%d%b%y').upper()
    indExpiryDate = [it for it in mydata.ExpiryDatesIndex if it <= iEnd][-1]
    indExpiryDate = indExpiryDate.strftime('%d%b%y').upper()
      
    indexDateRange = [i for i in mydata.indexcomponents.keys() if i.date() <= iStart]    
    compDate = list(mydata.indexcomponents.keys())[0] if len(indexDateRange) ==0 else indexDateRange[-1]
    
    nseComponents = mydata.indexcomponents[compDate]
    nseComponents = [mydata.DictBloomNSE[i] for i in nseComponents]
    tempMCap = mydata.Index.MarketCap.loc[:iStart].iloc[-1]
    tempMCap = tempMCap.loc[tempMCap.index.isin(nseComponents)]
    tempMCap = tempMCap.sort_values().iloc[-StocksCount:]
    nseComponents = list(set.intersection(set(nseComponents), set(tempMCap.index))) 
    nseComponents.append(INDEXNAME)
    
    if expiryDate in ['23APR14']:
        expiryDate = ExpiryCorrectionDict[expiryDate]
    
    stockFutsPrice = db_obj.GetNSEBhavCopyFutsData(secNames = nseComponents, fieldName = 'Close', expiry = expiryDate, fromDate = iStart - datetime.timedelta(days = 1), toDate = iEnd)
    if indExpiryDate != expiryDate:
        if INDEXNAME in stockFutsPrice.columns:
            del stockFutsPrice[INDEXNAME]
        indexFutsPrice = db_obj.GetNSEBhavCopyFutsData(secNames = nseComponents, fieldName = 'Close', expiry = indExpiryDate, fromDate = iStart - datetime.timedelta(days = 1), toDate = iEnd)
        stockFutsPrice = pandas.concat([stockFutsPrice, indexFutsPrice], axis = 1)
    ExiryShift = expiryDate in list(StocksExpiry.keys())# or indExpiryDate != expiryDate
    if ExiryShift:
        nseComponents.remove(INDEXNAME)
        tempFutsPrice = db_obj.GetNSEBhavCopyFutsData(secNames = nseComponents, fieldName = 'Close', expiry = StocksExpiry[expiryDate], fromDate = iStart - datetime.timedelta(days = 1), toDate = iEnd)

        stockFutsPrice = pandas.concat([stockFutsPrice, tempFutsPrice], axis = 1)
    #stockFutsPrice = GetNSEBhavCopyFutsData(bhavconn, secNames = nseComponents, fieldName = 'Close', expiry = expiryDate, fromDate = iStart.date() - datetime.timedelta(days = 1), toDate = iEnd.date())
    try:
        mydata.Index.Close = pandas.merge(mydata.Index.Close, stockFutsPrice, how = 'outer')#pandas.concat([mydata.Index.Close, stockFutsPrice], axis = 0)
    except:
        mydata.Index.Close = stockFutsPrice
    
    #tickersList = stockFutsPrice.columns
    #StrikesDiffDict = GetNSEBhavCopyStrikePointsDiff(conn = bhavconn, secNames = stockFutsPrice.columns, expiry = iEnd.strftime('%d%b%y').upper())
    StrikesDF = db_obj.GetNSEBhavCopyStrikePointsDiff(secNames = stockFutsPrice.columns, expiry = expiryDate, getStrikes = True)
    if ExiryShift or indExpiryDate != expiryDate:
        tempDF = db_obj.GetNSEBhavCopyStrikePointsDiff(secNames = stockFutsPrice.columns, expiry = indExpiryDate, getStrikes = True)
        #tempDF = db_obj.GetNSEBhavCopyStrikePointsDiff(secNames = stockFutsPrice.columns, expiry = StocksExpiry[expiryDate], getStrikes = True)
        StrikesDF = pandas.concat([StrikesDF, tempDF], axis = 0)
    #StrikeDiffDF = pandas.DataFrame(StrikesDiffDict, index = ['StrikeDiff']).transpose()
    
    StrikesGroups = StrikesDF.groupby('Name')
    if NumWeekAway == 0:
        startDate = iStart
    else:
        startDate = iEnd - datetime.timedelta(7*NumWeekAway)
    
    allDates = [startDate]
    for iIndex in stockFutsPrice.loc[startDate:].index[:-1]:
        indexChg = stockFutsPrice.loc[iIndex, INDEXNAME]/stockFutsPrice.loc[:allDates[-1], INDEXNAME].iloc[-1]
        indexChg = indexChg -1
        if IndexMoveLimit != 0 and numpy.abs(indexChg) >= IndexMoveLimit:
            allDates.append(iIndex)
    
    for activeDate in allDates:
        iDate = stockFutsPrice.loc[:activeDate].index[-1]
        curPriceSer = stockFutsPrice.backfill().loc[iDate].dropna()
        if onlyIndexMove:
            iPrevDate = stockFutsPrice.loc[:allDates[0]].index[-1]
            tempCurPriceSer = stockFutsPrice.backfill().loc[iPrevDate].dropna()
            tempCurPriceSer.loc[INDEXNAME] = curPriceSer.loc[INDEXNAME]
            curPriceSer = tempCurPriceSer
            
        strikesList = []
        for iDx in curPriceSer.index:
            temp = StrikesGroups.get_group(iDx)
            temp = temp.drop_duplicates(keep = 'last')
            temp = temp.sort_values('Strike')
            temp.index = temp.Strike
            price = curPriceSer.loc[iDx]
            if StrikeAway == 0:
                atm = numpy.abs(temp/price -1).idxmin().Strike
                upStrike = atm
                downStrike = atm
                if iDx == INDEXNAME:
                    adjPrice1 = atm*(1+IndexStrikeAway)
                    adjPrice2 = atm*(1-IndexStrikeAway)                    
                    upStrike = numpy.abs(temp/adjPrice1 -1).idxmin().Strike
                    downStrike = numpy.abs(temp/adjPrice2 -1).idxmin().Strike
            else:
                temp = temp/price -1
                atm = numpy.abs(temp).idxmin().Strike
                upStrike = temp.loc[atm:].index[StrikeAway]
                downStrike = temp.loc[:atm].index[-StrikeAway-1]
            strikesList.append((iDx, upStrike, downStrike))
            
        tickersDF = pandas.DataFrame(strikesList, columns = ['Scrip', 'UpOtm', 'DownOtm'])
        tickersDF.index = tickersDF.Scrip
        tickersDF.UpOtm = tickersDF.UpOtm.apply(getStrikeString)#.astype('int')
        tickersDF.DownOtm = tickersDF.DownOtm.apply(getStrikeString)#.astype('int')
        
        if ExiryShift or indExpiryDate != expiryDate:
            tickers1 = []
            tickers2= []
            for iTick in tickersDF.index:
                if iTick == 'BANKNIFTY':
                    #tickers1.append(iTick+StocksExpiry[expiryDate]+'CE'+str(tickersDF.loc[iTick, 'UpOtm']))
                    #tickers2.append(iTick+StocksExpiry[expiryDate]+'PE'+str(tickersDF.loc[iTick, 'DownOtm']))
                    tickers1.append(iTick+indExpiryDate+'CE'+str(tickersDF.loc[iTick, 'UpOtm']))
                    tickers2.append(iTick+indExpiryDate+'PE'+str(tickersDF.loc[iTick, 'DownOtm']))
                else:
                    #tickers1.append(iTick+StocksExpiry[expiryDate]+'CE'+str(tickersDF.loc[iTick, 'UpOtm']))
                    #tickers2.append(iTick+StocksExpiry[expiryDate]+'PE'+str(tickersDF.loc[iTick, 'DownOtm']))
                    tickers1.append(iTick+expiryDate+'CE'+str(tickersDF.loc[iTick, 'UpOtm']))
                    tickers2.append(iTick+expiryDate+'PE'+str(tickersDF.loc[iTick, 'DownOtm']))
        else:    
            tickers1 = [it+expiryDate+'CE'+str(tickersDF.loc[it, 'UpOtm']) for it in tickersDF.index]
            tickers2 = [it+expiryDate+'PE'+str(tickersDF.loc[it, 'DownOtm']) for it in tickersDF.index]
        #tickers = [it+expiryDate+iR+str(tickersDF.loc[it]) for iR in ['CE', 'PE', 'CA', 'PA'] for it in tickersDF.index]
        mydata.Tickers.extend(tickers1)
        mydata.Tickers.extend(tickers2)
        mydata.TradingDates[iDate.date()] = list(set.union(set(tickers1), set(tickers2)))
        print(iDate.date(), len(mydata.TradingDates[iDate.date()]))
  
#SettlementPrice = GetNSEBhavCopyDatabyTicker(conn = bhavconn, tickers = list(set(mydata.Tickers)), fieldName = 'SETTLE_PR')
ClosePrice = db_obj.GetNSEBhavCopyDatabyTicker( tickers = list(set(mydata.Tickers)), fieldName = 'CLOSE')
mydata.Close = ClosePrice#numpy.minimum(SettlementPrice, ClosePrice)
# #f = open('Z:/Pickles/Hist_FutsData'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')

if os.path.exists('Z:/Pickles/'):
    f = open(f'Z:/Pickles/BNF_Disp_{strategy_names[strategyNum]}_{datetime.datetime.today().date().strftime("%d%b-%Y") +".pkl"}', 'wb')
else:
    f = open(f'G:/Shared drives/BackTests/Pickles/BNF_Disp_{strategy_names[strategyNum]}_{datetime.datetime.today().date().strftime("%d%b-%Y") +".pkl"}', 'wb')
    #pickleFile = 'G:/Shared drives/BackTests/Pickles/NiftyDispersionData17Mar-2023.pkl'

pickle.dump(mydata, f)
f.close()
t2 = time.time()
priceconn.close()
print('Data Building Completed:', round((t2-t1)/60, 1), 'Mins!')