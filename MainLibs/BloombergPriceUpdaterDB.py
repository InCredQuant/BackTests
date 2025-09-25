# -*- coding: utf-8 -*-
"""
Created on Fri Nov 11 10:59:52 2022

@author: Viren@InCred
"""
import Bloomberg as bbg
import blpapi
import pandas
import sqlite3
import datetime
import numpy
import re
from GetData import *
from dateutil.relativedelta import relativedelta

priceconn = GetConn('PriceData')#sqlite3.connect(dbpath +'/PriceData.db')
analystconn = GetConn('AnalystData')#sqlite3.connect(dbpath + '/AnalystData.db')

startday = datetime.date(2000,1, 1)
today = datetime.date.today()
monthstartdate = datetime.date(today.year, today.month, 1)#datetime.datetime(2001, 1, 1).date()

indicesList = ['SENSEX INDEX', 'NIFTY INDEX', 'NIFTYJR INDEX', 'BSE100 INDEX', 'NSE100 INDEX' , 'NSEMCAP INDEX', 'NSESMCP INDEX','BSE200 INDEX', 'BSE500 INDEX', 'NSE500 INDEX', 'NSE200 INDEX',
               'NSEAUTO INDEX', 'NSEBANK INDEX', 'NSEFMCG INDEX', 'NSEIT INDEX', 'NSEMET INDEX', 'NSEPHRM INDEX', 'NSEPSBK INDEX', 'NSEINFR INDEX', 'NSECON INDEX', 'NSEFIN INDEX', 
               'NSECMD INDEX', 'NSENRG INDEX', 'NSEPSE INDEX', 'BSEALL INDEX', 'NMIDSELP INDEX']
# for updating the Indices Constituents, in case of past dates run manually

(newStocks, oldStocks, checkDate, newFuts, oldFuts, checkDateFut) = bbg.StocksTickerstoUpdate()

#checkDateFut = checkDate
#ok = newFuts.extend([it.replace('1', '2') for it in newFuts])
#ok = newFuts.extend([it.replace('1', '2') for it in oldFuts])

#indices = ['NSE200 INDEX']#, 'NSECMD INDEX', 'NSENRG INDEX', 'NSEPSE INDEX']
# if checkDate.day <= 4:
    
# monthstartdate = datetime.date(checkDate.year, checkDate.month+1, 1)-datetime.timedelta(1)
# for ind in indicesList:
#      bbg.RequestIndexData(monthstartdate, ind)#monthstartdate
#bbg.RequestIndexData(startday, ind)

futIndices = ['RNS1 Index', 'NZ1 Index', 'AF1 Index', 'MYB1 Index']#, 'NZ2 Index', 'AF2 Index']
indicesList.extend(futIndices)


#BloomRequestData(tickerslist,datatype = 'stockdata',sectype = 'not index',action = 'insert', startdate = "20050101", enddate = datetime.date.today().strftime('%Y%m%d'))
print('Updating Price Data..')
d1 = bbg.BloomRequestData(oldStocks, startdate = checkDate.strftime('%Y%m%d')) # updating price data from last updated date
if len(newStocks) >0:
    d2 = bbg.BloomRequestData(newStocks, startdate = startday.strftime('%Y%m%d')) # updating data for those where some corp action or new introduced

print('Updating Index Price Data..')
d3 = bbg.BloomRequestData(indicesList, sectype = 'index', startdate = checkDate.strftime('%Y%m%d')) # updating all the indices data, from last uddated date
#d4 = bbg.BloomRequestData(futIndices, sectype = 'index', startdate = startday.strftime('%Y%m%d'))

'''
print('Updating Analyst Data..')
d5 = bbg.BloomRequestData(oldStocks, datatype = 'analystdata', startdate = checkDate.strftime('%Y%m%d'))
d6 = bbg.BloomRequestData(newStocks, datatype = 'analystdata', startdate = startday.strftime('%Y%m%d'))

print('Updating EPS Data..')
d7 = bbg.BloomRequestData(oldStocks, datatype = 'EPSData', startdate = checkDate.strftime('%Y%m%d'))
d8 = bbg.BloomRequestData(newStocks, datatype = 'EPSData', startdate = startday.strftime('%Y%m%d'))

'''
print('Updating Fut Data..')
d9 = bbg.BloomRequestData(oldFuts, datatype = 'FutData', startdate = checkDateFut.strftime('%Y%m%d'))
d10 = bbg.BloomRequestData(newFuts, datatype = 'FutData', startdate = startday.strftime('%Y%m%d'))

priceconn.close()
analystconn.close()