# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 10:49:24 2025
@author: Viren@InCred

Backtesting Data for the Long-Short IV Model
1. Long Short based on IV of the stocks- Select the top stocks from the universe based on IV, Short high IV stocks and Long low IV Stocks
2. Based on IV/ Trailing Volatility, Go long on the top stocks which shows lower ratio and Short on the top stock which shows ligher ratio

"""
import pandas as pd
import datetime as dt
import numpy as np
import psycopg2
from MainLibs.GetData import getStrikeString, CommaSeparatedList
import re

OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d+[A-Z]+\d+)(?P<option_type>[A-Z]+)(?P<strike>\d+(\.\d+)?)'


portConn = psycopg2.connect(dbname= 'data', user= 'postgres', password='postgres', host='10.147.0.69', port='5432')#
portCurs = portConn.cursor()

startDate = dt.date(2022, 3, 30)
endDate = dt.date(2025, 5, 20)


basesql = '''SELECT "Ticker", "Date", "IV", "Vega" FROM public.greeks_nsefno WHERE "Ticker" ~* ANY (ARRAY['^RELIANCE', '^TCS'])  AND "Date"::date >= %s  AND "Date"::date <= %s;'''

# Assuming startDate and endDate are either 'YYYY-MM-DD' strings or datetime.date objects
portCurs.execute(portCurs.mogrify(basesql, (startDate, endDate, )))
DeltaData = pd.DataFrame(portCurs.fetchall(), columns =  [rec[0] for rec in portCurs.description])
DeltaData.index = DeltaData.Ticker

tempDF = pd.DataFrame([re.match(OptionTickerRegEx, iTicker).groupdict() for iTicker in DeltaData.Ticker], index = DeltaData.Ticker)
#tempDF.strike = tempDF.strike.astype('float')
tempDF.strike = tempDF.strike.apply(getStrikeString)

DeltaData = pd.concat([DeltaData, tempDF], axis = 1)

