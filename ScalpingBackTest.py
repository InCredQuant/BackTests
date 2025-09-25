# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 15:30:06 2023

@author: virendra.kumar_incre
"""

"""
Created on Mon Aug  7 09:30:06 2023
@author: Viren@InCred
"""
#%matplotlib notebook
#from ipywidgets import *
from ipywidgets import interactive
#%matplotlib inline

import psycopg2
import pandas as pd
import datetime as dt
import numpy as np

import warnings
warnings.filterwarnings("ignore")

TickerDict = {'Nifty': 'NIFTY-I.NFO', 'BankNifty' : 'BANKNIFTY-I.NFO'}
ticker = 'Nifty'

conn = psycopg2.connect(dbname= 'alts', user= 'postgres', password='postgres', host='192.168.44.9', port='5432')
curs = conn.cursor()

basesql = '''Select "Ticker", "Date", "Time", "Close" from pricedata."stocks" where "Ticker" = '{}';'''.format(TickerDict[ticker])
curs.execute(basesql)
df = pd.DataFrame(curs.fetchall(), columns =  [rec[0] for rec in curs.description])
df.Date = pd.to_datetime(df.Date)



df['TimeK'] = pd.to_datetime(df.Time, format = '%H:%M:%S').dt.time
df.Time = pd.to_timedelta(df.Time)
df['DateTime'] = df.Date + df.Time
df.index = df.DateTime

dailyPriceDF = pd.DataFrame(df.loc[:, 'Close'].resample('30Min').mean().dropna().resample('d').last().dropna())
dailyPriceDF.columns = [ticker]

df = df[df.TimeK >= dt.time(9, 16, 0)]
df = df[df.TimeK <= dt.time(15, 28, 0)]

priceDF = pd.DataFrame(df['Close'])
priceDF.columns = [ticker]
priceDFchg = priceDF.diff()



####################################################  Original Basic verison ############################################################
StartQty = 0 # Starting Quantity
AddQty = 50
#MaxQty = 5000
PointDiff = [20, 300, 5] # from min Points, Maximum Points, Differ by each interval, By how much points diff we want to enter/exit
PointDiffPct = [10, 150, 5]# bps Difference
Percentile = 0.70 # 70 Percentle

# it is computation Heavy Part/ it migh take some minutes to execute
#numCols = [x for x in range(PointDiff[0], PointDiff[1]+1, PointDiff[2])]
numCols = [x for x in range(PointDiffPct[0], PointDiffPct[1]+1, PointDiffPct[2])]
Quantity = []
Quantity.append([StartQty]*len(numCols))
Tracker = [0]*len(numCols)

for iT in priceDF.index[1:]:
    Tracker += priceDFchg.loc[iT, ticker]
    qty = [0]*len(numCols)
    price = int(priceDF.loc[iT, ticker]/100)*100
    points = [i*price/10000 for i in numCols]
    
    for j, val in enumerate(Tracker):
        qty[j] = max(0, Quantity[-1][j] - int(Tracker[j]/points[j])*AddQty) # if index Goes Up, then sutract the Quantity and otherway add Up more
        if int(Tracker[j]/points[j]) != 0:
            Tracker[j] = 0    
    Quantity.append(qty)

QuantityDF = pd.DataFrame(Quantity)
QuantityDF.columns = [str(x)+'bps' for x in numCols]
QuantityDF.index = priceDF.index[:]

ExposureDF = np.multiply(QuantityDF, priceDF.iloc[:])
pnlDF = np.multiply(QuantityDF, priceDFchg.shift(-1).iloc[:]).shift(1)
pnlPerctDF = pnlDF/(ExposureDF.shift(1).abs().quantile(Percentile))

pnlNAV = (1+pnlPerctDF)
pnlNAV.iloc[0] = 100
pnlNAV = pnlNAV.cumprod()

#############################################################################################
import matplotlib
import matplotlib.pyplot as plt

plt.style.use('ggplot')
matplotlib.rcParams['figure.figsize'] = [30.0, 12.0]
matplotlib.rcParams['font.size'] = 25
matplotlib.rcParams['lines.linewidth'] = 3.0


pnlDaily = pnlNAV.resample('d').last().dropna()
pnlDaily.index = pnlDaily.index.date

indexRet = priceDF.resample('d').last()
indexRet.index = pd.to_datetime(indexRet.index.date)
pnlDaily.index = pd.to_datetime(pnlDaily.index)
indexRet = indexRet.loc[pnlDaily.index]

indexNAV = (1+indexRet.pct_change())
indexNAV.iloc[0] = 100
indexNAV = indexNAV.cumprod()

QuantityDaily = QuantityDF.resample('d').last().dropna()
QuantityDaily.index = QuantityDaily.index.date

def combined(PointsInterval):
    with plt.ion():
        fig, ax1 = plt.subplots()
        color = 'tab:red'
        ax1.set_xlabel('Date')
        ax1.set_ylabel('NAV', color=color)
        ax1.plot(pnlDaily.loc[:, PointsInterval], color=color)
        ax1.plot(indexNAV, color='tab:green')
        
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
        color = 'tab:blue'
        ax2.set_ylabel('Quantity', color=color)  # we already handled the x-label with ax1
        ax2.plot(QuantityDaily.loc[:, PointsInterval], color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()  # otherwise the right y-label is slightly clipped
        plt.show()

def nav(PointsInterval):
    with plt.ion():
        plt.plot(pnlDaily.loc[:, PointsInterval])
        plt.xlabel('Date')
        plt.title(ticker + ' -Scalping NAV')
        plt.show()
    #plt.scatter(df[x], df[y])
    #plt.show()

interactive_plot = interactive(combined, PointsInterval=pnlDaily.columns)
interactive_plot


###########################  Moving Average verison      #################

"""
Created on Mon Aug  7 09:30:06 2023
@author: Viren@InCred
"""
#%matplotlib notebook
#from ipywidgets import *
from ipywidgets import interactive
#%matplotlib inline

import psycopg2
import pandas as pd
import datetime as dt
import numpy as np

import warnings
warnings.filterwarnings("ignore")

TickerDict = {'Nifty': 'NIFTY-I.NFO', 'BankNifty' : 'BANKNIFTY-I.NFO'}
ticker = 'Nifty'

conn = psycopg2.connect(dbname= 'alts', user= 'postgres', password='postgres', host='192.168.44.9', port='5432')
curs = conn.cursor()

basesql = '''Select "Ticker", "Date", "Time", "Close" from trade."stocks" where "Ticker" = '{}';'''.format(TickerDict[ticker])
curs.execute(basesql)
df = pd.DataFrame(curs.fetchall(), columns =  [rec[0] for rec in curs.description])
df.Date = pd.to_datetime(df.Date)

df['TimeK'] = pd.to_datetime(df.Time, format = '%H:%M:%S').dt.time
df.Time = pd.to_timedelta(df.Time)
df['DateTime'] = df.Date + df.Time
df.index = df.DateTime

dailyPriceDF = pd.DataFrame(df.loc[:, 'Close'].resample('30Min').mean().dropna().resample('d').last().dropna())
dailyPriceDF.columns = [ticker]

df = df[df.TimeK >= dt.time(9, 16, 0)]
df = df[df.TimeK <= dt.time(15, 28, 0)]

priceDF = pd.DataFrame(df['Close'])
priceDF.columns = [ticker]
priceDFchg = priceDF.diff()




StartQty = 0 # Starting Quantity
AddQty = 50 if ticker == 'Nifty' else 25
#MaxQty = 5000
#PointDiff = [20, 300, 5] # from min Points, Maximum Points, Differ by each interval, By how much points diff we want to enter/exit
PointDiffPct = [70, 90, 10]# bps Difference
Percentile = 0.80 # 70 Percentle

# it is computation Heavy Part/ it migh take some minutes to execute
#numCols = [x for x in range(PointDiff[0], PointDiff[1]+1, PointDiff[2])]
numCols = [x for x in range(PointDiffPct[0], PointDiffPct[1]+1, PointDiffPct[2])]
Tracker = [0]*len(numCols)

# if Above DMA, then Long Biased-> No Short Position, Cover All Shorts if changes from below to Above DMA
#if below DMA then Short Biased -> No Long Position, Close all Long if Changes from Above DMA to below DMA
#1. Check for Single Moving Average Version [3, 21], DMA

MADays = [i for i in range(5, 21+1, 4)]
OverAllQty = pd.DataFrame()

for imaDay in MADays:
    Quantity = []
    Quantity.append([StartQty]*len(numCols))
    dailyPriceDF['MA'] = dailyPriceDF.loc[:, ticker].rolling(imaDay).mean()    
    for iT in priceDF.index[1:]:
        if iT.date() <= dailyPriceDF.dropna().index[0].date():
            continue
        Tracker += priceDFchg.loc[iT, ticker]
        qty = [0]*len(numCols)
        price = int(priceDF.loc[iT, ticker]/100)*100
        points = [i*price/10000 for i in numCols]
        tempDF = dailyPriceDF.loc[:iT.date(), :].iloc[-2]
        dmaPos = True if tempDF[ticker] >= tempDF['MA'] else False
        
        for j, val in enumerate(Tracker):
            if dmaPos:
                qty[j] = max(0, Quantity[-1][j] - int(Tracker[j]/points[j])*AddQty) # if index Goes Up, then sutract the Quantity and otherway add Up more
            else:
                qty[j] = min(0, Quantity[-1][j] - int(Tracker[j]/points[j])*AddQty)
            if int(Tracker[j]/points[j]) != 0:
                Tracker[j] = 0
        Quantity.append(qty)        
    QuantityDF = pd.DataFrame(Quantity, index = priceDF.index[-len(Quantity):])
    QuantityDF.columns = [str(imaDay)+'DMA_'+str(x)+'bps' for x in numCols]
    OverAllQty = pd.concat([OverAllQty, QuantityDF], axis = 1)
    #QuantityDF.index = priceDF.index[:]

ExposureDF = np.multiply(OverAllQty, priceDF.iloc[-len(OverAllQty.index):])
pnlDF = np.multiply(OverAllQty, priceDFchg.shift(-1).iloc[-len(OverAllQty.index):]).shift(1)
pnlPerctDF = pnlDF/(ExposureDF.shift(1).abs().quantile(Percentile))

pnlNAV = (1+pnlPerctDF)
pnlNAV.iloc[0] = 100
pnlNAV = pnlNAV.fillna(1)
pnlNAV = pnlNAV.cumprod()


############################################################################################

import matplotlib
import matplotlib.pyplot as plt

plt.style.use('ggplot')
matplotlib.rcParams['figure.figsize'] = [30.0, 12.0]
matplotlib.rcParams['font.size'] = 25
matplotlib.rcParams['lines.linewidth'] = 3.0

pnlDaily = pnlNAV.resample('d').last().dropna()
pnlDaily.index = pnlDaily.index.date

indexRet = priceDF.resample('d').last()
indexRet.index = pd.to_datetime(indexRet.index.date)
pnlDaily.index = pd.to_datetime(pnlDaily.index)
indexRet = indexRet.loc[pnlDaily.index]

indexNAV = (1+indexRet.pct_change())
indexNAV.iloc[0] = 100
indexNAV = indexNAV.cumprod()

QuantityDaily = QuantityDF.resample('d').last().dropna()
QuantityDaily.index = QuantityDaily.index.date

import multiprocessing
multiprocessing.Process()