# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 11:30:07 2024

@author: virendra.kumar_incre
"""

import pandas
import os
import matplotlib.pyplot as plt
import numpy as np
from GetData import *
import pickle

indexName = 'BANKNIFTY'

basePath = 'G:/Shared drives/BackTests/Spot Data 1min/'

spotData = pandas.read_csv(os.path.join(basePath, '.NIFTY BANK.csv'))
spotData.index = pandas.to_datetime(spotData.date)
spotData = spotData.resample('1T').last().dropna()

fut1Data = pandas.DataFrame()
fut2Data = pandas.DataFrame()


for iYr in range(2019, 2024):
    tempfut1Data = pandas.read_parquet(os.path.join(basePath, f'BANKNIFTY_synthetic_futures_{iYr}.parquet'))
    tempfut2Data = pandas.read_parquet(os.path.join(basePath, f'BANKNIFTY_synthetic_futures_{iYr}-II.parquet'))
    fut1Data = pandas.concat([fut1Data, tempfut1Data], axis = 0)
    fut2Data = pandas.concat([fut2Data, tempfut2Data], axis = 0)


fut1 = pandas.DataFrame(fut1Data.Synthetic)
fut1.columns = ['S1']

fut2 = pandas.DataFrame(fut2Data.Synthetic)
fut2.columns = ['S2']

spot = pandas.DataFrame(spotData.close)
spot.columns = ['Spot']

inData = pandas.concat([fut1, fut2, spot], axis = 1)
#inData.columms = ['S1', 'S2', 'Spot']
inData = inData.dropna()
inData['D1'] = inData.S1 - inData.Spot
inData['D2'] = inData.S2 - inData.Spot


# Plot the distribution chart (histogram)
plt.hist(inData.D1/inData.Spot, bins=200, edgecolor='black')  # Adjust the number of bins as needed
plt.title('Distribution S1- Spot')
plt.xlabel('Value')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

plt.hist(inData.D2/inData.Spot, bins=200, edgecolor='black')  # Adjust the number of bins as needed
plt.title('Distribution S2- Spot')
plt.xlabel('Value')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

plt.hist((inData.S1-inData.S2)/inData.Spot, bins=200, edgecolor='black')  # Adjust the number of bins as needed
plt.title('Distribution S1- S2')
plt.xlabel('Value')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

inData['%D1'] = inData.D1/inData.Spot
inData['%D2'] = inData.D2/inData.Spot
inData['%Diff'] = (inData.S1-inData.S2)/inData.Spot

###################
# Starts the Backtesting Logic
lowerLevel = -0.20/100
upperLevel = 0.10/100

# Rules for Taking Positions
#if %Diff <= -18 bps & %D1 > 9bps	then go short on D1 and Long on D2	Exit if %D1  comes below 3 bps	6 bps movement capture			0.06%	29.18886
#if %Diff >= 5 bps & %D1 < -5 bps	then go long on D1 and short  on D2	Exit if %D1 comes above -1 bps	4 bps movement capture			0.04%	19.45924

from  GetDataPostGres import DataBaseConnect

db_obj = DataBaseConnect()
db_obj.Connect()

#
#spotFile = 'G:/Shared drives/BackTests/BANKNIFTY_FUT_OPT_DATA/banknifty.pkl'
#f = open(spotFile, 'rb')
#mydata = pickle.load(f)
#f.close()

import datetime
import pandas
import numpy
import math

indexName = 'BANKNIFTY'
strikeSpan = 0.02# 2%
mydata = MyBacktestData()
mydata.ExpiryDates = db_obj.getExpiryDates('w', symbol = indexName)
mydata.Spot = db_obj.getIndexSpotMinData(indexName, mydata.ExpiryDates[0], mydata.ExpiryDates[-1], fieldName = 'Close')
for iExpiry in mydata.ExpiryDates[100:]:
    iD = mydata.ExpiryDates.index(iExpiry)
    nearExpiry = iExpiry.strftime('%d%b%y').upper()
    pastExpiry = mydata.ExpiryDates[iD-1]
    fromDate = pastExpiry+ datetime.timedelta(1)
    toDate = iExpiry
    tempData = mydata.Spot.loc[pandas.to_datetime(fromDate): pandas.to_datetime(toDate) + datetime.timedelta(1)]
    lowerStrike = math.floor((tempData.min()*(1- strikeSpan))[0]/100)*100
    upperStrike = math.ceil((tempData.max()*(1+ strikeSpan))[0]/100)*100
    mydata.Close1 = db_obj.getDatabyExpiry(lowerStrike, upperStrike, expiryDate = nearExpiry, name = indexName, fieldName = 'Close', week = 'W1')   
    mydata.Close2 = db_obj.getDatabyExpiry(lowerStrike, upperStrike, expiryDate = mydata.ExpiryDates[iD+1].strftime('%d%b%y').upper(), name = indexName, fieldName = 'Close', week = 'W2')
    for iTime in mydata.Spot.index:
        
    break