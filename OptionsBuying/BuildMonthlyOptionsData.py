# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 10:01:50 2023

@author: Viren@InCred
Build Monthly OptionsData for Options Buying Selling
"""
import sqlite3

#from GetData import GetConn
from GetData import *
import MyTechnicalLib
import time
#from FactorsDef import *
import pickle
import pandas as _pd
import datetime as _dt
#import dask.dataframe as _dd
import os

t1 = time.time()
parentFolder = 'G:/Shared drives/BackTests/OptionsBacktest/workspace/intraday_data_process/options_intraday_nifty/'
allfiles = os.listdir(parentFolder)
allfiles.sort()
#startDate = datetime.datetime(2022, 11, 29)
#endDate = datetime.datetime(2022, 12, 31)

#Filename = 'NIFTY_2019-03-31.pkl'

completed = ['NIFTY_2019-01-31.pkl', 'NIFTY_2019-02-28.pkl']#[, 'NIFTY_2019-03-31.pkl', 'NIFTY_2019-04-30.pkl', 'NIFTY_2019-06-30.pkl', 'NIFTY_2019-11-30.pkl', 'NIFTY_2019-12-31.pkl', 'NIFTY_2020-01-31.pkl', 'NIFTY_2020-02-29.pkl', 'NIFTY_2020-03-31.pkl', 'NIFTY_2020-04-30.pkl', 'NIFTY_2020-05-31.pkl', 'NIFTY_2020-07-31.pkl', 'NIFTY_2020-08-31.pkl', 'NIFTY_2020-09-30.pkl', 'NIFTY_2020-12-31.pkl', 'NIFTY_2021-02-28.pkl', 'NIFTY_2021-03-31.pkl', 'NIFTY_2021-01-31.pkl', 'NIFTY_2020-11-30.pkl', 'NIFTY_2020-10-31.pkl', 'NIFTY_2022-04-30.pkl', 'NIFTY_2021-11-30.pkl', 'NIFTY_2019-05-31.pkl', 'NIFTY_2019-07-31.pkl', 'NIFTY_2019-08-31.pkl', 'NIFTY_2019-09-30.pkl', 'NIFTY_2019-10-31.pkl', 'NIFTY_2020-06-30.pkl', 'NIFTY_2021-04-30.pkl', 'NIFTY_2021-05-31.pkl', 'NIFTY_2021-06-30.pkl', 'NIFTY_2021-07-31.pkl', 'NIFTY_2021-08-31.pkl', 'NIFTY_2021-09-30.pkl', 'NIFTY_2021-10-31.pkl', 'NIFTY_2021-12-31.pkl', 'NIFTY_2022-01-31.pkl', 'NIFTY_2022-02-28.pkl', 'NIFTY_2022-03-31.pkl', 'NIFTY_2022-05-31.pkl', 'NIFTY_2022-06-30.pkl', 'NIFTY_2022-07-31.pkl', 'NIFTY_2022-08-31.pkl', 'NIFTY_2022-09-30.pkl', 'NIFTY_2022-10-31.pkl', 'NIFTY_2022-11-30.pkl', 'NIFTY_2022-12-31.pkl']
for iFilename in allfiles:
    if iFilename in completed:
        continue
    t2 = time.time()
    priceconn = GetConn('PriceData', gDrive = 'Y')
    mydata = MyBacktestData()
    
    mydata.ExpiryDates = QueryExpiryDates(priceconn, expirytype = 'Weekly')
    fields = ['Open', 'High', 'Low', 'Close', 'Volume','OpenInterest']
    
    fileDate = iFilename.split('_')[1].replace('.pkl', '')
    startDate = _dt.datetime.strptime(fileDate, '%Y-%m-%d')
    startDate = _dt.datetime(startDate.year, startDate.month, 1)
    endDate = _dt.datetime.strptime(fileDate, '%Y-%m-%d')+datetime.timedelta(1)
    
    StrikeSpan = 5    
    indexfile = 'G:/Shared drives/BackTests/OptionsBacktest/workspace/intraday_data_process/futures_intraday/NIFTY-I.pkl'
    
    file = open(indexfile,  'rb')
    indexData = pickle.load(file)
    file.close()
    
    indexData.index = [_dt.datetime.combine(it[0], _dt.datetime.strptime(it[1], '%H:%M:%S').time()) for it in indexData.loc[:, ['Date', 'Time']].values]
    indexData.columns = [i.replace(' ', '') for i in indexData.columns]
    indexData = indexData.sort_index()
    indexData = indexData.loc[indexData.index.isin([i for i in indexData.index if i.time() >= _dt.time(9, 15) and i.time() <= _dt.time(15, 30)]), :]
    
    for fieldName in fields:
        tempIndexData = _pd.DataFrame(indexData.loc[:, fieldName])
        tempIndexData.columns = [indexData.loc[:, 'Name'][0].split('-')[0]]
        tempIndexData = tempIndexData.sort_index()
        setattr(mydata.Index, fieldName, tempIndexData)
        
    mydata.indexprice = mydata.Index.Close
    del indexData
    ##### Index Data Updated ############
    
    for fieldName in fields:
        if not hasattr(mydata, fieldName):
            setattr(mydata, fieldName, _pd.DataFrame())
    
    ExpiryRange = [ i for i in mydata.ExpiryDates if (datetime.datetime.strptime(i, '%Y-%m-%d') >= startDate and datetime.datetime.strptime(i, '%Y-%m-%d') <= endDate + datetime.timedelta(31) )]
    ExpiryRange = [datetime.datetime.strptime(i, '%Y-%m-%d').strftime('%d%b%y').upper() for i in ExpiryRange]
    
    #picklefilename = 'G:/Shared drives/BackTests/OptionsBacktest/workspace/intraday_data_process/options_intraday_nifty/NIFTY_2022-12-31.pkl'
    picklefilename = os.path.join(parentFolder, iFilename)
    file = open(picklefilename,  'rb')
    tempdata = pickle.load(file)
    file.close()
    
    ff = mydata.indexprice.loc[startDate:endDate]
    StrikeRange = [str(i) for i in range(int(ff.min()/100)*100 - StrikeSpan*100, int(ff.max()/100)*100 + (StrikeSpan+1)*100, 100)]
    tempdata = tempdata[tempdata.StrikePrice.isin(StrikeRange)]
    tempdata = tempdata[tempdata.ExpiryDate.isin(ExpiryRange)]
    tempdata.columns = [i.replace(' ', '') for i in tempdata.columns]
    
    
    tempdata['ID'] = [''.join(it) for it in tempdata.loc[:, ['Name', 'StrikePrice', 'Call_Or_Put', 'ExpiryDate']].values]
    gg = tempdata.groupby('ID')
    
    
    for grp in gg.groups.keys():
        dtemp = gg.get_group(grp)
        dtemp.index = [_dt.datetime.combine(it[0], _dt.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in dtemp.loc[:, ['Date', 'Time']].values]
        dtemp = dtemp.loc[dtemp.index.isin([i for i in dtemp.index if i.time() >= _dt.time(9, 15) and i.time() <= _dt.time(15, 30)]), :]
        for fieldName in fields:
            #dd = dtemp.loc[:, ['Date', 'Time', fieldName]]
            #dd.index = [_dt.datetime.combine(it[0], _dt.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in dd.loc[:, ['Date', 'Time']].values]
            #dd = dd.loc[dd.index.isin([i for i in dd.index if i.time() >= _dt.time(9, 15) and i.time() <= _dt.time(15, 30)]), :]
            #del dd['Date']
            #del dd['Time']
            dd = _pd.DataFrame(dtemp.loc[:, fieldName])
            dd.columns = [grp]
            setattr(mydata, fieldName, _pd.concat([getattr(mydata, fieldName), dd], axis = 1))
    del tempdata        
    
    for fieldName in fields:
        if hasattr(mydata, fieldName):
            setattr(mydata, fieldName, getattr(mydata, fieldName).sort_index())
    
    #f = open('Z:/Pickles/NiftyOptions_Dec22_'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
    
    f = open(os.path.join('G:/Shared drives/BackTests/Pickles/NiftyOptions/', iFilename), 'wb')
    #f = open(os.path.join('Z:/Pickles/NiftyOptions/', iFilename), 'wb')
    pickle.dump(mydata, f)
    f.close()
    t3 = time.time()
    print(iFilename, round((t3-t2)/60, 2), 'Mins', sep = ' ')
    del mydata
    completed.append(iFilename)
print('Completed in:', round((t3-t1)/60, 2), 'Mins', sep = ' ')