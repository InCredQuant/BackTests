# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 12:36:58 2023

@author: Viren@InCred
To update the DB from the NSE Bhav Copy.
It unzips the bhavcopy file and uploads it to the DB
"""
import sys
import os
import pandas as pd
import sqlite3
sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
from MainLibs.GetData import *
#import zipfile
import pdb
#import psycopg2
#import psycopg2.extras
#import mibian
import time


def getIndicesSpotData():
    IndDict = {'NSEBANK INDEX' : 'BANKNIFTY', 'NSEFIN INDEX' : 'FINNIFTY', 'NSEMCAP INDEX' : 'MIDCPNIFTY', 'NIFTY INDEX' : 'NIFTY'}
    priceConn = GetConn('PriceData', gDrive = 'Y')    
    
    SpotPriceDF = GetDataForIndicesFromBloomDB(priceConn, list(IndDict.keys()), 'PX_LAST')
    SpotPriceDF = SpotPriceDF.sort_index()
    SpotPriceDF.columns = [IndDict[it] for it in SpotPriceDF.columns]
    return SpotPriceDF
                                           
def ReadFile(zipFileName):
    cols = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN','HIGH', 'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH','OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']
    oneDayData = pd.read_csv(zipFileName, compression='zip')
    if 'OPTIONTYPE' in oneDayData.columns:
        oneDayData.rename(columns={'OPTIONTYPE':'OPTION_TYP'}, inplace=True)
    oneDayData = oneDayData[cols]
    #----------------------------------------------------------------------------------------
    try:
        oneDayData['EXPIRY_DT'] = pd.to_datetime(oneDayData['EXPIRY_DT'], format="%d-%b-%Y").dt.strftime('%d%b%y').str.upper()
    except:
        try:
            oneDayData['EXPIRY_DT'] = pd.to_datetime(oneDayData['EXPIRY_DT'], format="%d-%b-%y").dt.strftime('%d%b%y').str.upper()
        except:
            raise
    # ----------------------------------------------------------------------------------------
    try:
        oneDayData['TIMESTAMP'] = pd.to_datetime(oneDayData['TIMESTAMP'], format="%d-%b-%Y").dt.date
    except:
        try:
            oneDayData['TIMESTAMP'] = pd.to_datetime(oneDayData['TIMESTAMP'], format="%d-%b-%y").dt.date
        except:
            raise
    # ----------------------------------------------------------------------------------------
    #new_col = oneDayData.SYMBOL + oneDayData.STRIKE_PR.astype('str') + oneDayData.OPTION_TYP + oneDayData.EXPIRY_DT#.astype('int')
    temp1 = oneDayData.loc[oneDayData.STRIKE_PR.astype('int') == oneDayData.STRIKE_PR].copy()
    temp1.STRIKE_PR = temp1.STRIKE_PR.astype('int').astype('str')
    
    temp2 = oneDayData[oneDayData.STRIKE_PR.astype('int') != oneDayData.STRIKE_PR].copy()
    temp2.STRIKE_PR = temp2.STRIKE_PR.astype('str')
    
    oneDayData = pd.concat([temp1, temp2], axis = 0)
    new_col = oneDayData.SYMBOL + oneDayData.EXPIRY_DT + oneDayData.OPTION_TYP + oneDayData.STRIKE_PR.astype('str')
    oneDayData.insert(loc=0, column='Ticker', value=new_col)
    #oneDayData['Ticker'] = 
    return oneDayData
    
    
def InsertoDB(fullData):
    fnoconn = GetConn('BhavCopy', gDrive = 'N')
    fullData.to_sql('NSEFNO', fnoconn, if_exists = 'append', index = False)
    fnoconn.commit()
    fnoconn.close()
    return 1

def GetAllDatesfromDB():
    bhavconn = GetConn('BhavCopy', gDrive = 'Y')
    basesql = "select distinct(TIMESTAMP) as Date from NSEFNO;"
    curs = bhavconn.cursor()
    curs.execute(basesql)
    dates = curs.fetchall()
    dates = [it[0] for it in dates]
    bhavconn.close()
    return dates


def GetOneDayBhavData(iDate):
    bhavconn = GetConn('BhavCopy', gDrive = 'Y')
    basesql = "select * from NSEFNO where TIMESTAMP = '%s';" % (iDate)
    curs = bhavconn.cursor()
    curs.execute(basesql)
    df = pandas.DataFrame(curs.fetchall())
    #df.columns = [rec[0] for rec in curs.description]
    df.columns = ['Ticker', 'Instrument', 'Scrip', 'Expiry', 'Strike', 'OptionType', 'Open',
           'High', 'Low', 'Close', 'SettlePrice', 'Contracts', 'Val_Lakh', 'OI',
           'ChgOI', 'Date']
    df.index = df.Ticker
    bhavconn.close()
    return df





def CalculateGreeks(df):
    df.OptionType = [it.replace('CA', 'CE').replace('PA', 'PE') for it in df.OptionType]
    df.Ticker = df.Scrip + df.Expiry + df.OptionType + df.Strike.apply(getStrikeString)
    df.index = df.Ticker
    for iField in ['IV', 'Delta', 'Delta2', 'Rho', 'Theta', 'Vega', 'Gamma']:
        df[iField] = len(df.index)*[numpy.NAN]
        
    for iIndex in df.index:
        if df.loc[iIndex, 'Instrument'] in ['OPTIDX', 'OPTSTK']:
            tempdf = df.loc[iIndex]
            expiryDate = datetime.datetime.strptime(tempdf.loc['Expiry'], '%d%b%y')
            curDate = datetime.datetime.strptime(tempdf.loc['Date'], '%Y-%m-%d')
            futTicker = "".join(tempdf.loc[['Scrip', 'Expiry']])+'XX0'
            daysToExpiry = (expiryDate - curDate).days
            optionType = tempdf.loc['OptionType']
            if futTicker in df.index:
                futPrice = df.loc[futTicker, 'Close']
                intRate = 0
            else:
                futPrice = SpotPriceDF.loc[curDate, tempdf.Scrip]
                intRate = 6 # 6 %is taken Risk free rate
            
            if optionType == 'CE':
                c = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], callPrice = tempdf.loc['Close'])
                opt = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], volatility  = c.impliedVolatility)
                df.loc[iIndex, 'Delta'] = opt.callDelta
                df.loc[iIndex, 'Delta2']  = opt.callDelta2
                df.loc[iIndex, 'Rho'] = opt.callRho
                df.loc[iIndex, 'Theta'] = opt.callTheta
            elif optionType == 'PE':
                p = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], putPrice = tempdf.loc['Close'])
                opt = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], volatility  = p.impliedVolatility)
                df.loc[iIndex, 'Delta'] = opt.putDelta
                df.loc[iIndex, 'Delta2']  = opt.putDelta2
                df.loc[iIndex, 'Rho'] = opt.putRho
                df.loc[iIndex, 'Theta'] = opt.putTheta
            
            try:
                df.loc[iIndex, 'IV'] = opt.volatility
            except:
                pass
            df.loc[iIndex, 'Vega'] = opt.vega
            df.loc[iIndex, 'Gamma'] = opt.gamma
    return df

def InsertoPostGresDB(df):
    conn = psycopg2.connect(
        database="Quant",
      user='Viren', 
      password='viren123', 
      host='localhost', 
      port= '5432'
    )
      
    conn.autocommit = True
    if len(df) > 0:
        df_columns = list(df)
        # create (col1,col2,...)
        columns = '","'.join(df_columns)
        columns = '"'+columns+'"'

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {};".format('"Quant".bhavcopy_nsefno',columns,values)

        cur = conn.cursor()
        
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        conn.commit()
        cur.close()
    return 1

#----------------------
# AllDates = GetAllDatesfromDB()
# SpotPriceDF = getIndicesSpotData()
# failList = []
# successList = []
#
# AllDates.reverse()
#
# for iDate in AllDates:
#    try:
#        t1 = time.time()
#        df = GetOneDayBhavData(iDate)
#        #df = df[df.Scrip == 'NIFTY']
#        t2 = time.time()
#        idf = CalculateGreeks(df)
#        t3 = time.time()
#        InsertoPostGresDB(idf)
#        successList.append(iDate)
#        t4 = time.time()
#        print(iDate, 'Download: ', numpy.round((t2-t1)/60, 1),'Processing: ', numpy.round((t3-t2)/60, 1), 'Upload: ', numpy.round((t4-t3)/60, 1))
#    except:
#         failList.append(iDate)
#         print('Failed: ', iDate)
#         #pdb.set_trace()
   
#zipFilesDir = r'G:\Shared drives\BackTests\DB\June23\\'#r'G:\Shared drives\BackTests\DB\Bhavcopy\2023\June\\'#'Z:/LiveDB/Bhavcopy/'
zipFilesDir = 'G:/Shared drives/BackTests/DB/Bhavcopy/temp/'

Completed = []
ErrorFiles = []
finalDF = pd.DataFrame()
counter = 0
for root, dirs, files in os.walk(zipFilesDir):
    for filename in files:
        zipFileName = os.path.join(root, filename)
        try:
            df = ReadFile(zipFileName)
            finalDF = pd.concat([finalDF, df], axis = 0)
            Completed.append(filename)
            print('Processed:', filename)
            counter += 1
        except:
            ErrorFiles.append(filename)
            print('Error:', filename)            
            pass
        if counter >= 250:
            try:
                InsertoDB(finalDF)
                print('Inserted to DB!')
                counter = 0
                finalDF = pd.DataFrame()
            except:
                raise
                #pdb.set_trace()

try:
    InsertoDB(finalDF)
    print('Inserted to DB!')
except:
    raise
    #pdb.set_trace()

# zipFileName = 'C:/Users/virendra.kumar_incre/Downloads/fo30MAR2021bhav_revised.zip'
# indata = ReadFile(zipFileName)
# InsertoDB(indata)