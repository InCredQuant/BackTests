# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 10:20:14 2023

@author: virendra.kumar_incre
It calculates the Portoflio Delta, based on the Positions from the backtested Excel file
For Options Positions, It takes Quantity Sheet form the Backtest File
For Futs/ Equity Positions, it Takes, Position Sheet from the backtest File.
"""

import pandas as pd_
import datetime as dt_
import psycopg2
import pdb
import tkinter as tk_
from tkinter import filedialog
import numpy
from GetData import *
import re
#import time

root = tk_.Tk()
#root.withdraw()

conn = GetConn('PriceData')
if 'conn' in locals():
    startDate = dt_.date(2000, 12, 31)
    indexData = GetDataForFutTickersFromBloomDB(conn, ['NZ1 INDEX'], 'PX_LAST', startDate)
    indexData = pandas.DataFrame(indexData.loc[startDate:, :])

FundSize = 100 # Cr.
OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d+[A-Z]+\d+)(?P<option_type>[A-Z]+)(?P<strike>\d+(\.\d+)?)'

# USed to  Make Dict of Strategy Names and Weights
#dict([(temp.loc[it, 'StrategyCode'], float(temp.loc[it, 'Weight'].replace('%', ''))/100) for it in temp.index])
#dict([(temp.loc[it, 'StrategyCode'], temp.loc[it, 'ModelName']) for it in temp.index])
stNames = {'BNOMDISP1': 'Expiry to Expiry', 'BNOMDISP2': '3 Week to Expiry', 'BNOMDISP3': 'Strangle 3 Week to Expiry', 'BNOMDISP4': 'Shift Entire Straddle at 3% Bank Nifty Move', 'NFFDMOM15': 'Nifty Breakout', 'NFFDMOM14': 'Nifty Pattern', 'NFFDMOM10': 'Nifty-2RSI withTrend', 'NFFDMOM12': 'Nifty-Seasonality', 'NFFDMOM06': 'Nifty-ROCMA', 'BNFDMOM15': 'BankNifty Breakout', 'BNFDMOM14': 'BankNifty Pattern', 'BNFDMOM12': 'BankNifty Seasonality', 'BNFDMOM04': 'BankNifty 20wma+macd', 'BNFDMOM13': 'BankNifty Asymmetric Daily', 'BNFDMOM06': 'BankNifty ROCMA', 'BNODMAXOI': 'BankNifty Max OI', 'BNFDSPREAD': 'BankNiftySpread', 'BNFWCOND': 'BNFCondor'}
stWeights = {'BNOMDISP1': 0.05, 'BNOMDISP2': 0.05, 'BNOMDISP3': 0.05, 'BNOMDISP4': 0.05, 'BNODMAXOI': 0.1, 'NFFDMOM15': 0.03, 'NFFDMOM14': 0.03, 'NFFDMOM10': 0.02, 'NFFDMOM12': 0.02, 'NFFDMOM06': 0.02, 'BNFDMOM15': 0.03, 'BNFDMOM14': 0.03, 'BNFDMOM12': 0.02, 'BNFDMOM04': 0.02, 'BNFDMOM13': 0.02, 'BNFDMOM06': 0.02, 'BNFDSPREAD': 0.07, 'BNFWCOND': 0.1}

FutsList = ['NFFDMOM15', 'NFFDMOM14', 'NFFDMOM10', 'NFFDMOM12', 'NFFDMOM06', 'BNFDMOM15', 'BNFDMOM14', 'BNFDMOM12', 'BNFDMOM04', 'BNFDMOM13', 'BNFDMOM06']
OptModelsWithPosition = ['BNODMAXOI', 'BNFDSPREAD', 'BNFWCOND']
def ReadFile(Opt = True):    
    file_path = filedialog.askopenfilename(title = 'Select File:')
    SheetName = 'Quantity' if Opt else 'Position'
    portData = pd_.read_excel(file_path, sheet_name = SheetName, index_col = 0, header= 0)
    print('Selected File: ', file_path.split('/')[-1])
    return portData

def GetDelta(tickersList: list, filedName: str = 'Delta') -> pd_.DataFrame:
    # Database connection using "with" statement for proper management
    with psycopg2.connect(dbname='nseinfo', user='postgres', password='admin', host='192.168.44.9', port='5432') as conn:
        sql = f'SELECT "Ticker", "Date", "{filedName}" FROM public."GREEKS_NSEFNO" WHERE "Ticker" IN %s;'
        with conn.cursor() as curs:
            curs.execute(sql, (tuple(tickersList),))
            df = pd_.DataFrame(curs.fetchall(), columns=["Ticker", "Date", filedName])
    # Use pivot to transform the DataFrame efficiently
    fullDF = df.pivot(index="Date", columns="Ticker", values=filedName)
    return fullDF.sort_index()

FinalDeltaDF = pd_.DataFrame()
for iKey in stNames.keys():
    Option = False if iKey in FutsList else True
    print(iKey, stNames[iKey])
    indata = ReadFile(Opt = Option)
    LocalDeltaDF = pd_.DataFrame()
    FutsBucket = []
    if Option:
        tempStrikeDF = pd_.DataFrame([re.match(OptionTickerRegEx, iTicker).groupdict() for iTicker in indata.columns], index = indata.columns)
        tempStrikeDF.strike = tempStrikeDF.strike.astype('float')
        StrikeDF = pd_.DataFrame([tempStrikeDF.strike.values]*len(indata.index), index=indata.index, columns=indata.columns)
        if len(tempStrikeDF[tempStrikeDF.strike ==0])>0:
            dtemp = tempStrikeDF[tempStrikeDF.strike ==0]
            for tTick in dtemp.index:
                if 'BANKNIFTY' in tTick.upper():
                    StrikeDF.loc[:, tTick] = indexData.loc[StrikeDF.index, 'AF1 INDEX']
                    FutsBucket.append(tTick)
                elif 'NIFTY' in  tTick.upper():
                    StrikeDF.loc[:, tTick] = indexData.loc[StrikeDF.index, 'NZ1 INDEX']
                    FutsBucket.append(tTick)
        if iKey in OptModelsWithPosition:
            exposureCalc = (indata[indata<0]*StrikeDF).abs().max(axis = 1)
            qtyCalc = numpy.divide(FundSize*stWeights[iKey]*(10**7), exposureCalc)
            indataAdjQty = indata.multiply(qtyCalc, axis = 0)
        else:
            indataAdjQty = indata*FundSize*stWeights[iKey]/5.0
        indataAdjQty = indataAdjQty.fillna(0).astype('int')
        delta = GetDelta(tickersList = list(indata.columns))
        delta.index = pd_.DatetimeIndex(delta.index)
        if len(FutsBucket)>0:
            for iTick in FutsBucket:
                delta[iTick] = len(delta.index)*[1.0]
        for iCol in indata.columns:
            tempQtyDF = indataAdjQty.loc[:, iCol]
            deltaDF = delta.loc[:, iCol][delta.index.isin(tempQtyDF.index)]
            tempQtyDF = tempQtyDF[tempQtyDF.index.isin(deltaDF.index)]
            deltaCal = numpy.multiply(tempQtyDF, deltaDF*StrikeDF.loc[:, iCol])
            LocalDeltaDF = pd_.concat([LocalDeltaDF, deltaCal], axis =1)            
    else:
        LocalDeltaDF = indata*FundSize*stWeights[iKey]*(10**7)    
    CalDeltaDF = pd_.DataFrame(LocalDeltaDF.sum(axis = 1))
    CalDeltaDF.columns = [iKey]
    FinalDeltaDF = pd_.concat([FinalDeltaDF, CalDeltaDF], axis = 1)

FinalDeltaDF['Total'] = FinalDeltaDF.sum(axis = 1)
for jCol in FinalDeltaDF.columns:
    FinalDeltaDF[jCol+'(%)'] = FinalDeltaDF[jCol]/(FundSize*(10**7))#stWeights[jCol]
root.withdraw()
