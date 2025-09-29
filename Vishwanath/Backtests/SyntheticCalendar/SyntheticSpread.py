# -*- coding: utf-8 -*-
"""
Created on Thu May  2 17:48:02 2024

@author: Viren@InCred
Positional Synthetic Futs: IF there is some Spread of Current Synthetic Weekly Futs vs Spot, then take benefit of that by taking Opposite Posiiton on the Next Week Expiry
"""

from FactoryBackTester import FactoryBackTester
import MyTechnicalLib
import pandas
import numpy
import datetime
import matplotlib.pyplot as plt
#import copy
#from random import choice
import pdb
import math
import warnings
import colorama
#from colorama import Fore, Style
colorama.init(autoreset=True)
from GetData import *
import os
import time
#import sqlite3
from GetData import *
#import MyTechnicalLib
#import math
import re

warnings.filterwarnings("ignore")



#from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter

class SyntheticCalendarSpread(FactoryBackTester):
    def __init__(self,data, indexName, posStartTime , posEndTime, expiry1 , expiry2, backtestName):
        FactoryBackTester.__init__(self, data)
        #self.TimeDiff = timeDiff # timedif fis in Minutes
        #self.StopLossLimit = sl
        #self.TargetLimit = target
        self.TransactionCostRate = 0.0#0.03/100 #0.03/100# 5 bps transaction Charges #0.01# 1% of the Transaction
        self.TradedValue = 50000000# 5Cr.
        self.StrategyType = 'OP'
        self.PairTrades = True # True if Both PE and CE positions, used for gross exposure calculation, if PairTrades then half exposure otherwise one side exposure
        self.OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d{2}[A-Z]+\d{2})(?P<strike>\d+(\.\d+)?)(?P<option_type>[A-Z]+)'
        # In the latest database Update, we have moved to Weekly Tagging 
        #self.OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d{2}[A-Z]+\d{2})(?P<strike>\d+(\.\d+)?)(?P<option_type>[A-Z]+)'
        
        self.ExpiryW1 = expiry1
        #self.ExpiryDate = expiry1
        self.ExpiryW2 = expiry2
        self.PosStartTime = posStartTime
        self.PosEndTime = posEndTime
        self.IndexName = indexName
        self.BacktestName = backtestName
        self.StrikeDiff = 200 if self.IndexName.lower() == 'nifty' else 500
        self.CurrentSyntheticPosition = 0
        self.ATMStrike = 0
        
        self.MaxQty = 150*3
        self.ShortSpread = 0.09/100
        self.LongSpread = -0.05/100
        self.Short_Pct_Diff = -0.18/100
        self.Long_pct_Diff = 0.05/100
        self.Short_Cut = 0.01/100
        self.Long_Cut = -0.01/100
        self.ChgSpread = 0.02/100
        
            
    def basicdatainitialize(self):        
        self.CurrentTime = self.Close.index[0]#pandas.to_datetime('2016-06-02')
        self.LastPosChangeTime = self.CurrentTime
        #self.EndTime = pandas.to_datetime('2023-04-17')
        self.UpdateDates = self.Close.index[self.Close.index.isin(self.BackTestData.indexpriceSpot.index)]#self.BackTestData.TradingDates#self.ExpiryDates
        #self.UpdateDates = [datetime.datetime.strptime(it, '%Y-%m-%d') for it in self.UpdateDates]
        #self.UpdateDates = [i for i in self.UpdateDates if  i >= self.CurrentTime and i <= self.EndTime]
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)# it tracks for the exit records, StopLoss, Target or anything which may be added 
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)# It tracks before applying anytype of StopLoss or target
        #self.RSIPosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.indexprice),index=self.BackTestData.indexprice.index,columns=self.BackTestData.indexprice.columns)
        self.Quantity = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.Exposure = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.SyntheticW = {}#pandas.DataFrame(numpy.zeros_like(self.Close[self.Close.columns[:1]]), index = self.Close.index, columns = ['SyntheticW1', 'SyntheticW2'])
        self.trade_reg = TradeRegister()
        self.ActivePositions = {}
        #self.Strategy = pandas.DataFrame(numpy.nan, index=self.Close.index,columns=self.Close.columns)
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
        
    def MakePositionNil(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.Position.loc[self.CurrentTime] = 0
        self.Quantity.loc[self.CurrentTime] = 0
        self.UpdateOrderBook(strategyID = self.BacktestName, options = 'y')
        self.CapitalAllocation[self.CurrentTime, self.IndexName + self.ExpiryW1+'1CASH'] = self.CurrentNAV
        self.Position.loc[self.CurrentTime, self.IndexName + self.ExpiryW1+'1CASH'] = 1
        self.Quantity.loc[self.CurrentTime, self.IndexName + self.ExpiryW1+'1CASH'] = self.CurrentNAV
        self.CurTickersWt = pandas.DataFrame()
        
    def InCreasePosition(self, quantity):
        nearStrikeCall = self.IndexName+self.ExpiryW1+str(self.ATMStrike)+'CE'
        nearStrikePut = self.IndexName+self.ExpiryW1+str(self.ATMStrike)+'PE'
        nextWkStrikeCall = self.IndexName+self.ExpiryW2+str(self.ATMStrike)+'CE'
        nextWkStrikePut = self.IndexName+self.ExpiryW2+str(self.ATMStrike)+'PE'
        allTickers = [nearStrikeCall, nearStrikePut, nextWkStrikeCall, nextWkStrikePut]
        tempDF = pandas.DataFrame([re.match(self.OptionTickerRegEx, iTicker).groupdict() for iTicker in allTickers], index = allTickers)
        tempDF.strike = tempDF.strike.astype('int')        
        if self.CurrentSyntheticPosition <0:
            tempDF.loc[nearStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikeCall] - quantity
            tempDF.loc[nearStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikePut] + quantity
            tempDF.loc[nextWkStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikeCall] + quantity
            tempDF.loc[nextWkStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikePut] - quantity        
        elif self.CurrentSyntheticPosition >0:
            tempDF.loc[nearStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikeCall] + quantity
            tempDF.loc[nearStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikePut] - quantity
            tempDF.loc[nextWkStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikeCall] - quantity
            tempDF.loc[nextWkStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikePut] + quantity            
        self.CurTickersWt = tempDF
        self.CurTickersWt['Price'] = self.Close.loc[self.CurrentTime, self.CurTickersWt.index]
        self.CurTickersWt['Wt'] = numpy.multiply(self.CurTickersWt.Quantity, self.CurTickersWt.Price)
        self.CurTickersWt.Wt = self.CurTickersWt.Wt/self.CurTickersWt.Wt.sum()
    
    def DeCreasePosition(self, quantity):
        nearStrikeCall = self.IndexName+self.ExpiryW1+str(self.ATMStrike)+'CE'
        nearStrikePut = self.IndexName+self.ExpiryW1+str(self.ATMStrike)+'PE'
        nextWkStrikeCall = self.IndexName+self.ExpiryW2+str(self.ATMStrike)+'CE'
        nextWkStrikePut = self.IndexName+self.ExpiryW2+str(self.ATMStrike)+'PE'
        allTickers = [nearStrikeCall, nearStrikePut, nextWkStrikeCall, nextWkStrikePut]
        #tempDF = pandas.DataFrame([re.match(self.OptionTickerRegEx, iTicker).groupdict() for iTicker in allTickers], index = allTickers)
        #tempDF.strike = tempDF.strike.astype('int')
        tempDF = pandas.DataFrame(self.Quantity.loc[self.CurrentTime])
        tempDF.columns = ['Quantity']
        if self.CurrentSyntheticPosition <0:
            tempDF.loc[nearStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikeCall] + quantity
            tempDF.loc[nearStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikePut] - quantity
            tempDF.loc[nextWkStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikeCall] - quantity
            tempDF.loc[nextWkStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikePut] + quantity        
        elif self.CurrentSyntheticPosition >0:
            tempDF.loc[nearStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikeCall] - quantity
            tempDF.loc[nearStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nearStrikePut] + quantity
            tempDF.loc[nextWkStrikeCall, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikeCall] + quantity
            tempDF.loc[nextWkStrikePut, 'Quantity'] = self.Quantity.loc[self.LastTime, nextWkStrikePut] - quantity         
        self.CurTickersWt = tempDF
        self.CurTickersWt['Price'] = self.Close.loc[self.CurrentTime, self.CurTickersWt.index]
        self.CurTickersWt['Wt'] = numpy.multiply(self.CurTickersWt.Quantity, self.CurTickersWt.Price)
        self.CurTickersWt.Wt = self.CurTickersWt.Wt/self.CurTickersWt.Wt.sum()
        
        
        
    def UpdateSpecificStats(self):
        if self.CurrentTime <= self.EndTime:# and self.CurrentTime.time() >= self.PosStartTime:# and self.CurrentTime.time() <= self.PosEndTime: 
            futATM = int(self.BackTestData.indexprice.loc[self.CurrentTime]/self.StrikeDiff)*self.StrikeDiff
            spotValue = self.BackTestData.indexpriceSpot.loc[self.CurrentTime][0]
            CEW1Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW1+str(futATM)+'CE']
            PEW1Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW1+str(futATM)+'PE']
            S1 = futATM + CEW1Pr - PEW1Pr
            
            CEW2Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW2+str(futATM)+'CE'] 
            PEW2Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW2+str(futATM)+'PE']            
            S2 = futATM + CEW2Pr - PEW2Pr
            self.SyntheticW[self.CurrentTime] = {'Spot': spotValue, 'S1': S1, 'S2': S2, 'CEW1': self.IndexName+self.ExpiryW1+str(futATM)+'CE', 'PEW1': self.IndexName+self.ExpiryW1+str(futATM)+'PE',  'CEW2': self.IndexName+self.ExpiryW2+str(futATM)+'CE', 'PEW2': self.IndexName+self.ExpiryW2+str(futATM)+'PE', 'CEW1P': CEW1Pr, 'PEW1P': PEW1Pr, 'CEW2P': CEW2Pr, 'PEW2P': PEW2Pr}#.loc[self.CurrentTime, 'SyntheticW1'] = S1
            #self.SyntheticW.loc[self.CurrentTime, 'SyntheticW2'] = S2
            if not (numpy.isnan(S1) or numpy.isnan(S2)):
                if self.CurrentSyntheticPosition == 0:
                    D1 = S1/spotValue -1
                    D2 = S2/spotValue -1
                    pct_Diff = D1-D2                    
                    if D1 > self.ShortSpread and pct_Diff <= self.Short_Pct_Diff:#0.04
                        #if Near Week is on premium by 9 bps and current premium wrt next is 18bps discount, then enter Short on Current Week and Long on Next Week
                        self.CurrentSyntheticPosition = -1
                        self.ATMStrike  = futATM
                        self.InCreasePosition(150)
                        self.ShortSpread += self.ChgSpread
                        self.Short_Pct_Diff -= self.ChgSpread
                    # if D1 < self.LongSpread and pct_Diff >= self.Long_pct_Diff:
                    #     #if Near Week is on discount by 5 bps and current premium wrt next is 5bps premium, then enter Long on Current Week and Short on Next Week
                    #     self.CurrentSyntheticPosition = 1
                    #     self.ATMStrike  = futATM
                    #     self.InCreasePosition(150)
                    #     self.LongSpread -= self.ChgSpread
                    #     self.Long_pct_Diff += self.ChgSpread
                    else:
                        self.CurTickersWt = pandas.DataFrame()
                        
                elif self.CurrentSyntheticPosition != 0:
                    CEW1Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW1+str(self.ATMStrike)+'CE']
                    PEW1Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW1+str(self.ATMStrike)+'PE']
                    S1 = futATM + CEW1Pr - PEW1Pr
                    
                    CEW2Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW2+str(self.ATMStrike)+'CE'] 
                    PEW2Pr = self.Close.loc[self.CurrentTime, self.IndexName+self.ExpiryW2+str(self.ATMStrike)+'PE']            
                    S2 = futATM + CEW2Pr - PEW2Pr
                    self.SyntheticW[self.CurrentTime] = {'Spot': spotValue, 'S1': S1, 'S2': S2, 'CEW1': self.IndexName+self.ExpiryW1+str(futATM)+'CE', 'PEW1': self.IndexName+self.ExpiryW1+str(futATM)+'PE',  'CEW2': self.IndexName+self.ExpiryW2+str(futATM)+'CE', 'PEW2': self.IndexName+self.ExpiryW2+str(futATM)+'PE', 'CEW1P': CEW1Pr, 'PEW1P': PEW1Pr, 'CEW2P': CEW2Pr, 'PEW2P': PEW2Pr}#.loc[self.CurrentTime, 'SyntheticW1'] = S1
                    if not (numpy.isnan(S1) or numpy.isnan(S2)):
                        D1 = S1/spotValue -1
                        D2 = S2/spotValue -1
                        pct_Diff = D1-D2
                        if self.CurrentSyntheticPosition < 0 and D1 > self.ShortSpread and pct_Diff <= self.Short_Pct_Diff:#0.04
                            #if Near Week is on premium by 9 bps and current premium wrt next is 18bps discount, then enter Short on Current Week and Long on Next Week
                            if self.Quantity.loc[self.LastTime].abs().max() < self.MaxQty:
                                self.CurrentSyntheticPosition -= 1
                                self.InCreasePosition(150)
                                self.ShortSpread += self.ChgSpread
                                self.Short_Pct_Diff -= self.ChgSpread
                                self.Short_Cut += self.ChgSpread
                        
                        if self.CurrentSyntheticPosition < 0 and D1 < self.Short_Cut:
                            #current week premium comes below 3bps
                            self.CurrentSyntheticPosition += 1
                            self.DeCreasePosition(150)
                            self.Short_Cut -= self.ChgSpread
                            #self.MakePositionNil()
                            
                        # if self.CurrentSyntheticPosition >0 and  D1 < self.LongSpread and pct_Diff >= self.Long_pct_Diff:
                        #     if self.Quantity.loc[self.LastTime].abs().max() < self.MaxQty:
                        #         self.CurrentSyntheticPosition += 1
                        #         self.InCreasePosition(150)
                        #         self.LongSpread -= self.ChgSpread
                        #         self.Long_pct_Diff += self.ChgSpread
                        #         self.Long_Cut -= self.ChgSpread
                            
                        # if self.CurrentSyntheticPosition >0  and D1 > self.Long_Cut:
                        #     self.CurrentSyntheticPosition -= 1
                        #     self.DeCreasePosition(150)
                        #     self.Long_Cut += self.ChgSpread
                        #     self.MakePositionNil()
                    else:
                        self.CurTickersWt = pandas.DataFrame()
        else:
            self.CurTickersWt = pandas.DataFrame()
        if self.CurrentTime.date() == datetime.datetime.strptime(self.ExpiryW1, '%d%b%y').date() and self.CurrentTime.time() >= datetime.time(15, 9, 59):
            self.MakePositionNil()
            
    def updateCapAllocation(self):
        if hasattr(self, "CurTickersWt") and len(self.CurTickersWt)> 0:
            self.CapitalAllocation.loc[self.CurrentTime] = 0
            self.Position.loc[self.CurrentTime] = 0
            self.Quantity.loc[self.CurrentTime] = 0
            for iTicker in self.CurTickersWt.index:#self.CurWeights.keys():
                self.CapitalAllocation.loc[self.CurrentTime,iTicker] = self.CurrentNAV*self.CurTickersWt.loc[iTicker, 'Wt']#self.CurrentNAV*self.CurWeights[iTicker]
                self.Position.loc[self.CurrentTime, iTicker] = numpy.sign(self.CurTickersWt.loc[iTicker, 'Quantity'])
                self.Quantity.loc[self.CurrentTime, iTicker] = self.CurTickersWt.loc[iTicker, 'Quantity']                
            #print(self.CurrentTime)
            self.UpdateOrderBook(strategyID = self.BacktestName, options = 'y')


def ReadParquetData(basePath, fromDate = datetime.date(2019, 1, 1), toDate = datetime.date(2019, 2, 15), expiry = '14FEB19', fieldName = 'Close', lowerStrike = 1000, upperStrike =  100000, inMultiple = 100):
    #import os
    monthList = [fromDate.strftime('%Y-%m')]
    monthList.append(toDate.strftime('%Y-%m'))
    monthList = list(set(monthList))
    allFiles = os.listdir(basePath)
    files = []
    for iMonth in monthList:
        files.extend([it for it in allFiles if iMonth in it])
        
    finalDF = pandas.DataFrame()  
    for iFile in files:
        inData = pandas.read_parquet(os.path.join(basePath, iFile))
        inData = inData[inData.ExpiryDate == expiry]
        inData.index = pandas.to_datetime(inData['Date']) + pandas.to_timedelta(inData['Time'])
        inData.StrikePrice = inData.StrikePrice.astype('int')
        inData = inData[(inData.StrikePrice >= lowerStrike) &(inData.StrikePrice <= upperStrike)]
        inData = inData[inData.StrikePrice % inMultiple == 0]
        gg = inData.groupby('Ticker')        
        temp_dfs = [pandas.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper().replace('.NFO', '')}) for grp in gg.groups.keys()]
        df = pandas.concat(temp_dfs, axis=1)
        df = df.loc[fromDate: toDate + datetime.timedelta(1), :]
        df = df.sort_index()
        finalDF = pandas.concat([finalDF, df], axis = 0)
    finalDF = finalDF.sort_index()
    return finalDF

if __name__=='__main__':    
    from intraday_db_postgres import DataBaseConnect
    import concurrent.futures
    import pickle
    from openpyxl import load_workbook
    
    import tkinter as tk
    from tkinter import filedialog

    root = tk.Tk()
    root.withdraw()
    root.title('Tk test')
    root.update()
    #file_path = filedialog.askopenfilename()
    basePathDir = filedialog.askdirectory()
    
    tt1 = time.time()
    INDEXNAME = 'BANKNIFTY'
    slippage = ''#'1pct'
    strategy = f'SyntheticCalendarSpread_{INDEXNAME.title()}_OnlyShortQtyCap_V2'
    #SyntheticSpread(FactoryBackTester):
    threads = 4 # How many thread are running
    posStartTime = datetime.time(9, 15, 59)
    posEndTime = datetime.time(15, 14, 59)
    basePath = f'{basePathDir}/Data/{INDEXNAME}_FUT_OPT_DATA/'
    
    #portConn = psycopg2.connect(dbname= 'data', user= 'postgres', password='postgres', host='192.168.44.4', port='5432')
    #portCurs = portConn.cursor()
    
    engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'#
    db_obj = DataBaseConnect(engine_url)
    #time.sleep(60*60*4)
    if 2==3:#db_obj.connect():
        mydata = MyBacktestData()
        mydata.Index = Index()
        mydata.Index.Close = db_obj.get_IntraDaySeries(tickers = [INDEXNAME + '-I.NFO', INDEXNAME+'-II.NFO'], fromDate = '2016-01-01', toDate = '2024-04-30', fieldName = 'Close')
        mydata.Index.Spot = db_obj.get_IntraDaySeries(tickers = [INDEXNAME], fromDate = '2016-01-01', toDate = '2024-04-30', fieldName = 'Close', spot = True)
        mydata.ExpiryDates = db_obj.getExpiryDates(expiryType= 'weekly', symbol = INDEXNAME)
        mydata.ExpiryDates = [it for it in mydata.ExpiryDates if it <= datetime.date(2024, 4, 30)]
        if datetime.date(2023, 6, 29) in mydata.ExpiryDates:
             mydata.ExpiryDates.remove(datetime.date(2023, 6, 29))
             mydata.ExpiryDates.append(datetime.date(2023, 6, 28))
             mydata.ExpiryDates.sort()             
        if datetime.date(2023, 3, 30) in mydata.ExpiryDates:
            mydata.ExpiryDates.remove(datetime.date(2023, 3, 30))
             
        fpkl =  open(f'{basePath}{INDEXNAME.lower()}.pkl', 'wb')
        pickle.dump(mydata, fpkl)
        fpkl.close()
    else:        
        fpkl = open(f'{basePath}{INDEXNAME.lower()}.pkl', 'rb')
        mydata = pickle.load(fpkl)
        fpkl.close()
    
    removeDates = ['2023-03-30', '2024-03-28', '2024-04-25']
    removeDates = [datetime.datetime.strptime(it, '%Y-%m-%d').date() for it in removeDates]
            
    mydata.ExpiryDates = [iDate for iDate in mydata.ExpiryDates if iDate >= datetime.date(2019, 1, 1) and iDate not in removeDates]
    allTradesDF = pandas.DataFrame()
    FullNAV = pandas.DataFrame()
    SyntheticDF = pandas.DataFrame()
    
    rollList = [0.0]
    for rollAt in rollList:
        TimeDict = {}
        t0 = time.time()
        
        backtestName = f'{INDEXNAME.title()}{strategy}_'
        fp = f'{basePathDir}/Backtests/Options/{strategy}/'
        if not os.path.exists(fp):
            os.makedirs(fp)        
        filepath = fp+backtestName+datetime.datetime.strftime(datetime.datetime.now().date(), '_%d%b%Y')        
        allTradesDF = pandas.DataFrame()
        FullNAV = pandas.DataFrame()             
        TimeDict['IndexData Fetched'] = time.time()-t0
        def _backtest_chunk(chunk):            
            global allTradesDF
            global FullNAV
            global SyntheticDF
            
            for iD  in chunk:
                iExpDate = mydata.ExpiryDates[iD]
                t1 = time.time()
                nearExpiry = mydata.ExpiryDates[iD+1]
                nextExpiry = mydata.ExpiryDates[iD+2]
                if INDEXNAME == 'BANKNIFTY' and  nextExpiry == datetime.date(2023, 9, 6):
                    nextExpiry = datetime.date(2023, 9, 7)
                priceData = MyBacktestData()
                
                priceData.indexprice = mydata.Index.Close.loc[iExpDate+ datetime.timedelta(1): mydata.ExpiryDates[iD+1] + datetime.timedelta(1), INDEXNAME + '-I']
                priceData.indexpriceSpot = mydata.Index.Spot
                startDate = priceData.indexprice.index[0].date()
                endDate = priceData.indexprice.index[-1].date()
                
                expiryW1 = nearExpiry.strftime('%d%b%y').upper()
                expiryW2 = nextExpiry.strftime('%d%b%y').upper() 
                
                lowerStrike = 0.90*priceData.indexprice.min()
                upperStrike = 1.10*priceData.indexprice.max()                
                
                StrikeSelection = 200 if INDEXNAME.lower() in 'nifty' else 500
                closeW1 = ReadParquetData(basePath, fromDate = startDate, toDate = endDate, expiry = expiryW1, fieldName = 'Close', lowerStrike = lowerStrike, upperStrike =  upperStrike, inMultiple = StrikeSelection)
                closeW2 = ReadParquetData(basePath, fromDate = startDate, toDate = endDate, expiry = expiryW2, fieldName = 'Close', lowerStrike = lowerStrike, upperStrike =  upperStrike, inMultiple = StrikeSelection)
                
                priceData.Close = pandas.concat([closeW1, closeW2], axis = 1)
                priceData.Close = priceData.Close.loc[[it for it in priceData.Close.index if it.time() >=  posStartTime], :]
                priceData.Close[INDEXNAME+expiryW1+'1CASH'] = 1
                
                stTime = datetime.time(9, 16, 59)
                endTime = datetime.time(15, 27, 59)
                
                priceData.indexprice = priceData.indexprice[priceData.indexprice.index.time >= stTime]
                priceData.indexprice = priceData.indexprice[priceData.indexprice.index.time <= endTime]
                
                priceData.indexpriceSpot = priceData.indexpriceSpot[priceData.indexpriceSpot.index.time >= stTime]
                priceData.indexpriceSpot = priceData.indexpriceSpot[priceData.indexpriceSpot.index.time <= endTime]
                
                priceData.Close = priceData.Close[priceData.Close.index.time >= stTime]
                priceData.Close = priceData.Close[priceData.Close.index.time <= endTime]
                
                model = SyntheticCalendarSpread(priceData, indexName = INDEXNAME, posStartTime = posStartTime, posEndTime = posEndTime, expiry1 = expiryW1, expiry2 = expiryW2, backtestName = backtestName)
                model.run()   
                
                model.savebacktestresult(f'{filepath}_{nearExpiry}.xlsx' , fullData = True)         
                tradeDF = model.trade_reg.get_trade_register()            
                allTradesDF = pandas.concat([allTradesDF, tradeDF], axis = 0)
                tempnav = model.NAV.resample('d', convention = 'end').last().dropna()
                tempnavChg = tempnav.pct_change()
                tempnavChg.iloc[0] = tempnav.iloc[0]/100 - 1        
                FullNAV = pandas.concat([FullNAV, tempnavChg], axis = 0)
                
                SyntheticDF = pandas.concat([SyntheticDF, pandas.DataFrame(model.SyntheticW).transpose()], axis = 0)
                
                TimeDict[expiryW1] = time.time() -t1
                print(expiryW1)#, int(rollAt*10000), "BPS")
                
        def parallel_backtest(num_threads=1):            
            chunk_size = (len(mydata.ExpiryDates)-2) // num_threads
            dataThread = range(0, len(mydata.ExpiryDates)-1, chunk_size)
            dataThread = [it for it in dataThread]
            if len(dataThread) < num_threads +1:
                dataThread.append(len(mydata.ExpiryDates)-1)
            chunks = [range(dataThread[it], dataThread[it+1]) for it in range(len(dataThread)-1)]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(_backtest_chunk, chunks)
            return results
    
        parallel_backtest(num_threads=threads)
        stats_obj = Stats(allTradesDF)
    
    FullNAV = FullNAV.sort_index()
    
    FullNAV['Expiry'] = [it if it.date() in mydata.ExpiryDates else numpy.nan for it in FullNAV .index]
    FullNAV.Expiry = FullNAV.Expiry.fillna(method = 'bfill')
    FullNAV['Date'] = FullNAV.index
    FullNAV['DaysToExpiry'] = [((FullNAV['Date']>=it)&(FullNAV['Date']<= FullNAV.loc[it, 'Expiry'])).sum()-1 for it in FullNAV.index]        
    #statsStrategyDF = stats_obj.create_stats(filter_by = Filter.STRATEGY_ID)
    #filepath = fp+backtestName+datetime.datetime.strftime(datetime.datetime.now().date(), '_%d%b%Y')
       
    writer = pandas.ExcelWriter(f'{filepath}_{slippage}_Slippage_TradeRegister.xlsx')#.replace('.xlsx', '_TradeRegister.xlsx'))
    allTradesDF.to_excel(writer,'Trades')
    #statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
    #statsStrategyDF.transpose().to_excel(writer,'Stats-Strategy')
    #statsPositionDF.to_excel(writer,'Stats-Position') 
    FullNAV.to_excel(writer,  'DailyReturns')
    pandas.DataFrame(TimeDict, index = ['Time']).transpose().to_excel(writer,  'RunningTime')
    try:
        writer.save()
    except:
        pass
    writer.close()        
    resultsFile = f'{basePathDir}/Backtests/Options/{INDEXNAME.title()}_{strategy}.xlsx'        
    try:
        tempDailyRets = pandas.read_excel(resultsFile, sheet_name = 'DailyReturns', index_col = [0])
        tempDailyAvgRets = pandas.read_excel(resultsFile, sheet_name = 'DailyAvgReturns', index_col = [0])
        book = load_workbook(resultsFile)
        fwriter = pandas.ExcelWriter(resultsFile, engine = 'openpyxl')
        fwriter.book = book
        if os.path.exists(resultsFile):
            os.remove(resultsFile)
                    
    except:
        fwriter = pandas.ExcelWriter(resultsFile, engine = 'openpyxl')
    
    rets = FullNAV.groupby('DaysToExpiry').mean('NAV')
    rets.columns = [backtestName]
    cnt = pandas.DataFrame(FullNAV.groupby('DaysToExpiry').count().loc[:, 'Date'])
    cnt.columns = [backtestName+'-Count']
    dailyavgRet = pandas.concat([rets, cnt], axis = 1)
    if 'tempDailyAvgRets' in locals():
        dailyavgRet = pandas.concat([tempDailyAvgRets, dailyavgRet], axis = 1)
    try:        
        dailyRets = pandas.DataFrame(FullNAV.loc[:, 'NAV'])
        dailyRets.columns = [backtestName]
        if 'tempDailyRets' in locals():
            dailyRets = pandas.concat([tempDailyRets, dailyRets], axis = 1)        
        dailyRets.to_excel(fwriter, 'DailyReturns', index = True)
    except:
        pass
    dailyavgRet.to_excel(fwriter, 'DailyAvgReturns', index = True)
    SyntheticDF.to_csv(resultsFile.replace('.xlsx', '.csv'))
    try:
        fwriter.save()
    except:
        pass
    fwriter.close()        
    #TimeDict = {'Completed': time.time()}
    print("Completed: ", time.time()-tt1)
    
