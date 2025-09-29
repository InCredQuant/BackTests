# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 10:51:53 2024

@author: Viren@InCred
IntrdayOptions Selling: MultiLeg/Multi Time Options Selling
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

from TradeGroup import TradeGroup

warnings.filterwarnings("ignore")



#from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter

class MultiOptions(FactoryBackTester):
    def __init__(self,data, indexName, posStartTime, posEndTime, expiryDate, rollAt, backtestName):
        FactoryBackTester.__init__(self, data)
        #self.TimeDiff = timeDiff # timedif fis in Minutes
        #self.StopLossLimit = sl
        #self.TargetLimit = target
        self.TransactionCostRate = 0.01# 1% of the Transaction
        self.TradedValue = 50000000# 5Cr.
        self.StrategyType = 'OP'
        self.PairTrades = True # True if Both PE and CE positions, used for gross exposure calculation, if PairTrades then half exposure otherwise one side exposure
        self.OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d{2}[A-Z]+\d{2})(?P<strike>\d+(\.\d+)?)(?P<option_type>[A-Z]+)'
        self.PosStartTime = posStartTime
        self.PosEndTime = posEndTime
        self.ExpiryDate = expiryDate
        self.IndexName = indexName
        self.RollAt = rollAt
        self.BacktestName = backtestName
            
    def basicdatainitialize(self):        
        self.CurrentTime = self.Close.index[0]#pandas.to_datetime('2016-06-02')
        self.LastPosChangeTime = self.CurrentTime
        #self.EndTime = pandas.to_datetime('2023-04-17')
        self.UpdateDates = self.Close.index#self.BackTestData.TradingDates#self.ExpiryDates
        #self.UpdateDates = [datetime.datetime.strptime(it, '%Y-%m-%d') for it in self.UpdateDates]
        #self.UpdateDates = [i for i in self.UpdateDates if  i >= self.CurrentTime and i <= self.EndTime]
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)# it tracks for the exit records, StopLoss, Target or anything which may be added 
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)# It tracks before applying anytype of StopLoss or target
        #self.RSIPosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.indexprice),index=self.BackTestData.indexprice.index,columns=self.BackTestData.indexprice.columns)
        self.Quantity = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.Exposure = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.trade_reg = TradeRegister()
        
        #self.Strategy = pandas.DataFrame(numpy.nan, index=self.Close.index,columns=self.Close.columns)
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True

    def StopLoss1(self):
        pdb.set_trace()
    
    def StopLossHandler(self):
        self.StopLoss1()
        
    def AddGroupPosition(self, *args):
                         
        posDict: dict, combinedSL: float = 0.20, indSecSL = 0.35, slType:str = 'pct', combinedTarget: float = 0.50, indSecTarget: float = 1.0, targetType: str = 'pct'):
        """  
        Parameters
        ----------
        posDict : dict
            This Dictionary have the ticker and quantity
        sl : number
            DESCRIPTION. The default is 0.20.-> 20%, options are either in absolute terms or in % terms
        slType : str
            DESCRIPTION.
        target : str, optional 'pct'| 'abs'
            DESCRIPTION. The default is 0.50. -> 50%, options are either in absolute terms or in % terms
        targetType : str, optional 'pct'| 'abs'
            DESCRIPTION.
        Returns
        -------
        Creates Active Positions Dictionary and append the full data to it

        """
                         
        if not hasattr(self, 'ActivePositions'):
            self.ActivePositions = {}# it will be used to store the active positions for Stop Loss and target Related purpose
        
        newGroup = TradeGroup()
        newGroup.set_entry_date(self.CurrentTime)        
        newGroup.set_combined_entry_price(sum([posDict[it]*self.Close.loc[self.CurrentTime, it] for it in posDict.keys()]))
        #nesGroup.
        
        
    def UpdateSpecificStats(self):
        try:
            indexChange = self.BackTestData.indexprice.loc[self.CurrentTime]/self.BackTestData.indexprice.loc[self.LastPosChangeTime] -1
            indexChange = abs(indexChange) - self.RollAt#(0.04/10)
        except:
            indexChange = [-1]
        if self.CurrentTime <= self.EndTime and self.CurrentTime.time() >= self.PosStartTime and self.CurrentTime.time() <= self.PosEndTime and (indexChange[0] >= 0 or self.LastPosChangeTime == self.CurrentTime or self.CurrentTime.time() == self.PosStartTime):
            self.LastPosChangeTime = self.CurrentTime            
            atm = int(self.BackTestData.indexprice.loc[self.CurrentTime]/100)*100
            
            ceStrike1 = self.IndexName+self.ExpiryDate+str(atm)+'CE'
            ceStrike2 = self.IndexName+self.ExpiryDate+str(atm+200)+'CE'
            peStrike1 = self.IndexName+self.ExpiryDate+str(atm)+'PE'
            peStrike2 = self.IndexName+self.ExpiryDate+str(atm-200)+'PE'
            
            ceStrike3 = self.IndexName+self.ExpiryDate+str(atm+300)+'CE'
            peStrike3 = self.IndexName+self.ExpiryDate+str(atm-400)+'PE'
            
            
            strikes = {ceStrike1: -50, ceStrike2: 50, peStrike1: -50, peStrike2: 50, ceStrike3: -150, peStrike3: 200}
            pdb.set_trace()
            #atm1 = int(numpy.round(atm*1.005/100)*100)
            #atm2 = int(numpy.round(atm*0.995/100)*100)      
            #allTickers = [self.IndexName+self.ExpiryDate+str(atm1)+'CE', self.IndexName+self.ExpiryDate+str(atm2)+'PE']
            #allTickers = [self.IndexName+self.ExpiryDate+str(ceStrike1)+'CE', self.IndexName+self.ExpiryDate+str(ceStrike2)+'CE', self.IndexName+self.ExpiryDate+str(peStrike1)+'PE', self.IndexName+self.ExpiryDate+str(peStrike2)+'PE']        
            tempDF = pandas.DataFrame([re.match(self.OptionTickerRegEx, iTicker).groupdict() for iTicker in strikes.keys()], index = strikes.keys())
            tempDF.strike = tempDF.strike.astype('int')
            tempDF['Quantity'] = strikes.values()
            
            
            self.CurTickersWt = tempDF#pandas.DataFrame([50 if numpy.abs(tempDF.loc[it, 'strike'] - atm) >= 400 else -25 for it in tempDF.index], index = tempDF.index, columns = ['Quantity'])
            #self.CurTickersWt = pandas.DataFrame(len(allTickers)*[-1], index = allTickers, columns = ['Quantity'])
            
            self.CurTickersWt['Price'] = self.Close.loc[self.CurrentTime, self.CurTickersWt.index]
            self.CurTickersWt['Wt'] = numpy.multiply(self.CurTickersWt.Quantity, self.CurTickersWt.Price)
            self.CurTickersWt.Wt = self.CurTickersWt.Wt/self.CurTickersWt.Wt.sum()
            #self.Weights.loc[self.CurrentTime, dtemp.index] = dtemp['Weight'].values#[i[0] for i in dtemp.values]
        elif self.CurrentTime.time() == self.PosEndTime:
            self.CapitalAllocation.loc[self.CurrentTime] = 0
            self.Position.loc[self.CurrentTime] = 0
            self.Quantity.loc[self.CurrentTime] = 0
            self.UpdateOrderBook(strategyID = self.BacktestName, options = 'y')
            self.CapitalAllocation[self.CurrentTime, self.IndexName + self.ExpiryDate+'1CASH'] = self.CurrentNAV
            self.Position.loc[self.CurrentTime, self.IndexName + self.ExpiryDate+'1CASH'] = 1
            self.Quantity.loc[self.CurrentTime, self.IndexName + self.ExpiryDate+'1CASH'] = self.CurrentNAV
            self.CurTickersWt = pandas.DataFrame()
            self.LastPosChangeTime = self.CurrentTime
        else:
            self.CurTickersWt = pandas.DataFrame()
            
    def updateCapAllocation(self):
        if len(self.CurTickersWt)> 0:
            
            self.CapitalAllocation.loc[self.CurrentTime] = 0
            self.Position.loc[self.CurrentTime] = 0
            self.Quantity.loc[self.CurrentTime] = 0
            for iTicker in self.CurTickersWt.index:#self.CurWeights.keys():
                self.CapitalAllocation.loc[self.CurrentTime,iTicker] = self.CurrentNAV*self.CurTickersWt.loc[iTicker, 'Wt']#self.CurrentNAV*self.CurWeights[iTicker]
                self.Position.loc[self.CurrentTime, iTicker] = numpy.sign(self.CurTickersWt.loc[iTicker, 'Quantity'])
                self.Quantity.loc[self.CurrentTime, iTicker] = self.CurTickersWt.loc[iTicker, 'Quantity']
                
            #print(self.CurrentTime)
            #self.UpdateOrderBook(strategyID = self.BacktestName, options = 'y')


def ReadParquetData(basePath, fromDate = datetime.date(2019, 1, 1), toDate = datetime.date(2019, 2, 15), expiry = '14FEB19', fieldName = 'Close', lowerStrike = 1000, upperStrike =  100000):
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
    #root.title('Tk test')
    #root.update()
    # file_path = filedialog.askopenfilename()
    basePathDir = filedialog.askdirectory()
    
    
    tt1 = time.time()
    INDEXNAME = 'BANKNIFTY'
    strategy = 'RollingStranglePt50PctAway'
    slippage = '1pct'
    
    
    #rollAt = 0.05/10# 50bps Change Roll
    rollList = [0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01]
    threads = 1 # How many thread are running    
    
    posStartTime = datetime.time(9, 44, 59)
    posEndTime = datetime.time(15, 4, 59)
    basePath = f'{basePathDir}/Data/{INDEXNAME}_FUT_OPT_DATA/'
    
    engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.9"}:{"5433"}/{"Data"}'#
    db_obj = DataBaseConnect(engine_url)
    if 2 == 3:#db_obj.connect():        
        mydata = MyBacktestData()
        mydata.Index = Index()
        mydata.Index.Close = db_obj.get_IntraDaySeries(tickers = [INDEXNAME + '-I.NFO', INDEXNAME+'-II.NFO'], fromDate = '2016-01-01', toDate = '2024-01-31', fieldName = 'Close')
        mydata.ExpiryDates = db_obj.getExpiryDates(expiryType= 'weekly', symbol = INDEXNAME)
        mydata.ExpiryDates = [it for it in mydata.ExpiryDates if it <= datetime.date(2024, 1, 31)]
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
    if datetime.date(2023, 3, 30) in mydata.ExpiryDates:
         mydata.ExpiryDates.remove(datetime.date(2023, 3, 30))
    
    allTradesDF = pandas.DataFrame()
    FullNAV = pandas.DataFrame()
    
    #time.sleep(40*60)
    
    for rollAt in rollList:        
        TimeDict = {}
        t0 = time.time()
        
        backtestName = f'{INDEXNAME.title()}{strategy}_Pt{str(int(rollAt*1000))}pct'
        fp = f'{basePathDir}/Backtests/Options/{INDEXNAME.title()}_{strategy}/'
        if not os.path.exists(fp):
            os.makedirs(fp)
        
        filepath = fp+backtestName+datetime.datetime.strftime(datetime.datetime.now().date(), '_%d%b%Y')
        
        allTradesDF = pandas.DataFrame()
        FullNAV = pandas.DataFrame()
             
        TimeDict['IndexData Fetched'] = time.time()-t0
        
        def _backtest_chunk(chunk):
            global allTradesDF
            global FullNAV
            #backtestName = f'{INDEXNAME.title()}RollingStraddlePt5pct'
            #fp = f'G:/Shared drives/BackTests/BackTestsResults/Options/RollingStraddle/{INDEXNAME.title()}/'
            #filepath = fp+backtestName+datetime.datetime.strftime(datetime.datetime.now().date(), '_%d%b%Y')
            for iD  in chunk:
                iExpDate = mydata.ExpiryDates[iD]
                t1 = time.time()
                nearExpiry = mydata.ExpiryDates[iD+1]                
                priceData = MyBacktestData()
                
                
                priceData.indexprice = mydata.Index.Close.loc[iExpDate+ datetime.timedelta(1): mydata.ExpiryDates[iD+1] + datetime.timedelta(1), [INDEXNAME + '-I']]
                startDate = priceData.indexprice.index[0].date()
                endDate = priceData.indexprice.index[-1].date()
                expiry = nearExpiry.strftime('%d%b%y').upper() 
                
                lowerStrike = 0.97*priceData.indexprice.min()[0]-200
                upperStrike = 1.03*priceData.indexprice.max()[0]+200
                
                priceData.Close = ReadParquetData(basePath, fromDate = startDate, toDate = endDate, expiry = expiry, fieldName = 'Close', lowerStrike = lowerStrike, upperStrike =  upperStrike)
                priceData.Close = priceData.Close.loc[[it for it in priceData.Close.index if it.time() >=  posStartTime], :]
                priceData.Close[INDEXNAME+expiry+'1CASH'] = 1
                #priceData.High = ReadParquetData(basePath, fromDate = startDate, toDate = endDate, expiry = expiry, fieldName = 'High')
                #priceData.High = priceData.High.loc[priceData.Close.index,:]
                #priceData.High[INDEXNAME+expiry+'1CASH'] = 1
            
                model = MultiOptions(priceData, indexName = INDEXNAME, posStartTime = posStartTime, posEndTime = posEndTime, expiryDate = expiry, rollAt = rollAt, backtestName = backtestName)
                model.run()               
                       
                #model.savebacktestresult(filepath+expiry+'.xlsx' , fullData = False)            
                tradeDF = model.trade_reg.get_trade_register()            
                allTradesDF = pandas.concat([allTradesDF, tradeDF], axis = 0)
                tempnav = model.NAV.resample('d', convention = 'end').last().dropna()
                tempnavChg = tempnav.pct_change()
                tempnavChg.iloc[0] = tempnav.iloc[0]/100 - 1        
                FullNAV = pandas.concat([FullNAV, tempnavChg], axis = 0)
                TimeDict[expiry] = time.time() -t1
                print(expiry, ' Done')
        
        def parallel_backtest(num_threads=1):
            chunk_size = (len(mydata.ExpiryDates)-1) // num_threads
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
        #statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
        #statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
        
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
        dailyRets = pandas.DataFrame(FullNAV.loc[:, 'NAV'])
        dailyRets.columns = [backtestName]
        if 'tempDailyRets' in locals():
            dailyRets = pandas.concat([tempDailyRets, dailyRets], axis = 1)        
        dailyRets.to_excel(fwriter, 'DailyReturns', index = True)
        dailyavgRet.to_excel(fwriter, 'DailyAvgReturns', index = True)        
        try:
            fwriter.save()
        except:
            pass
        fwriter.close()        
        #TimeDict = {'Completed': time.time()}
        print("Completed: ", time.time()-tt1)
    
