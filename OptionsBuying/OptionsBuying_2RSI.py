# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 15:11:42 2023

@author: Viren@InCred
"""
from FactoryBackTester import FactoryBackTester
import MyTechnicalLib
import pandas
import numpy
import datetime
import pdb
import matplotlib.pyplot as plt
import copy
from random import choice
import pdb
import math
import warnings
import colorama
from colorama import Fore, Style
colorama.init(autoreset=True)
warnings.filterwarnings("ignore")

from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter

class DailyTradingSignals(FactoryBackTester):
    def __init__(self,data, timeDiff =  60, rsi = 10, sl = -0.35, target = 1.0):
        FactoryBackTester.__init__(self, data)
        self.TimeDiff = timeDiff # timedif fis in Minutes
        self.rsi = rsi
        self.StopLossLimit = sl
        self.TargetLimit = target
        
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2019-03-29 09:15:00')#pandas.to_datetime('2020-04-30 09:15:00')#pandas.to_datetime('2019-03-29 09:15:00')#self.Close.index[1]#pandas.to_datetime('2022-12-01 09:15:00')#('2022-08-31')# '2013-01-07'
        self.EndTime = pandas.to_datetime('2022-12-29 15:14:00')#and i <= datetime.datetime(2022, 12, 29, 15, 14, 0)
        self.UpdateDates = list(self.Close.loc[self.CurrentTime:, :].index)#.values() backtest iterate on these dates/times, will be checking for stop loss on
        self.UpdateDates = [i for i in self.UpdateDates if i.time() <= datetime.time(15, 30, 0) and i.time() >= datetime.time(9, 15, 0) and i <= self.EndTime]
        self.GetAllRebalanceTimes(self.TimeDiff)# updating self.RebalanceTimes
        self.RebalanceTimes = [i for i in self.RebalanceTimes if i.time() <= datetime.time(15, 30, 0) and i.time() >= datetime.time(9, 15, 0) and i <= self.EndTime]
        self.IndexPrice = self.BackTestData.Index.Close
        self.IndexPriceDaily = self.IndexPrice.resample('d', convention = 'end').last().dropna()        
        self.RSI2Daily = MyTechnicalLib.GetRSI(self.IndexPriceDaily, 2)
        self.RSI2Daily.index = [i+datetime.timedelta(hours = 16) for i in self.RSI2Daily.index]
        self.RSI2 = MyTechnicalLib.GetRSI(self.IndexPrice.loc[self.RebalanceTimes, :], 2)#.loc[self.RebalanceTimes, :]
        self.SMA50 = MyTechnicalLib.MovingAverage(self.IndexPrice.loc[self.RebalanceTimes, :], 50)
        
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)# it tracks for the exit records, StopLoss, Target or anything which may be added 
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)# It tracks before applying anytype of StopLoss or target
        self.RSIPosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.indexprice),index=self.BackTestData.indexprice.index,columns=self.BackTestData.indexprice.columns)
        
        self.trade_reg = TradeRegister()
        #self.Strategy = pandas.DataFrame(numpy.nan, index=self.Close.index,columns=self.Close.columns)
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
        
    def UpdateSpecificStats(self):
        ticker = self.IndexPrice.columns[0]
        #pdb.set_trace()
        #print(self.CurrentTime)
        #if self.CurrentTime >= datetime.datetime(2019, 3, 19, 12, 15, 59):
        #     pdb.set_trace()
        #self.RSIwith50SMATrend_Options()
        self.StrikeCE = math.ceil(self.IndexPrice.loc[:self.CurrentTime].iloc[-1]/100)*100
        self.StrikePE = math.floor(self.IndexPrice.loc[:self.CurrentTime].iloc[-1]/100)*100
        self.NearExpiryTime = [i+datetime.timedelta(hours = 15, minutes = 29) for i in self.ExpiryDates if i+datetime.timedelta(hours = 15, minutes = 29) >= self.CurrentTime][0]
        MinsToExpiry = (self.NearExpiryTime - self.CurrentTime).total_seconds()/60
        if MinsToExpiry < max(60, self.TimeDiff):#self.TimeDiff
            self.NearExpiryTime = [i+datetime.timedelta(hours = 15, minutes = 29) for i in self.ExpiryDates if i+datetime.timedelta(hours = 15, minutes = 29 ) >= self.CurrentTime][1]
            self.PositionWOSL.loc[self.CurrentTime] = 0
            if hasattr(self, 'ActiveStrike'):
                del self.ActiveStrike
        self.NearExpiryDate = self.NearExpiryTime.strftime('%d%b%y').upper()

        if self.CurrentTime in self.RebalanceTimes:
            try:
                prevPosition = self.PositionWOSL.loc[self.LastTime, self.ActiveStrike]
            except:
                prevPosition = 0
                
            try:
                self.StrikeCE = math.ceil(self.IndexPrice.loc[self.CurrentTime]/100)*100
                self.StrikePE = math.floor(self.IndexPrice.loc[self.CurrentTime]/100)*100
            except:
                self.StrikeCE = math.ceil(self.IndexPrice.loc[:self.CurrentTime].iloc[-1]/100)*100
                self.StrikePE = math.floor(self.IndexPrice.loc[:self.CurrentTime].iloc[-1]/100)*100
            PrevDayRSI = self.RSI2Daily.loc[:self.CurrentTime, ticker].iloc[-1]
            CurRSI = self.RSI2.loc[self.CurrentTime, ticker]
            if prevPosition == 0:
                if CurRSI >= 100- self.rsi:#(PrevDayRSI>= 100 - self.rsi) and (CurRSI >= 100- self.rsi):
                    self.ActiveStrike = ticker+str(self.StrikePE)+'PE'+self.NearExpiryDate
                    self.PositionWOSL.loc[self.CurrentTime, self.ActiveStrike] = 1
                elif CurRSI <= self.rsi:#PrevDayRSI<= self.rsi and CurRSI <= self.rsi:#1, 3, 5, 10
                    self.ActiveStrike = ticker+str(self.StrikeCE)+'CE'+self.NearExpiryDate
                    self.PositionWOSL.loc[self.CurrentTime, self.ActiveStrike] = 1                    
            elif hasattr(self, 'ActiveStrike') and (('CE' in self.ActiveStrike and CurRSI < 50) or ('PE' in self.ActiveStrike and CurRSI > 50)):
                self.PositionWOSL.loc[self.CurrentTime, self.ActiveStrike] = 0
                
        else:
            self.PositionWOSL.loc[self.CurrentTime] = self.PositionWOSL.loc[self.LastTime]
    
    def updateCapAllocation(self):
        positionWOSL = self.PositionWOSL.loc[self.CurrentTime]
        tempDF = positionWOSL[positionWOSL != 0]
    
        for ticker in tempDF.index:
            self.StopLossTrail(ticker, self.StopLossLimit)
            self.Target(ticker, self.TargetLimit)
    
        self.Position.loc[self.CurrentTime] = self.PositionWOSL.loc[self.CurrentTime]
        for ticker in tempDF.index:
            TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL)
            checkExitPosition = self.PositionExitDF.loc[TradeTakenDate:self.CurrentTime, ticker].dropna()
            if len(checkExitPosition) >0:
                self.Position.loc[self.CurrentTime, ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, ticker] = self.PositionWOSL.loc[self.CurrentTime, ticker]

        CurrentPosition = self.Position.loc[self.CurrentTime]
        PositionWithSL = CurrentPosition[CurrentPosition != 0]
        
        if self.CurrentTime in self.RebalanceTimes:
            self.CapitalAllocation.loc[self.CurrentTime] = 0
            for iTicker in PositionWithSL.index:
                self.CapitalAllocation.loc[self.CurrentTime,iTicker]= PositionWithSL.loc[iTicker]*self.CurrentNAV            
        self.UpdateOrderBook(strategyID = '2RSI-OptionsBuying', options = 'y')
        #print(self.CurrentTime)

def mainExecution(Va = []):
    import pickle
    import os
    import time
    timeFreq = Va[0]
    rsi = Va[1]
    sl = Va[2]
    target = Va[3]
    basePath = 'G:/Shared drives/BackTests/BackTestsResults/OptionsBuying/2Period_CurrentRSI_'+str(100-rsi)+'_'+str(rsi)+'_'+str(timeFreq)+'Mins/SL'+str(abs(int(sl*100)))+'pct_'+'Target'+str(int(100*target))+'pct/'
    if not os.path.exists(basePath):
        os.makedirs(basePath)
    #pickleParent = 'Z:/Pickles/NiftyOptions/'
    pickleParent = 'G:/Shared drives/BackTests/Pickles/NiftyOptions/'
    completed = []#['NIFTY_2019-01-31.pkl']
    
    allFiles = os.listdir(pickleParent)
    allFiles.sort()
    #File = ['NIFTY_2021-07-31.pkl']
    plotResultDF = pandas.DataFrame()
    tradeDFAllTrades = pandas.DataFrame()
    t1 = time.time()
    ErrorFiles = []
    
    for iFile in allFiles:     
        if iFile in completed:
            continue
        try:
            t2 = time.time()
            #print(iFile.replace('.pkl', ''), 'started', sep = ' ')
            dataFile = os.path.join(pickleParent, iFile)#'Z:/Pickles/NiftyOptions_Dec22_16Jan-2023.pkl'
            f = open(dataFile, 'rb')
            mydata = pickle.load(f)
            f.close()
            
            a = DailyTradingSignals(mydata, timeDiff = timeFreq, rsi = rsi, sl = sl, target = target)#timeDiff: Mins
            a.run()
            #a.ResultFrameWithIndex()        
            
            ticker = iFile.split('_')[0]
            fileDate = iFile.split('_')[1].replace('.pkl', '')
            startDate = datetime.datetime.strptime(fileDate, '%Y-%m-%d')
            
            backtestName = ticker+'_'+datetime.datetime.strftime(startDate, '%b%Y')
            filepath = basePath+backtestName+'.xlsx'
            
            a.savebacktestresult(filepath)
            #backtestName = backtestName.replace(', ', '_').replace(' ', '_')
            #navData = a.PlotResult.dropna()
            
            tradeDF = a.trade_reg.get_trade_register()
            filepath = basePath+backtestName+'_TradesRegister.xlsx'#_TradesRegister
            stats_obj = Stats(tradeDF)
            statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
            statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
            statsStrategyDF = stats_obj.create_stats(filter_by = Filter.STRATEGY_ID)
            
            writer = pandas.ExcelWriter(filepath)
            tradeDF.to_excel(writer,'Trades')
            statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
            statsStrategyDF.transpose().to_excel(writer,'Stats-Strategy')
            statsPositionDF.to_excel(writer,'Stats-Position') 
            writer.save()
            writer.close()
            
            tradeDFAllTrades = pandas.concat([tradeDFAllTrades, tradeDF], axis = 0)
            try:
                plotResultDF = pandas.concat([plotResultDF, a.PlotResult.pct_change()], axis = 0)
            except:
                pass
            
            t3 = time.time()
            print(Fore.GREEN + Style.BRIGHT + iFile.replace('.pkl', ''), round((t3-t2)/60, 2), 'Mins', sep = ' ')
            completed.append(iFile)
        except:
            ErrorFiles.append(iFile)
            print(Fore.RED + Style.BRIGHT + iFile.replace('.pkl', '')+ ' Error', sep = ' ')
            #print(iFile.replace('.pkl', ''), 'Error', sep = ' ')


    plotResultDF = plotResultDF.sort_index()
    plotResultNAV = (1+plotResultDF)
    plotResultNAV.iloc[0] = 100
    plotResultNAV = plotResultNAV.cumprod()
    
    stats_obj = Stats(tradeDFAllTrades)
    statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
    statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
    statsStrategyDF = stats_obj.create_stats(filter_by = Filter.STRATEGY_ID)
    
    writer = pandas.ExcelWriter(basePath+'FullReport_TradesRegister.xlsx')
    tradeDFAllTrades.to_excel(writer,'Trades')
    statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
    statsStrategyDF.transpose().to_excel(writer,'Stats-Strategy')
    statsPositionDF.to_excel(writer,'Stats-Position')
    plotResultNAV.resample('1Min', origin = 'start').last().dropna().to_excel(writer,'NAV')    
    writer.save()
    writer.close()

    f = open( basePath+'NAV.pkl', 'wb')
    pickle.dump({'returns': plotResultDF, 'NAV' : plotResultNAV}, f)
    f.close()
    
    t4 = time.time()
    print('Completed in:', round((t4-t1)/60, 2), 'Mins', sep = ' ')
            
if __name__=='__main__':
    import pickle
    #basePath = 'Z:/BacktestsResults/OptionsBuying/'
    #timeFreq = 60 # in Minutes
    #from multiprocessing import Process
    FreqRange = [125, 75, 60, 30]
    SLRange = [-0.35, -0.5]
    TargetRange = [1.0, 2.0, 5.0]
    RSIRange = [1, 2, 3, 5, 10]# 1, 2, 3, 5
    #basePath = 'G:/Shared drives/BackTests/BackTestsResults/OptionsBuying/2RSI_97_3_60Mins/SL35_Target100/'
    
    VarList = []
    for timeFreq in FreqRange:
        for rsi in RSIRange:
            for sl in SLRange:
                for target in TargetRange:
                    VarList.append([timeFreq, rsi, sl, target])
                    
    # instantiating process with arguments
    CompletedList = []
    for iVar in VarList:
        mainExecution(Va = iVar)
        CompletedList.append(iVar)


'''    
VarList = []
for timeFreq in FreqRange:
    for rsi in RSIRange:
        for sl in SLRange:
            for target in TargetRange:
                VarList.append([timeFreq, rsi, sl, target])

number_of_task = len(VarList)
number_of_processes = 8
procs = []

# instantiating process with arguments
for iVar in VarList:
    #print(iVar)
    proc = Process(target=mainExecution, args=(iVar,))
    procs.append(proc)
    proc.start()

for proc in procs:
    proc.join()

'''
