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
import matplotlib.pyplot as plt
#import copy
#from random import choice
import pdb
import math
import warnings
import colorama
#from colorama import Fore, Style
colorama.init(autoreset=True)

import os
import time
#import sqlite3
from GetData import *
import MyTechnicalLib
import math

warnings.filterwarnings("ignore")

#from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter

class MACDOptionsTradingSignals(FactoryBackTester):
    def __init__(self,data, timeDiff =  60, sl = -1, target = 5.0):
        FactoryBackTester.__init__(self, data)
        self.TimeDiff = timeDiff # timedif fis in Minutes
        self.StopLossLimit = sl
        self.TargetLimit = target        
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2019-03-29 09:15:00')#pandas.to_datetime('2020-04-30 09:15:00')#pandas.to_datetime('2019-03-29 09:15:00')#self.Close.index[1]#pandas.to_datetime('2022-12-01 09:15:00')#('2022-08-31')# '2013-01-07'
        self.EndTime = pandas.to_datetime('2022-12-29 15:14:00')#and i <= datetime.datetime(2022, 12, 29, 15, 14, 0)
        self.UpdateDates = list(self.Close.loc[self.CurrentTime:, :].index)#.values() backtest iterate on these dates/times, will be checking for stop loss on
        self.UpdateDates = [i for i in self.UpdateDates if i.time() <= datetime.time(15, 30, 0) and i.time() >= datetime.time(9, 15, 0) and i <= self.EndTime]
        self.GetAllRebalanceTimes(self.TimeDiff)# updating self.RebalanceTimes
        self.RebalanceTimes = [i for i in self.RebalanceTimes if i.time() <= datetime.time(15, 30, 0) and i.time() >= datetime.time(9, 15, 0) and i <= self.EndTime]
        #pdb.set_trace()
        #self.TransactionCostRate = 0.000#2# Total 2 bps Transaction Charges
        self.IndexPrice = self.BackTestData.Index.Close
        self.IndexPriceDaily = self.IndexPrice.resample('d', convention = 'end').last().dropna()
        
        self.MACD, self.MACDSignal = MyTechnicalLib.MACD(self.IndexPrice)
        self.MACDDiff = numpy.subtract(self.MACD, self.MACDSignal)
        #pdb.set_trace()
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)# it tracks for the exit records, StopLoss, Target or anything which may be added 
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)# It tracks before applying anytype of StopLoss or target
        #self.RSIPosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.indexprice),index=self.BackTestData.indexprice.index,columns=self.BackTestData.indexprice.columns)
        
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
        # if self.CurrentTime >= datetime.datetime(2020, 4, 30, 15, 15, 0):
        #       pdb.set_trace()
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
            
            CurMACDDiff = self.MACDDiff.loc[self.CurrentTime, ticker]
            if CurMACDDiff > 0:
                self.ActiveStrike = ticker+str(self.StrikeCE)+'CE'+self.NearExpiryDate
                self.PositionWOSL.loc[self.CurrentTime, self.ActiveStrike] = 1
            elif CurMACDDiff < 0:
                self.ActiveStrike = ticker+str(self.StrikePE)+'PE'+self.NearExpiryDate
                self.PositionWOSL.loc[self.CurrentTime, self.ActiveStrike] = 1                    
            #elif hasattr(self, 'ActiveStrike') and (('CE' in self.ActiveStrike and CurMACDDiff < 0) or ('PE' in self.ActiveStrike and CurMACDDiff > 0)):
            #    self.PositionWOSL.loc[self.CurrentTime, self.ActiveStrike] = 0
                
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
            #print(self.CurrentTime)

        self.UpdateOrderBook(strategyID = 'NiftyHourlyMACD', options = 'y')
        

if __name__=='__main__':
    #import pickle
    #import os
    #import time
    import pickle
    
    
    #mydata = MyBacktestData()
    #mydata.Index = Index()
    
    pickleFile = os.path.join('G:/Shared drives/BackTests/Pickles/NiftyOptions/', 'NIFTY_5Mins_1otm_Mar19_Dec22.pkl')    
    if os.path.exists(pickleFile):
        f = open(pickleFile, 'rb')
        mydata = pickle.load(f)
        f.close()
    else:
        print('Data Error! Working on Data.')
        mydata = BuildOptionsData()
        f = open(pickleFile, 'wb')
        try:
            pickle.dump(mydata, f)
        except:
            import dill as pickle
            pickle.dump(mydata, f)
        f.close()
    
    SignalTime = 60# mins
    slList = [-0.25, -0.5]
    targetList = [1.0, 1.5, 2.0, 2.5, 5.0]
    for sl in slList:
        for target in targetList:
            if sl == -0.25 and target == 1.0:
                continue
            print(sl, target, " Running")
            t11 = time.time()
            model = MACDOptionsTradingSignals(mydata, timeDiff = SignalTime, sl = sl, target = target)
            model.run() 
            
            basePath = 'G:/Shared drives/BackTests/BackTestsResults/OptionsBuying/NiftyHourlyMACD/'
            if not os.path.exists(basePath):
                os.mkdir(basePath)        
            backtestName = str(SignalTime)+'MinsMACD_SL'+str(int(abs(sl*100)))+'_Target'+str(int(abs(target*100)))+'_'#+
            filepath = basePath+backtestName+datetime.datetime.strftime(datetime.datetime.now().date(), '%d%b%Y')+'.xlsx'        
            model.savebacktestresult(filepath, fullData = False)
            
            tradeDF = model.trade_reg.get_trade_register()
            stats_obj = Stats(tradeDF)
            statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
            #statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
            #statsStrategyDF = stats_obj.create_stats(filter_by = Filter.STRATEGY_ID)
            
            writer = pandas.ExcelWriter(filepath.replace('.xlsx', '_TradeRegister.xlsx'))
            tradeDF.to_excel(writer,'Trades')
            statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
            model.PlotResult.to_excel(writer, 'NAV')
            #statsStrategyDF.transpose().to_excel(writer,'Stats-Strategy')
            #statsPositionDF.to_excel(writer,'Stats-Position') 
            writer.save()
            writer.close()
            t12 = time.time()
            print('Time Taken:', round((t12-t11)/60, 1), 'Mins', sep = ' ')
            # navData = model.PlotResult.dropna()
            # navData.plot(title = backtestName, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_NAV.jpg')
            
            # temp = navData.pct_change(252)
            # temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
            # temp = pandas.DataFrame(temp)
            # temp.columns = ['Rolling 1Yr Returns ' + backtestName]
            # temp['X-Axis'] = 0
            # temp.plot(title = backtestName, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_RR.jpg')
            
            # tmp = navData.resample('a').last()
            # tmp = tmp.pct_change()
            # tmp.index = tmp.index.year
            # tmp.plot(kind = 'bar', title = backtestName, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_Y_Plot.jpg')
            
            # tmp2 = navData.resample('q').last()
            # tmp2 = tmp2.pct_change()
            # tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
            # tmp2.plot(kind = 'bar', title = backtestName, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_Q_Plot.jpg')
