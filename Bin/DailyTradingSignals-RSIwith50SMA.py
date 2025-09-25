# -*- coding: utf-8 -*-
"""
Created on Thu Oct  6 11:44:53 2022

@author: Viren@Incred
Testing RSI(2) with 50SMA:
    1. Close Price > 50SMA => 1, Close Price < 50SMA => -1
    2. IF 2RSI> 90, -1, else if 2RSI < 10, 1
    
    Based on both positions:
        if both signal gives same positions then same position (either Long or Short)
        otherwise-> IF Previous Position is not same as Current SMA Position or (Current RSI Position is not neutral and Last Position is different than Current RSI Position) then Neutral
        Otherwise continure same as previous position

Entry: When both Gives same signal,
Exit-> when either SMA gives opposite signal or RSISignal gives opposite trade signal
"""
#import time
from FactoryBackTester import FactoryBackTester
#from FactoryBackTester import NameDecorator
#from collections import namedtuple
import MyTechnicalLib
import pandas
import numpy
#import sklearn.ensemble
#from scipy.stats import linregress
import datetime
import pdb
import matplotlib.pyplot as plt
import copy
#from scipy import stats
import pdb
from random import choice
import warnings
warnings.filterwarnings("ignore")

from order_base import Order, Position, OptionType, Segment

from trade_register import TradeRegister
from stats import Stats, Filter

import warnings
warnings.filterwarnings("ignore")

class ModelPortRSIwithSMA(FactoryBackTester):
    def __init__(self,data, ticker):
        FactoryBackTester.__init__(self,data)
        self.ticker = ticker
        
        
    def basicdatainitialize(self):
        ClosePrice = self.BackTestData.DMA50.loc[:, self.ticker].dropna()
        self.CurrentTime = max(ClosePrice.index[10] ,pandas.to_datetime('2009-12-31'))#pandas.to_datetime('2004-12-31')# '2013-01-07'
        #self.GetAllUpdateDates(self.Days)
        #self.CurrentTime = self.BackTestData.ExpiryDates[30]
        self.UpdateDates = list(self.BackTestData.DMA50.index)#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.00#15# 15 bps total, assuming 5bps transaction charges and 10 bps slippages
        self.RSIPosition=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.SMAPosition=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.StartingPosition = -1        
        self.trade_reg = TradeRegister()
        self.order = Order()
        self.StopLossLimit = -0.10 # 5%
        #self.TradeDetails = []
        #self.RSIPosition.loc[self.LastTime, self.ticker] = -1
        #pdb.set_trace()

    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.BackTestData.Close.loc[self.CurrentTime]
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True

    def UpdateSpecificStats(self):
        # if self.CurrentTime in self.BackTestData.Close.loc[datetime.datetime(2009, 1, 22):, self.ticker].index:
        #     pdb.set_trace()
        if numpy.isnan(self.BackTestData.DMA50.loc[self.CurrentTime, self.ticker]):
            self.order.symbol = self.ticker
            self.order.segment = Segment.EQ
            self.order.quantity = 1
            if self.Position.loc[self.LastTime, self.ticker] in [1, -1]:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
                self.EndOrderPosition(self.LastTime)
                next
            
        else:
            self.RSI = self.BackTestData.RSI2.loc[:, self.ticker].dropna()
            self.DMA = self.BackTestData.DMA50.loc[:, self.ticker].dropna()
            
            if self.CurrentPrice.loc[self.ticker] > self.DMA.loc[self.CurrentTime]:
                self.SMAPosition.loc[self.CurrentTime, self.ticker] = 1
            elif self.CurrentPrice.loc[self.ticker] <= self.DMA.loc[self.CurrentTime]:
                self.SMAPosition.loc[self.CurrentTime, self.ticker] = -1
            
            if self.RSI.loc[:self.CurrentTime].iloc[-1] < 10:
                self.RSIPosition.loc[self.CurrentTime, self.ticker] = 1                
            elif self.RSI.loc[:self.CurrentTime].iloc[-1] > 90:
                self.RSIPosition.loc[self.CurrentTime, self.ticker] = -1
            
            
        
    def DetectPositionStartTime(self):
        tempList = self.Position.loc[:self.CurrentTime, self.ticker]
        tempList = tempList.sort_index(ascending = False)
        iDate = tempList.index[0]
        self.TradeTakenTime = None
        for iTime in list(tempList.index[:90]):
            if tempList.loc[iTime] != tempList.iloc[0]:
                self.TradeTakenTime = iDate
                break
            else:
                iDate = iTime
    
    def EndOrderPosition(self, endTime):
        self.order.exit_date = self.CurrentTime
        self.order.exit_price = self.BackTestData.Close.loc[endTime, self.ticker]
        self.trade_reg.append_trade(self.order)
        self.order = Order()
        self.order.symbol = self.ticker
        self.order.segment = Segment.EQ
        self.order.quantity = 1
    
    def StartOrderPosition(self, startPositon = choice([Position.LONG, Position.SHORT])):
        self.order = Order()
        self.order.symbol = self.ticker
        self.order.segment = Segment.EQ
        self.order.quantity = 1
        self.order.entry_date = self.CurrentTime
        self.order.entry_price = self.BackTestData.Close.loc[self.CurrentTime, self.ticker]
        self.order.position = startPositon         
    
    def populateOrderPosition(self):
        # generating Trade Log for stats Calculations
        self.order.symbol = self.ticker
        self.order.segment = Segment.EQ
        self.order.quantity = 1
        if self.Position.loc[self.LastTime, self.ticker] == 0:
            if self.Position.loc[self.CurrentTime, self.ticker] == 1:
                self.StartOrderPosition(Position.LONG)            
            elif self.Position.loc[self.CurrentTime, self.ticker] == -1:
                self.StartOrderPosition(Position.SHORT)          
            
        elif self.Position.loc[self.LastTime, self.ticker] == 1:
            if self.Position.loc[self.CurrentTime, self.ticker] == 0:
                self.EndOrderPosition(self.CurrentTime)
            elif self.Position.loc[self.CurrentTime, self.ticker] == -1:
                self.EndOrderPosition(self.CurrentTime)
                self.StartOrderPosition(Position.SHORT)
        
        elif self.Position.loc[self.LastTime, self.ticker] == -1:
            if self.Position.loc[self.CurrentTime, self.ticker] == 0:
                self.EndOrderPosition(self.CurrentTime) 
            elif self.Position.loc[self.CurrentTime, self.ticker] == 1:
                self.EndOrderPosition(self.CurrentTime)
                self.StartOrderPosition(Position.LONG)
        
        
    def StopLoss(self):
        self.DetectPositionStartTime()
        try:
            rets = (self.BackTestData.Close.loc[self.CurrentTime, self.ticker]/self.BackTestData.Close.loc[self.TradeTakenTime, self.ticker]) -1
            if rets*self.Position.loc[self.CurrentTime, self.ticker] < self.StopLossLimit:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
            #self.EndOrderPosition(self.CurrentTime)
        except:
            pass
    
    # Based on both positions:
    #     if both signal gives same positions then same position (either Long or Short)
    #     otherwise-> IF Previous Position is not same as Current SMA Position or (Current RSI Position is not neutral and Last Position is different than Current RSI Position) then Neutral
    #     Otherwise continue same as previous position
        
        
    def updateCapAllocation(self):
        if self.SMAPosition.loc[self.CurrentTime, self.ticker] == self.RSIPosition.loc[self.CurrentTime, self.ticker]:
            self.Position.loc[self.CurrentTime, self.ticker] = self.RSIPosition.loc[self.CurrentTime, self.ticker]
        else:
            #otherwise-> IF Previous Position is not same as Current SMA Position or (Current RSI Position is not neutral and Last Position is different than Current RSI Position) then Neutral
            cond1 = self.Position.loc[self.LastTime, self.ticker] != self.SMAPosition.loc[self.CurrentTime, self.ticker]
            cond2 = self.RSIPosition.loc[self.CurrentTime, self.ticker] != 0 and (self.Position.loc[self.LastTime, self.ticker] != self.RSIPosition.loc[self.CurrentTime, self.ticker])
            if cond1 or cond2:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, self.ticker] = self.Position.loc[self.LastTime, self.ticker]
            
        # Stop Loss- -10%
        # if self.Position.loc[self.CurrentTime, self.ticker] in [-1, 1]:
        #     self.StopLoss()    
        self.populateOrderPosition()    
            
        self.CurrentPosition = self.Position.loc[self.CurrentTime, self.ticker]
        if self.CurrentPosition == 1:
            self.CapitalAllocation.loc[self.CurrentTime,self.ticker]= 1.0*self.CurrentNAV#/self.TotalPosition
        elif self.CurrentPosition == -1:
            self.CapitalAllocation.loc[self.CurrentTime,self.ticker]= -1.0*self.CurrentNAV#/self.TotalPosition
        else:
            self.CapitalAllocation.loc[self.CurrentTime,self.ticker]= 0
        #print(self.CurrentTime)

                
if __name__=='__main__':
    import pickle
    dirPath = 'Z:/'#'G:/Shared drives/BackTests/'# 'Z:/'
    dataFile = dirPath +'Pickles/BSE100_FutsData11Oct-2022.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    basePath = 'Z:/BacktestsResults/DailyTradingSignals/RSI2with50SMA'
    #print('BSE100-'+HighLow[hl2]+'RSI('+rsiDays[rsiDay]+')-'+HighLow[hl]+'Mom('+MomDays[mDay]+')-'+rebal[rPeriod])
    finalDF = pandas.DataFrame()
    position = pandas.DataFrame()
    tradeDF = pandas.DataFrame()
    
    for ticker in mydata.Close.columns:
        try:
            a = ModelPortRSIwithSMA(mydata, ticker)
            a.run()
            print(ticker)
            ticker = ticker.replace('/', '_')
            backtestName = ticker# + '-RSI(2)-50SMA'
            #backtestName = 'BSE100-'+HighLow[hl]+'-Mom('+MomDays[mDay]+')-'+rebal[rPeriod]
            #backtestName = 'BSE100-'+HighLow[hl2]+'RSI('+rsiDays[rsiDay]+')-'+HighLow[hl]+'Mom('+MomDays[mDay]+')-'+rebal[rPeriod]
            
            a.ResultFrameWithIndex()
            
            #results = a.GetStatsReturns(Riskfree=0.05)
            
            # filepath = basePath+backtestName+'_'+str(datetime.datetime.today().date())+'.xlsx'
            # #a.savebacktestresult(filepath)
            # backtestName = backtestName.replace(', ', '_').replace(' ', '-')
            # navData = a.PlotResult.dropna()
            
            # yrsDiff = (navData.index[-1] - navData.index[0]).days/365.0
            # cagrRet = (navData.iloc[-1])**(1/yrsDiff) -1
            # AverageChurn = 0#int(100*a.Churn.resample('a').sum().mean())
            # titlename = 'CAGR-'+ str(["%.1f" % i for i in (cagrRet.values*100)]) + ',Churn-'+str(AverageChurn)+'%, ' +backtestName
            
            # navData.plot(title = titlename, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_NAV.jpg')   
            
            # temp = navData.pct_change(252)
            # temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
            # temp = pandas.DataFrame(temp)
            # temp.columns = ['Rolling 1Yr Returns ' + titlename]
            # temp['X-Axis'] = 0
            # temp.plot(title = titlename, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_RR.jpg')
            
            # tmp = navData.resample('a').last()
            # tmp = tmp.pct_change()
            # tmp.index = tmp.index.year
            # tmp.plot(kind = 'bar', title = titlename, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_Y_Plot.jpg')
            
            # tmp2 = navData.resample('q').last()
            # tmp2 = tmp2.pct_change()
            # tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
            # tmp2.plot(kind = 'bar', title = titlename, figsize =(18,6))
            # plt.savefig(basePath+backtestName+'_Q_Plot.jpg')
            
            a.NAV.columns = [ticker]
            finalDF = pandas.concat([finalDF, a.NAV], axis = 1)
            position = pandas.concat([position, a.Position.loc[:, ticker]], axis = 1)
            
            df = a.trade_reg.get_trade_register()
            tradeDF = pandas.concat([tradeDF, df], axis = 0)
        except:
            pass
    
    filepath = basePath+str(datetime.datetime.today().date())+'.xlsx'
    stats_obj = Stats(tradeDF)
    statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
    statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
    
    writer = pandas.ExcelWriter(filepath)
    
    finalDF.to_excel(writer,'NAV')
    position.to_excel(writer,'Position')
    tradeDF.to_excel(writer,'Trades')
    statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
    statsPositionDF.to_excel(writer,'Stats-Position')
    writer.save()
    writer.close()
    
    #cagrData.append([backtestName, HighLow[hl], MomDays[mDay], HighLow[hl2], rsiDays[rsiDay], rebal[rPeriod], round(cagrRet.values[0]*100, 1), round(cagrRet.values[1]*100, 1), AverageChurn])
    #pandas.DataFrame(cagrData, columns =['Des', 'HighLow-MoM', 'MomDays', 'HighLow-RSI', 'RSIDays', 'RebalanceFreq', 'StrategyCAGR', 'IndexCAGR', 'Churn']).to_clipboard()