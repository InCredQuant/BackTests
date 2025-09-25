# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 13:58:53 2022

@author: Viren@Incred
Testing BodyOutSideBand:
    20EMA, Lower Band (99% of EMA), Upper Band (101% of EMA)
    
    1. TradeSignal: If High > Previous Upper band -> 1, If Lower < Previous Lower Band -> -1
    
    If Current TradeSignal is not Neutral and Last TradeSignal is Neutral, then Signal as Current TradeSignal
    if High or Low Crosses the band then Close the Position
    therwise hits StopLoss - -2%, or  target 6%, then Exit Position
    
    
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
from random import choice
#from scipy import stats
import pdb
import warnings
warnings.filterwarnings("ignore")

from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter


import warnings
warnings.filterwarnings("ignore")

class BodyOutSideBand(FactoryBackTester):
    def __init__(self,data, ticker):
        FactoryBackTester.__init__(self,data)
        self.ticker = ticker
        
        
    def basicdatainitialize(self):
        ClosePrice = self.BackTestData.EMA21.loc[:, self.ticker].dropna()
        self.CurrentTime = max(ClosePrice.index[0] ,pandas.to_datetime('2009-12-31'))# '2013-01-07'
        #self.GetAllUpdateDates(self.Days)
        #self.CurrentTime = self.BackTestData.ExpiryDates[30]
        self.UpdateDates = list(self.BackTestData.EMA21.index)#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.02#15# 15 bps total, assuming 5bps transaction charges and 10 bps slippages
        self.TradePosition=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.trade_reg = TradeRegister()
        self.order = Order()
        self.StopLossLimit = -0.02# Stop loss
        self.TargetLimit = 0.06
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

    def DetectPostionStartDate(self):
        tempList = self.Position.loc[:self.CurrentTime, self.ticker]
        tempList = tempList.sort_index(ascending = False)
        iDate = tempList.index[0]
        self.TradeTakenDate = None
        for iTime in list(tempList.index[:90]):
            if tempList.loc[iTime] != tempList.iloc[0]:
                self.TradeTakenDate = iDate
                break
            else:
                iDate = iTime

    def EndOrderPosition(self, endTime):
        self.order.exit_date = self.CurrentTime
        self.order.exit_price = self.BackTestData.Close.loc[endTime, self.ticker]
        self.trade_reg.append_trade(self.order)
        self.order = Order()

    def StartOrderPosition(self, startPositon = choice([Position.LONG, Position.SHORT])):
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
        self.DetectPostionStartDate()        
        try:
            rets = (self.BackTestData.Close.loc[self.CurrentTime, self.ticker]/self.BackTestData.Close.loc[self.TradeTakenTime, self.ticker]) -1
            if rets*self.Position.loc[self.CurrentTime, self.ticker] < self.StopLossLimit:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
                #self.EndOrderPosition(self.CurrentTime)
        except:
            pass

    def Target(self):
        self.DetectPostionStartDate()        
        try:
            rets = (self.BackTestData.Close.loc[self.CurrentTime, self.ticker]/self.BackTestData.Close.loc[self.TradeTakenTime, self.ticker]) -1
            if rets*self.Position.loc[self.CurrentTime, self.ticker] > self.TargetLimit:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
                #self.EndOrderPosition(self.CurrentTime)
        except:
            pass

    def UpdateSpecificStats(self):
        if numpy.isnan(self.BackTestData.EMA21.loc[self.CurrentTime, self.ticker]):
            self.order.symbol = self.ticker
            self.order.segment = Segment.EQ
            self.order.quantity = 1
            if self.Position.loc[self.LastTime, self.ticker] in [1, -1]:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
                self.EndOrderPosition(self.LastTime)
                next
        else:
            if self.BackTestData.Low.loc[self.CurrentTime, self.ticker] > 1.01*self.BackTestData.EMA21.loc[self.LastTime, self.ticker]:
                self.TradePosition.loc[self.CurrentTime, self.ticker] = 1
            elif self.BackTestData.High.loc[self.CurrentTime, self.ticker] < 0.99*self.BackTestData.EMA21.loc[self.LastTime, self.ticker]:
                self.TradePosition.loc[self.CurrentTime, self.ticker] = -1
    
        
    def updateCapAllocation(self):
        if self.TradePosition.loc[self.LastTime, self.ticker] == 0 and self.TradePosition.loc[self.CurrentTime, self.ticker] != 0:
            self.Position.loc[self.CurrentTime, self.ticker] = self.TradePosition.loc[self.CurrentTime, self.ticker]
        elif self.Position.loc[self.LastTime, self.ticker] == 1:
            cond1 = self.BackTestData.High.loc[self.CurrentTime, self.ticker] < 1.01*self.BackTestData.EMA21.loc[self.LastTime, self.ticker]
            if cond1:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, self.ticker] = self.Position.loc[self.LastTime, self.ticker]
        elif self.Position.loc[self.LastTime, self.ticker] == -1:
            cond2 = self.BackTestData.Low.loc[self.CurrentTime, self.ticker] > 0.99*self.BackTestData.EMA21.loc[self.LastTime, self.ticker]
            if cond2:
                self.Position.loc[self.CurrentTime, self.ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, self.ticker] = self.Position.loc[self.LastTime, self.ticker]
        else:
            self.Position.loc[self.CurrentTime, self.ticker] = self.Position.loc[self.LastTime, self.ticker]
            
        if self.Position.loc[self.CurrentTime, self.ticker] in [-1, 1]:
             self.StopLoss()
             self.Target()             
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
    basePath = dirPath+'BacktestsResults/DailyTradingSignals/BodyOutSideBand_'
    #print('BSE100-'+HighLow[hl2]+'RSI('+rsiDays[rsiDay]+')-'+HighLow[hl]+'Mom('+MomDays[mDay]+')-'+rebal[rPeriod])
    finalDF = pandas.DataFrame()
    position = pandas.DataFrame()
    tradeDF = pandas.DataFrame()
    
    for ticker in mydata.Close.columns:
        try:
            a = BodyOutSideBand(mydata, ticker)
            a.run()
            print(ticker)
            ticker = ticker.replace('/', '_')
            a.ResultFrameWithIndex()
            
            a.NAV.columns = [ticker]
            finalDF = pandas.concat([finalDF, a.NAV], axis = 1)
            position = pandas.concat([position, a.Position.loc[:, ticker]], axis = 1)
            
            df = a.trade_reg.get_trade_register()
            tradeDF = pandas.concat([tradeDF, df], axis = 0)
        except:
             pass
        
    filepath = basePath+str(datetime.datetime.today().date())+'_Updated.xlsx'
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