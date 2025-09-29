# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 14:49:53 2023
@author: Viren@InCred

Nifty/bank Nifty Octopus Backtest
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

class Octopus(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self, data)
        #self.TimeDiff = timeDiff # timedif fis in Minutes
        #self.StopLossLimit = sl
        #self.TargetLimit = target
        self.TradedValue = 50000000# 5Cr.
        self.StrategyType = 'OP'
        self.PairTrades = True # True if Both PE and CE positions, used for gross exposure calculation, if PairTrades then half exposure otherwise one side exposure
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2016-06-02')
        #self.EndTime = pandas.to_datetime('2023-04-17')
        self.UpdateDates = self.BackTestData.TradingDates#self.ExpiryDates
        #self.UpdateDates = [datetime.datetime.strptime(it, '%Y-%m-%d') for it in self.UpdateDates]
        self.UpdateDates = [i for i in self.UpdateDates if  i >= self.CurrentTime and i <= self.EndTime]
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

        
    def UpdateSpecificStats(self):
        if self.CurrentTime < self.EndTime:
            atm = int(self.BackTestData.indexprice.loc[self.CurrentTime]/100)*100
            allTickers = self.BackTestData.TradingDates[self.CurrentTime.date()]
            #allTickers = list(set.intersection(set(allTickers), set(self.Close.columns)))
            tempDF = pandas.DataFrame([re.match(self.OptionTickerRegEx, iTicker).groupdict() for iTicker in allTickers], index = allTickers)
            tempDF.strike = tempDF.strike.astype('int')
            self.CurTickersWt = pandas.DataFrame([175 if numpy.abs(tempDF.loc[it, 'strike'] - atm) >= 400 else -25 for it in tempDF.index], index = tempDF.index, columns = ['Quantity'])
            #self.CurTickersWt = pandas.DataFrame(len(allTickers)*[-1], index = allTickers, columns = ['Quantity'])
            
            self.CurTickersWt['Price'] = self.Close.loc[self.CurrentTime, self.CurTickersWt.index]
            self.CurTickersWt['Wt'] = numpy.multiply(self.CurTickersWt.Quantity, self.CurTickersWt.Price)
            self.CurTickersWt.Wt = self.CurTickersWt.Wt/self.CurTickersWt.Wt.sum()
            #self.Weights.loc[self.CurrentTime, dtemp.index] = dtemp['Weight'].values#[i[0] for i in dtemp.values]
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.Position.loc[self.CurrentTime] = 0        
        self.Quantity.loc[self.CurrentTime] = 0
        for iTicker in self.CurTickersWt.index:#self.CurWeights.keys():
            self.CapitalAllocation.loc[self.CurrentTime,iTicker] = self.CurrentNAV*self.CurTickersWt.loc[iTicker, 'Wt']#self.CurrentNAV*self.CurWeights[iTicker]
            self.Position.loc[self.CurrentTime, iTicker] = numpy.sign(self.CurTickersWt.loc[iTicker, 'Quantity'])
            self.Quantity.loc[self.CurrentTime, iTicker] = self.CurTickersWt.loc[iTicker, 'Quantity']
        print(self.CurrentTime.date())
        self.UpdateOrderBook(strategyID = 'BankNifty_Octopus_7Legs_WeeklyExpiry_DownSideProtected', options = 'y')

if __name__=='__main__':
    import pickle    
    from pathlib import Path
    t11 = time.time()
    #BankNiftyDispersion_ExpiryToExpiry_Straddle04Apr-2023
    if os.path.exists('Z:/Pickles/'):
        pickleFile = 'Z:/Pickles/BankNiftyOctopus_7Legs_WeeklyExpiryData18Apr-2023.pkl'
    else:
        pickleFile = 'G:/Shared drives/BackTests/Pickles/BankNiftyOctopus_7Legs_WeeklyExpiryData18Apr-2023.pkl'
    if os.path.exists(pickleFile):
        f = open(pickleFile, 'rb')
        mydata = pickle.load(f)
        f.close()
    else:
        print('Data Error! Working on Data.')
        pdb.set_trace()
    
    model = Octopus(mydata)
    model.run()
            
    
    basePath = 'G:/Shared drives/BackTests/BackTestsResults/Octopus/BankNifty/'
    if not os.path.exists(basePath):
        path = Path(basePath)
        path.mkdir(parents=True)
        #os.mkdirs(basePath)       
    backtestName = 'BankNifty_Octopus_7Legs_WeeklyExpiry_DownSideProtected'
    filepath = basePath+backtestName+datetime.datetime.strftime(datetime.datetime.now().date(), '%d%b%Y')+'.xlsx'        
    model.savebacktestresult(filepath, fullData = True)
    
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
