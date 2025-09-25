# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 14:43:00 2023
@author: Viren@InCred

Nifty/bank Nifty Dispersion Model
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

class DispersionNifty(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self, data)
        #self.TimeDiff = timeDiff # timedif fis in Minutes
        #self.StopLossLimit = sl
        #self.TargetLimit = target
        self.TradedValue = 50000000# 5Cr.
        self.StrategyType = 'OP'
        self.PairTrades = True # True if Both PE and CE positions, used for gross exposure calculation, if PairTrades then half exposure otherwise one side exposure
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime(list(self.BackTestData.TradingDates.keys())[0])#pandas.to_datetime('2014-06-26')
        self.EndTime = pandas.to_datetime('2022-05-15')
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
            StocksCount = 6
            ticker = 'BANKNIFTY'            
            indexComp = [self.BackTestData.DictBloomNSE[it] for it in self.GetLatestIndexComponents()]
            curMcap = self.BackTestData.Index.MarketCap.loc[self.CurrentTime]
            curMcapWt = curMcap.loc[curMcap.index.isin(indexComp)].dropna()
            
            curMcapWt = curMcapWt.sort_values().iloc[-StocksCount:]
            curMcapWt = curMcapWt/curMcapWt.sum()
            
            #if self.CurrentTime >= datetime.datetime(2022, 12, 29):
            #pdb.set_trace()
            if self.CurrentTime in self.ExpiryDates:
                self.NearExpiryDate = [i for i in self.ExpiryDates if i >= self.CurrentTime][1]
            else:
                self.NearExpiryDate = [i for i in self.ExpiryDates if i >= self.CurrentTime][0]
                
            #self.NearExpiryDate = [i for i in self.ExpiryDates if i >= self.CurrentTime][1]
            temp = [it+self.NearExpiryDate.strftime('%d%b%y').upper()+x for x in ['CE', 'PE']  for it in curMcapWt.index]#
            
            # Make the tickers for searching in close price DF for all stocks and Index & assign Weight (dict)
            #CurTickers = {}
            #self.CurWeights = {}
            self.CurTickersWt = []
            for iTicker in temp:
                #CurTickers.update({iTicker: s for s in self.Close.columns if iTicker in s})
                #self.CurWeights.update({s: -curMcapWt.loc[iTicker[:len(iTicker)-9]] for s in self.Close.columns if iTicker in s})
                ok = [self.CurTickersWt.append([iTicker, s, -curMcapWt.loc[iTicker[:len(iTicker)-9]], s.replace(iTicker, '')]) for s in self.Close.columns if iTicker in s]
                #ok = [CurTickers.append([iTicker, s]) for s in self.Close.columns if iTicker in s]
                #ok = [self.CurWeights.append([s, -curMcapWt.loc[iTicker[:len(iTicker)-9]]]) for s in self.Close.columns if iTicker in s]
                #ok = [CurTickers.append(s) for s in self.Close.columns if ticker in s]
            indTickers = [ticker+self.NearExpiryDate.strftime('%d%b%y').upper()+x for x in ['CE', 'PE']]
            for indT in indTickers:
                #CurTickers.update({indT: s for s in self.Close.columns if indT in s})
                #self.CurWeights.update({s: 1.0 for s in self.Close.columns if indT in s})
                ok = [self.CurTickersWt.append([indT, s, 1.0, s.replace(indT, '')]) for s in self.Close.columns if indT in s]
                #ok = [CurTickers.append([indT, s]) for s in self.Close.columns if indT in s]
                #ok = [self.CurWeights.append([s, 1.0]) for s in self.Close.columns if indT in s]            
            #pdb.set_trace()
            self.CurTickersWt = pandas.DataFrame(self.CurTickersWt, columns = ['Ticker', 'TickStrike', 'Weight', 'Strike'])
            self.CurTickersWt.index = self.CurTickersWt.TickStrike
            self.CurTickersWt.Strike = self.CurTickersWt.Strike.astype('float')
            tempDF = pandas.DataFrame([re.match(self.OptionTickerRegEx, iTicker).groupdict() for iTicker in self.CurTickersWt.index], index = self.CurTickersWt.index)
            try:
                self.CurTickersWt['NSE'] = tempDF.symbol
            except:
                pdb.set_trace()
            AvgStrike = self.CurTickersWt.groupby('NSE').mean('Strike')
            self.CurTickersWt['AvgStrike'] = [AvgStrike.loc[it, 'Strike'] for it in self.CurTickersWt.NSE]
            
            self.CurTickersWt['Quantity'] = numpy.divide(self.CurTickersWt.Weight*self.TradedValue, self.CurTickersWt.AvgStrike).round()
            
            self.CurTickersWt['Price'] = self.Close.loc[self.CurrentTime, self.CurTickersWt.index]
            self.CurTickersWt['Wt'] = numpy.multiply(self.CurTickersWt.Quantity, self.CurTickersWt.Price)
            self.CurTickersWt.Wt = self.CurTickersWt.Wt/self.CurTickersWt.Wt.abs().sum()
            #self.Weights.loc[self.CurrentTime, dtemp.index] = dtemp['Weight'].values#[i[0] for i in dtemp.values]
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.Position.loc[self.CurrentTime] = 0        
        self.Quantity.loc[self.CurrentTime] = 0
        for iTicker in self.CurTickersWt.index:#self.CurWeights.keys():
            self.CapitalAllocation.loc[self.CurrentTime,iTicker] = self.CurrentNAV*self.CurTickersWt.loc[iTicker, 'Wt']#self.CurrentNAV*self.CurWeights[iTicker]
            self.Position.loc[self.CurrentTime, iTicker] = numpy.sign(self.CurTickersWt.loc[iTicker, 'Wt'])
            self.Quantity.loc[self.CurrentTime, iTicker] = self.CurTickersWt.loc[iTicker, 'Quantity']
        print(self.CurrentTime.date())
        self.UpdateOrderBook(strategyID = 'BNFDisp-Straddle_ExpToExp_', options = 'y')

if __name__=='__main__':
    import pickle    
    from pathlib import Path
    t11 = time.time()
    #BankNiftyDispersion_ExpiryToExpiry_Straddle04Apr-2023
    if os.path.exists('Z:/Pickles/'):
        pickleFile = 'Z:/Pickles/BNF_Disp_ExpiryToExpiry16May-2023.pkl'
    else:
        pickleFile = 'G:/Shared drives/BackTests/Pickles/BNF_Disp_ExpiryToExpiry16May-2023.pkl'
    if os.path.exists(pickleFile):
        f = open(pickleFile, 'rb')
        mydata = pickle.load(f)
        f.close()
    else:
        print('Data Error! Working on Data.')
        pdb.set_trace()
    
    model = DispersionNifty(mydata)
    model.run()
            
    
    basePath = 'G:/Shared drives/BackTests/BackTestsResults/Dispersion/BankNifty/'
    if not os.path.exists(basePath):
        path = Path(basePath)
        path.mkdir(parents=True)
        #os.mkdirs(basePath)       
    backtestName = 'BNFDisp-Straddle_ExpToExp_'
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
