# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 11:30:00 2022
@author: Viren@Incred
Selecting Top 10 for Long  and Bottom 10 for Short:
    Based on 6 Months Cross Sectional price Momentum
Finalised- 6Months Price Momentum, 1yr Low Vol, and 1 Months Price Reversal
"""

from FactoryBackTester import FactoryBackTester
#from FactoryBackTester import NameDecorator
import MyTechnicalLib
import pandas
import numpy
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

class SimpleMomenutmLongShort(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self,data)
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2011-12-29')#('2012-12-27')
        self.UpdateDates = self.ExpiryDates#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.000#25# Total 2 bps Transaction Charges        
        self.order = Order()
        self.trade_reg = TradeRegister()
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
    
    def GetMomRanks(self, ClosePrice, stocksList):
        OneYrEx1MMom = ClosePrice.pct_change(252, fill_method = None).loc[self.ExpiryDates[list(self.ExpiryDates).index(self.CurrentTime)-1], stocksList].dropna()
        
        OneYrMom = ClosePrice.pct_change(252, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        OneYrStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-252:].std().dropna()*numpy.sqrt(252)        
        
        # OneYrMom = OneYrMom.rank(ascending = True) # Higher is better
        #OneYrStd = OneYrStd.dropna().rank(ascending = False) # Lower is better
        
        SixMonthsMom = ClosePrice.pct_change(126, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        SixMonthsStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-126:].std().dropna()*numpy.sqrt(252)      
        
        # SixMonthsMom = SixMonthsMom.rank(ascending = True) # Higher is better
        #SixMonthsStd = SixMonthsStd.dropna().rank(ascending = False) # Lower is better
        
        # ThreeMonthsMom = ClosePrice.pct_change(63, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        #ThreeMonthsStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-63:].std()#*numpy.sqrt(252)        
        
        # ThreeMonthsMom = ThreeMonthsMom.rank(ascending = True) # Higher is better
        #ThreeMonthsStd = ThreeMonthsStd.dropna().rank(ascending = False) # Lower is better
        
        # OneMonthMom = ClosePrice.pct_change(21, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        # OneMonthMom = OneMonthMom.rank(ascending = False) # Lower is better, Reversal
        # OneMonthMom = OneMonthMom.sort_values()
        #self.FactorRanks = numpy.divide(OneYrEx1MMom, OneYrStd).dropna()#pandas.concat([OneYrStd, SixMonthsStd, ThreeMonthsStd], axis =1).mean(axis = 1)
        self.FactorRanks = numpy.divide(OneYrEx1MMom, OneYrStd).dropna()
        
        
    def UpdateSpecificStats(self):
        stocksList = list(self.Close.columns)
        indexNames = self.BackTestData.IndexInclusionFactor.loc[:self.CurrentTime].iloc[-1].dropna().index        
        stocksList = set.intersection(set(stocksList), set(indexNames))
        stocksList = self.Close.loc[self.CurrentTime, stocksList].dropna().index
        self.GetMomRanks(self.Close, stocksList)
        # OneYrStd = self.Close.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-252:].dropna().std()*numpy.sqrt(252)
        # OneYrStd = OneYrStd.rank(ascending = False) # Lower is better
        # self.FactorRanks = OneYrStd.sort_values()
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0           
        if len(self.FactorRanks) >0:
            self.FactorRanks = self.FactorRanks.sort_values()
            longStocks = self.FactorRanks.iloc[-20:].index # Top 10 Long
            shortStocks = self.FactorRanks.iloc[:20].index # bottom 10 Short
            self.Position.loc[self.CurrentTime] = 0
            for ticker in longStocks:
                self.CapitalAllocation.loc[self.CurrentTime, ticker] = self.CurrentNAV/(len(longStocks) + len(shortStocks))
                self.Position.loc[self.CurrentTime, ticker] = 1
            for ticker in shortStocks:
                self.CapitalAllocation.loc[self.CurrentTime, ticker] = -self.CurrentNAV/(len(longStocks) + len(shortStocks))
                self.Position.loc[self.CurrentTime, ticker] = -1
        self.UpdateOrderBook(strategyID = '12MVolAdjMom')
        print(self.CurrentTime.date(), len(longStocks) + len(shortStocks), '12MVolAdjMom')
                
if __name__=='__main__':
    import pickle
    dirPath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'# 'Z:/'
    dataFile = dirPath +'STFM_Stocks_2003_20230516.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    #G:\Shared drives\BackTests\BackTestsResults\SimpleMomLongShort
    basePath = 'G:/Shared drives/BackTests/BacktestsResults/SimpleMomLongShort/'
    
    a = SimpleMomenutmLongShort(mydata)
    a.run()
    a.ResultFrameWithIndex()
    backtestName = 'BSE100Futs_12MVolAdjMom_ex1M_LongShort_20StocksEach'
    filepath = basePath+backtestName+'_'+str(datetime.datetime.today().date())+'.xlsx'
    a.savebacktestresult(filepath)
    backtestName = backtestName.replace(', ', '_').replace(' ', '_')
    navData = a.PlotResult.dropna()
    
    yrsDiff = (navData.index[-1] - navData.index[0]).days/365.0
    cagrRet = (navData.iloc[-1])**(1/yrsDiff) -1
    AverageChurn = int(100*a.Churn.resample('a').sum().mean())
    titlename = 'CAGR-'+ str(["%.1f" % i for i in (cagrRet.values*100)]) + ',Churn-'+str(AverageChurn)+'%, ' +backtestName
    
    navData.plot(title = titlename, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_NAV.jpg')   
    
    temp = navData.pct_change(252)
    temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
    temp = pandas.DataFrame(temp)
    temp.columns = ['Rolling 1Yr Returns ' + titlename]
    temp['X-Axis'] = 0
    temp.plot(title = titlename, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_RR.jpg')
    
    tmp = navData.resample('a').last()
    tmp = tmp.pct_change()
    tmp.index = tmp.index.year
    tmp.plot(kind = 'bar', title = titlename, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_Y_Plot.jpg')
    
    tmp2 = navData.resample('q').last()
    tmp2 = tmp2.pct_change()
    tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
    tmp2.plot(kind = 'bar', title = titlename, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_Q_Plot.jpg') 
    
    tradeDF = a.trade_reg.get_trade_register()
        
    filepath = basePath+backtestName+'_TradesRegister.xlsx'
    stats_obj = Stats(tradeDF)
    statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
    statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
    
    writer = pandas.ExcelWriter(filepath)
    tradeDF.to_excel(writer,'Trades')
    statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
    statsPositionDF.to_excel(writer,'Stats-Position')
    writer.save()
    writer.close()
    
