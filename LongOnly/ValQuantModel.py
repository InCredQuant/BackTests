# -*- coding: utf-8 -*-
"""
Created on Tue May 27 12:35:22 2025
@author: Viren@Incred
Selecting Top Stocks based on ValQuant Score:
    1. Long High Score
    2. Long on High Change in Score
    3. Short Low Score
    4. Short Lowest Change in Score
    5. Long + Short: Long High Score + Short Low Score
    6. Long + Short: Long High Change in Score + Short on Lowest Low Score


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

class ValQuantModel(FactoryBackTester):
    def __init__(self,data, indexName = 'BSE100 INDEX'):
        FactoryBackTester.__init__(self,data)
        self.Index = indexName
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2014-07-31')#.date()
        self.GetAllUpdateDates(85)
        self.indexcomponents = self.BackTestData.IndexCompDict[self.Index]
        #self.UpdateDates = self.ExpiryDates#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.00025# Total 2 bps Transaction Charges        
        self.order = Order()
        self.trade_reg = TradeRegister()
        self.ValQuantScoreChange = self.BackTestData.ValQuantScore.pct_change(3)
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True

    def DetectPostionStartDate(self, ticker, positionMat):
        '''positionMat: self.Position/self.PositionWOSL'''
        #TradeTakenDate = self.CurrentTime
        tempList = positionMat.loc[:self.CurrentTime, ticker]
        tempList = tempList.sort_index(ascending = False)
        iDate = tempList.index[0]
        TradeTakenDate = tempList.index[0]
        for iTime in list(tempList.index[1:]):
            if tempList.loc[iTime] != tempList.iloc[1]:
                TradeTakenDate = iDate
                break
            else:
                iDate = iTime
        return TradeTakenDate

    def OrderPosition(self, iTicker, entry_date, position, exit_date=''):
        self.order = Order()
        self.order.symbol = iTicker
        self.order.segment = Segment.EQ
        self.order.quantity = 1
        if position == 1:
            self.order.position = Position.LONG
        elif position == -1:
            self.order.position = Position.SHORT
        self.order.entry_date = entry_date
        self.order.entry_price = self.Close.loc[entry_date, iTicker]
        if exit_date != '':
            exit_date = exit_date# Do nothing
        elif numpy.isnan(self.Close.loc[self.CurrentTime, iTicker]):
            exit_date = self.LastTime
        else:
            exit_date = self.CurrentTime
        self.order.exit_date = exit_date
        self.order.exit_price = self.Close.loc[exit_date, iTicker]
        self.trade_reg.append_trade(self.order)
        
    def StopLoss(self, ticker, StopLossLimit):
        TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL)    
        try:
            rets = (self.Close.loc[self.CurrentTime, ticker]/self.Close.loc[TradeTakenDate, ticker]) -1
            if rets*self.PositionWOSL.loc[self.CurrentTime, ticker] < StopLossLimit:
                self.PositionExitDF.loc[self.CurrentTime, ticker] = -1
                #self.EndOrderPosition(self.CurrentTime)
        except:
            pass

    def Target(self, ticker, TargetLimit):
        TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL)        
        try:
            rets = (self.Close.loc[self.CurrentTime, ticker]/self.Close.loc[TradeTakenDate, ticker]) -1
            if rets*self.PositionWOSL.loc[self.CurrentTime, ticker] >= TargetLimit:
                self.PositionExitDF.loc[self.CurrentTime, ticker] = -1
                #self.EndOrderPosition(self.CurrentTime)
        except:
            pass
    
    def UpdateSpecificStats(self):
        self.IndexStocks = self.GetLatestIndexComponents()
        universe = list(set.intersection(set(self.IndexStocks), set(self.Close.loc[self.CurrentTime].dropna().index)))
        
        
        ValScore = self.BackTestData.ValQuantScore.loc[:self.CurrentTime].iloc[-1].dropna()
        ValScore = ValScore.rank(ascending= True) # highest Score is the Highest Rank
        ValScore = ValScore.loc[ValScore.index.isin(universe)]
        
        ValScoreChg = self.ValQuantScoreChange.loc[:self.CurrentTime].iloc[-1].dropna()
        ValScoreChg = ValScoreChg.loc[ValScoreChg.index.isin(universe)]
        ValScoreChg = ValScoreChg.rank(ascending= True) # highest Score is the Highest Rank
        
        self.Factors =  (ValScore+ValScoreChg).sort_values()
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.Position.loc[self.CurrentTime] = 0
        StocksDF = self.Factors.iloc[-25:].index
        self.CapitalAllocation.loc[self.CurrentTime, StocksDF] = self.CurrentNAV*1.0/len(StocksDF)
        self.Position.loc[self.CurrentTime, StocksDF] = 1        
        print(self.CurrentTime.date(), len(self.Factors),  sep = ' ')
                
if __name__=='__main__':
    import pickle
    dirPath = 'G:/Shared drives/BackTests/'# 'Z:/'
    dataFile = 'G:/Shared drives/QuantFunds/EquityPlus/DataPickles/ValQuantModel20250527.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    basePath = dirPath+'BacktestsResults/ValQuantData/'
    indexName = 'BSE200 INDEX' 
    a = ValQuantModel(mydata, indexName = indexName) #['BSE100 INDEX', 'BSE200 INDEX']
    a.run()
    a.ResultFrameWithIndex()
    backtestName = 'ValQuant-Long-Score+ChgScoreTop25-'+ indexName.replace(' INDEX', '')
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
    
    temp = navData.pct_change(12)
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
    
    '''
    tradeDF = pandas.DataFrame()
    df = a.trade_reg.get_trade_register()
    tradeDF = pandas.concat([tradeDF, df], axis = 0)
        
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
    '''