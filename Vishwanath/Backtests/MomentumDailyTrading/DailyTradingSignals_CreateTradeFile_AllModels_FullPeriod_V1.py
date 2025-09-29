# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30 11:51:49 2022

@author: Viren@InCred
Description- Creates All Possible Trades for All Models, After creating all possible trades for all tickers and models, it will create the master file, which will be used for deciding the Dynmic Model selection
"""

from FactoryBackTester_V1 import FactoryBackTester
import MyTechnicalLib as mylib
import pandas
import numpy
import datetime
import pdb
import matplotlib.pyplot as plt
import copy
from random import choice
import pdb
import warnings
warnings.filterwarnings("ignore")

from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter

#TRADE_REGISTER = 'G:/Shared drives/BackTests/pycode/MomentumDailyTrading/AllTrades_TradesRegister.xlsx'
TRADE_REGISTER = 'AllTrades_TradesRegister.xlsx'

class DailyTradingSignals(FactoryBackTester):
    def __init__(self,data, modelNum = 0):
        FactoryBackTester.__init__(self,data)
        self.ModelNum = modelNum
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2011-12-29')#pandas.to_datetime('2022-12-29')#('2022-08-31')# '2013-01-07'
        self.UpdateDates = list(self.Close.index)#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.000#2# Total 2 bps Transaction Charges        
        self.order = Order()
        self.StopLossLimit = -0.10# Stop loss Limit at Individual level
        self.MODELS = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 3, '20WMA_MACD' : 4, 'BodyOutSideBand' : 5, 'ROCMA' : 6, 'RegressionCrossOver' : 7, 'Vortex' : 8, 'Oscillator' : 9, 'RSI50': 10, 'AssymetricWeekly' : 11, 'Seasoning' : 12, 'AssymetricDaily' : 13}
        self.ModelLookBack = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 2, '20WMA_MACD' : 2, 'BodyOutSideBand' : 2, 'ROCMA' : 2, 'RegressionCrossOver' : 1, 'Vortex' : 3, 'Oscillator' : 4, 'RSI50': 2, 'AssymetricWeekly' : 5, 'Seasoning' : 2, 'AssymetricDaily' : 2}# lookback period in Number of Quarters
        self.MODELSELECTOR = {}
        
        try:
            self.TRADE_REGISTER = pandas.read_excel(TRADE_REGISTER, sheet_name = 'Trades', header = 0, index_col = 0)
            self.TRADE_REGISTER.index = self.TRADE_REGISTER['EXIT_DATE']
            self.TRADE_REGISTER = self.TRADE_REGISTER.sort_index()
        except:
            pass
        
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)# it tracks for the exit records, StopLoss, Target or anything which might be added 
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)# It tracks before applying anytype of StopLoss or target
        self.trade_reg = TradeRegister()
        self.Strategy = pandas.DataFrame(numpy.nan, index=self.Close.index,columns=self.Close.columns)
        
        self.RSI2Position = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.SMA50Position = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.SMA100Position = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.DirectionPosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.TradePosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.RetsHigh = self.BackTestData.Index.High.pct_change(fill_method = None)
        self.RetsLow = self.BackTestData.Index.Low.pct_change(fill_method = None)
        self.RetsClose = self.BackTestData.Index.Close.pct_change(fill_method = None)
        
        self.MACDDiff = numpy.subtract(self.BackTestData.Index.MACD, self.BackTestData.MACDSignal)#self.BackTestData.Index.MACDSignal)
        self.PriceAboveWMA = numpy.subtract(self.BackTestData.Index.Close, self.BackTestData.Index.WMA20)
        self.ATRRatio = numpy.subtract(self.BackTestData.Index.Close, self.BackTestData.Index.SMA24)/self.BackTestData.Index.ATR24
        self.CloseDiffMinSMA = numpy.subtract(self.BackTestData.Index.Close, numpy.minimum(self.BackTestData.Index.SMA10, self.BackTestData.Index.SMA20))
        
        self.CloseWeekly = self.BackTestData.Index.Close.resample('w-FRI', convention = 'end').last()
        self.SMA10Weekly = mylib.MovingAverage(self.CloseWeekly, 10)
        self.SMA20Weekly = mylib.MovingAverage(self.CloseWeekly, 20)
        self.CloseDiffMinSMAWeekly = numpy.subtract(self.CloseWeekly, numpy.minimum(self.SMA10Weekly, self.SMA20Weekly))
        
        self.PviNviRatio = numpy.subtract(self.BackTestData.Index.PVI, self.BackTestData.Index.NVI)
        if self.ModelNum == 12:
            self.BackTestData.Index.Close = self.BackTestData.Index.Close.div(self.BackTestData.indexprice.loc[:, 'NZ1 INDEX'], axis = 0)
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
            
    def UpdateSpecificStats(self):        
        switcher = {1: self.MD1_RSIwith50SMATrend, 2: self.MD2_RSI50SMA, 3: self.MD3_Series3, 4: self.MD4_20WMA_MACD, 5: self.MD5_BodyOutSideBand, 6: self.MD6_ROCMA, 
                    7: self.MD7_RegressionCrossOver, 8: self.MD8_Vortex, 9: self.MD9_Oscillator, 10: self.MD10_RSI50, 11: self.MD11_AssymetricWeekly, 12: self.MD12_Seasoning, 13: self.MD11_AssymetricDaily}
        ExpiryDates = [it for it in self.BackTestData.ExpiryDates if datetime.datetime.strptime(it, '%Y-%m-%d').date() >= self.CurrentTime.date()]# NearExpiry is the date where we have to take the position any day
        if self.CurrentTime in self.ExpiryDates:
            self.NearExpiry = ExpiryDates[1]
        else:
            self.NearExpiry = ExpiryDates[0]
        
        pdb.set_trace()
        self.NearExpiry = datetime.datetime.strftime(datetime.datetime.strptime(self.NearExpiry, '%Y-%m-%d'), '%d%b%y').upper()
        tickers = set(self.BackTestData.Index.Close.loc[self.CurrentTime].dropna().index)        
        tickers = [it for it in tickers if it+self.NearExpiry+'XX0' in self.BackTestData.Close.columns]        
        
        self.indexNames = tickers
        func = switcher.get(self.ModelNum, lambda: 'Invalid Model')
        try:
            func(tickers)
        except:
            pdb.set_trace()
        
    def updateCapAllocation(self):
        #self.populateOrderPosition()
        self.CapitalAllocation.loc[self.CurrentTime] = 0       
        positionWOSL = self.PositionWOSL.loc[self.CurrentTime]
        tempDF = positionWOSL[positionWOSL != 0]
        
        for ticker in tempDF.index:
            self.StopLossTrail(ticker, self.StopLossLimit)
        
        self.Position.loc[self.CurrentTime] = self.PositionWOSL.loc[self.CurrentTime]
        for ticker in tempDF.index:
            TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL)
            checkExitPosition = self.PositionExitDF.loc[TradeTakenDate:self.CurrentTime, ticker].dropna()
            if len(checkExitPosition) >0:
                self.Position.loc[self.CurrentTime, ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, ticker] = self.PositionWOSL.loc[self.CurrentTime, ticker]

        MaxWT = 3.5/100
        indexName = 'NIFTY INDEX'#self.BackTestData.indexprice.columns[0]
        self.Strategy.loc[self.CurrentTime, indexName] = 'Hedge'
        CurrentPosition = self.Position.loc[self.CurrentTime]
        PositionWithSL = CurrentPosition[CurrentPosition != 0]
        netPosition = CurrentPosition.sum()
        
        GrossExposure = (PositionWithSL.abs().sum() + abs(netPosition))*MaxWT
        if GrossExposure > 2.0:
            MaxWT = 2.0/(PositionWithSL.abs().sum() + abs(netPosition))
            
        for iTicker in PositionWithSL.index:
            self.CapitalAllocation.loc[self.CurrentTime,iTicker]= PositionWithSL.loc[iTicker]*self.CurrentNAV*MaxWT
        if netPosition !=0:# Hedge the Net Position with Index
             self.Position.loc[self.CurrentTime, indexName] = -numpy.sign(netPosition)
             self.CapitalAllocation.loc[self.CurrentTime, indexName]= -netPosition*self.CurrentNAV*MaxWT
        if 'Cash' in self.Close.columns:# Allocate Remain to Cash
            self.CapitalAllocation.loc[self.CurrentTime, 'Cash'] = self.CurrentNAV*(1 - (PositionWithSL.abs().sum() + abs(netPosition))*MaxWT)
            self.Position.loc[self.CurrentTime, 'Cash'] = numpy.sign(self.CapitalAllocation.loc[self.CurrentTime, 'Cash'])
        self.UpdateOrderBook()
        new_dict = dict((value, key) for key, value in self.MODELS.items())
        print(self.CurrentTime.date(), new_dict[self.ModelNum])
                
if __name__=='__main__':
    import pickle
    dirPath = 'G:/Shared drives/BackTests/'# 'Z:/'
    #dataFile = 'G:/Shared drives/BackTests/Pickles/Hist_FutsData29May-2023.pkl'#
    #dataFile = 'Z:/Pickles/Hist_FutsData29May-2023.pkl'
    dataFile = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/STFDMOM_V2_20230621.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    basePath = dirPath+'BacktestsResults/DailyTradingSignals_Apr2023/'  
    iDict = {1: 'RSIwith50SMATrend', 2: 'RSI50SMA', 3: 'Series3', 4: '20WMA_MACD', 5: 'BodyOutSideBand', 6: 'ROCMA', 7: 'RegressionCrossOver', 8: 'Vortex', 9: 'Oscillator', 10: 'RSI50', 11: 'AssymetricWeekly', 12: 'Seasoning', 13: 'AssymetricDaily'}
    tradeDFAllTrades = pandas.DataFrame()
    
    for modelNum in iDict.keys():
        backtestName = iDict[modelNum]
        a = DailyTradingSignals(mydata, modelNum = modelNum)#iDict
        a.run()
        a.ResultFrameWithIndex()
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
        
    writer2 = pandas.ExcelWriter(TRADE_REGISTER.replace('.xlsx', str(datetime.datetime.today().date())+'.xlsx'))
    stats_obj = Stats(tradeDFAllTrades)
    statsSymbolDFAllTrades = stats_obj.create_stats(filter_by = Filter.SYMBOL)
    statsStrategyDFAllTrades = stats_obj.create_stats(filter_by = Filter.STRATEGY_ID)
    
    tradeDFAllTrades.to_excel(writer2,'Trades')
    statsSymbolDFAllTrades.transpose().to_excel(writer2,'Stats-Symbol')
    statsStrategyDFAllTrades.transpose().to_excel(writer2,'Stats-Strategy')
    writer2.save()
    writer2.close()