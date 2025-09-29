
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 17:15:52 2022

@author: Viren@Incred
DailyTrading Signals Combined Model:
"""
#import time
import warnings
warnings.filterwarnings("ignore")
from FactoryBackTester import FactoryBackTester
import MyTechnicalLib
import pandas
import numpy
import datetime
import pdb
import matplotlib.pyplot as plt
import copy
from random import choice
import os
import pdb


from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter
#from DailyTradingSignals_Futs import DailyTradingSignals


class DailyTradingSignalsIndexFuts(FactoryBackTester):
    def __init__(self,data, modelNum = 0, indexName = 'NZ1 Index'):
        FactoryBackTester.__init__(self, data)
        self.ModelNum = modelNum
        self.IndexName = indexName
        
    def basicdatainitialize(self):
        #self.CurrentTime = pandas.to_datetime('2023-10-01 09:14:59')#('2023-03-29')#('2011-12-28')#('2022-08-31')# '2013-01-07'#('2023-03-29')#
        self.CurrentTime = pandas.to_datetime('2004-12-30')#('2006-12-29')
        self.CurrentTime = pandas.to_datetime('2022-09-30')#('2022-03-31')
        self.UpdateDates = list(self.Close.loc[:, self.IndexName].dropna().index)#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.0003#3bps# Total 3 bps Transaction Charges        
        self.order = Order()
        self.StopLossLimit = -0.05# Stop loss Limit at Individual level
        self.MODELS = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 3, '20WMA_MACD' : 4, 'BodyOutSideBand' : 5, 'ROCMA' : 6, 'RegressionCrossOver' : 7, 'Vortex' : 8, 'Oscillator' : 9, 'RSI50': 10, 'AssymetricWeekly' : 11, 'Seasoning' : 12, 'AssymetricDaily' : 13}
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)# it tracks for the exit records, StopLoss, Target or anything which may be added 
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)# It tracks before applying anytype of StopLoss or target
        self.RSI2Position = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.SMA50Position = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.SMA100Position = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.DirectionPosition = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.TradePosition = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        
        self.RetsHigh = self.BackTestData.High.pct_change(fill_method = None)
        self.RetsLow = self.BackTestData.Low.pct_change(fill_method = None)
        self.RetsClose = self.Close.pct_change(fill_method = None)
        self.MACDDiff = numpy.subtract(self.BackTestData.MACD, self.BackTestData.MACDSignal)
        self.PriceAboveWMA = numpy.subtract(self.Close, self.BackTestData.WMA20)
        self.ATRRatio = numpy.subtract(self.Close, self.BackTestData.SMA24)/self.BackTestData.ATR24
        self.CloseDiffMinSMA = numpy.subtract(self.Close, numpy.minimum(self.BackTestData.SMA10, self.BackTestData.SMA20))
        
        self.CloseWeekly = self.BackTestData.CloseWeekly
        self.SMA10Weekly = self.BackTestData.SMA10Weekly
        self.SMA20Weekly = self.BackTestData.SMA20Weekly 
        self.CloseDiffMinSMAWeekly = numpy.subtract(self.CloseWeekly, numpy.minimum(self.SMA10Weekly, self.SMA20Weekly))
        
        self.PviNviRatio = numpy.subtract(self.BackTestData.PVI, self.BackTestData.NVI)
        self.trade_reg = TradeRegister()
        self.Strategy = pandas.DataFrame(numpy.nan, index=self.Close.index,columns=self.Close.columns)
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
     
    
    def UpdateSpecificStats(self):
        #if datetime.datetime(2014, 3, 3) <= self.CurrentTime:
        #    pdb.set_trace()
        #1, 2, 4,
        
        switcher = {1: self.MD1_RSIwith50SMATrend, 2: self.MD2_RSI50SMA, 3: self.MD3_Series3, 4: self.MD4_20WMA_MACD, 5: self.MD5_BodyOutSideBand, 6: self.MD6_ROCMA, 
                    7: self.MD7_RegressionCrossOver, 8: self.MD8_Vortex, 9: self.MD9_Oscillator, 10: self.MD10_RSI50, 11: self.MD11_AssymetricWeekly, 12: self.MD12_Seasoning, 13: self.MD11_AssymetricDaily}
        func = switcher.get(self.ModelNum, lambda: 'Invalid Model')
        try:
            if self.ModelNum == 12:
                func(set([self.IndexName]), sl = -0.03, target = 0.10)
            else:
                func(set([self.IndexName]))
        except:
            pdb.set_trace()
        
        
    def updateCapAllocation(self):
        #if self.CurrentTime.date() == datetime.date(2012, 4, 4):
        #    pdb.set_trace()
        self.CapitalAllocation.loc[self.CurrentTime] = 0       
        positionWOSL = self.PositionWOSL.loc[self.CurrentTime]
        tempDF = positionWOSL[positionWOSL != 0]
        #for ticker in tempDF.index:
        #    self.StopLossTrail(ticker, self.StopLossLimit)
        
        self.Position.loc[self.CurrentTime] = self.PositionWOSL.loc[self.CurrentTime]
        for ticker in tempDF.index:
            TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL, forStopLoss= True)
            checkExitPosition = self.PositionExitDF.loc[TradeTakenDate:self.CurrentTime, ticker].dropna()
            if len(checkExitPosition) >0:
                self.Position.loc[self.CurrentTime, ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, ticker] = self.PositionWOSL.loc[self.CurrentTime, ticker]

        CurrentPosition = self.Position.loc[self.CurrentTime]
        PositionWithSL = CurrentPosition[CurrentPosition != 0]
        
        for iTicker in PositionWithSL.index:
            self.CapitalAllocation.loc[self.CurrentTime,iTicker]= PositionWithSL.loc[iTicker]*self.CurrentNAV
        self.UpdateOrderBook()
        #print(self.CurrentTime.date(), dict([(value, key) for key, value in self.MODELS.items()])[self.ModelNum], self.IndexName)
        
                
if __name__=='__main__':
    import pickle
    dirPath = 'G:/Shared drives/QuantFunds/Liquid1/LiveModels/ModelFiles/'#'G:/Shared drives/BackTests/'# 'Z:/'
    #dataFile = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/IndexFutsData_Daily20250808.pkl'
    dataFile = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/IndexFutsData_2Hourly20250808.pkl'
    #dataFile = dirPath +'Pickles/Index_FutsData18Apr-2023.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    
    MODELS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]#, 11, 12]
    strategy = '2Hourly_'+datetime.date.today().strftime('%d%b%Y')#2Hourly
    Indices = {'MIDCPNIFTY-I' : 'MidCapNifty1', 'NIFTY-I': 'Nifty1', 'BANKNIFTY-I' : 'BankNifty1'}
    modelDict = {'BANKNIFTY-I' : MODELS, 'MIDCPNIFTY-I' : MODELS, 'NIFTY-I': MODELS}
    
    # strategy = 'Daily_'+datetime.date.today().strftime('%d%b%Y')
    # Indices = {'RNS1 INDEX': 'MidCapNifty1', 'NZ1 INDEX': 'Nifty1', 'AF1 INDEX' : 'BankNifty1', 'NZ2 INDEX': 'Nifty2', 'AF2 INDEX' : 'BankNifty2'}#{'AF1 INDEX' : 'BankNifty'}#
    # modelDict = {'RNS1 INDEX': MODELS, 'NZ1 INDEX': MODELS, 'AF1 INDEX' : MODELS}#{'NZ2 INDEX': MODELS, 'AF2 INDEX' : MODELS}##,
    
    #Indices = {'NIFTY INDEX': 'Nifty', 'NSEBANK INDEX': 'BankNifty', 'NMIDSELP INDEX' : 'MidCapNiftySelect'}#, 'NSEMCAP INDEX' : 'MidCapNifty'}#{'AF1 INDEX' : 'BankNifty'}#
    #modelDict = {'NMIDSELP INDEX' : MODELS}#{'NIFTY INDEX': MODELS, 'NSEBANK INDEX': MODELS, 'NSEMCAP INDEX' : MODELS}#{'NZ2 INDEX': MODELS, 'AF2 INDEX' : MODELS}##,
    
    
    #modelDict = {'MIDCPNIFTY-I' : MODELS}# {'BANKNIFTY-I' : MODELS}
    #modelDict = {'NZ1 INDEX': [1, 12, 6], 'AF1 INDEX' : [12, 4, 13, 6, ]}
    
    basePath = os.path.join(dirPath+'TradingSignals_Indices', strategy)
    #flag = 'Hourly_WithCharges'
    if not os.path.exists(basePath):
        os.makedirs(basePath)
    for ind in modelDict.keys():
        for ModelNum in modelDict[ind]:
            a = DailyTradingSignalsIndexFuts(mydata, modelNum = ModelNum, indexName = ind)
            a.run()
            a.ResultFrameWithIndex()            
            print(dict([(value, key) for key, value in a.MODELS.items()])[ModelNum], 'Completed!')    
            backtestName = Indices[ind]+'_'+dict([(value, key) for key, value in a.MODELS.items()])[ModelNum]+'_'+strategy.split('_')[0]+'_'+datetime.datetime.today().date().strftime('%Y%m%d')
            filepath = os.path.join(basePath, backtestName+'.xlsx')#'_'+str(datetime.datet 0e.today().date())+
            a.savebacktestresult(filepath)
            backtestName = backtestName.replace(', ', '_').replace(' ', '_')
            navData = a.PlotResult.dropna()
            
            yrsDiff = (navData.index[-1] - navData.index[0]).days/365.0
            cagrRet = (navData.iloc[-1])**(1/yrsDiff) -1
            AverageChurn = int(100*a.Churn.resample('a').sum().mean())
            titlename = 'CAGR-'+ str(["%.1f" % i for i in (cagrRet.values*100)]) + ',Churn-'+str(AverageChurn)+'%, ' +backtestName
            
            navData.plot(title = titlename, figsize =(18,6))
            plt.savefig(os.path.join(basePath, backtestName+'_NAV.jpg'))   
            
            temp = navData.pct_change(12 if 'Monthly' in backtestName else 252)
            temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
            temp = pandas.DataFrame(temp)
            temp.columns = ['Rolling 1Yr Returns ' + titlename]
            temp['X-Axis'] = 0
            temp.plot(title = titlename, figsize =(18,6))
            plt.savefig(os.path.join(basePath, backtestName+'_RR.jpg'))
            
            tmp = navData.resample('a').last()
            tmp = tmp.pct_change()
            tmp.index = tmp.index.year
            tmp.plot(kind = 'bar', title = titlename, figsize =(18,6))
            plt.savefig(os.path.join(basePath, backtestName+'_Y_Plot.jpg'))
            
            tmp2 = navData.resample('q').last()
            tmp2 = tmp2.pct_change()
            tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
            tmp2.plot(kind = 'bar', title = titlename, figsize =(18,6))
            plt.savefig(os.path.join(basePath, backtestName+'_Q_Plot.jpg')) 
            
            tradeDF = pandas.DataFrame()
            df = a.trade_reg.get_trade_register()
            tradeDF = pandas.concat([tradeDF, df], axis = 0)
                
            filepath = os.path.join(basePath, backtestName+'_TradesRegister.xlsx')
            stats_obj = Stats(tradeDF)
            statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
            statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
            
            writer = pandas.ExcelWriter(filepath)
            tradeDF.to_excel(writer,'Trades')
            statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
            statsPositionDF.to_excel(writer,'Stats-Position')
            writer.save()
            writer.close()
    