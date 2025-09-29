# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 11:04:28 2022

@author: viren@Incred
Testing RSI(2) with Trend (SMA50):
    1. Close Price > 50SMA => 1, Close Price < 50SMA => -1
    2. If Last RSI Position =1 and Last Date RSI > 90 and Current RSI < 90 => Current RSI Position -1, otherwise same position as last
    if Last RSI Position -1, and (Last Date RSI < 10 and Current RSI > 10) => Current RSI Position 1, otherwise same position as Last
    
    Final-> Long if both Long, Short if both short,, otherwise neutral
    apply 5% stopLoss, 

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
import warnings
warnings.filterwarnings("ignore")

class ModelPort(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self,data)
        
        
    def basicdatainitialize(self):        
        self.CurrentTime = pandas.to_datetime('2009-12-31')# '2013-01-07'
        #self.GetAllUpdateDates(self.Days)
        self.UpdateDates = list(self.BackTestData.Close.index)
        self.TransactionCostRate = 0.0015# 15 bps total, assuming 5bps transaction charges and 10 bps slippages
        self.RSIPosition=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.SMAPosition=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        #self.StartingPosition = -1
        #pdb.set_trace()
        self.StartingPosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close.iloc[-1, :]), index = self.BackTestData.Close.columns, columns = ['StartPosition'])
        self.StartingPosition.loc[:] = -1
        self.RSI = self.BackTestData.RSI2
        self.DMA = self.BackTestData.DMA50
        self.CapitalAllocation.loc[self.CurrentTime,:] = 100

    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.BackTestData.Close.loc[self.CurrentTime]
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True

    def UpdateSpecificStats(self):        
        rsi = self.RSI.loc[self.CurrentTime]#.dropna()
        dma = self.DMA.loc[self.CurrentTime]#.dropna()
        #pdb.set_trace()
        # 1. Close Price > 50SMA => 1, Close Price < 50SMA => -1
        # 2. If Last RSI Position =1 and Last Date RSI > 90 and Current RSI < 90 => Current RSI Position -1, otherwise same position as last
        # if Last RSI Position -1, and (Last Date RSI < 10 and Current RSI > 10) => Current RSI Position 1, otherwise same position as Last
        
        # Final-> Long if both Long, Short if both short,, otherwise neutral
        priceGrtrDMA = dma[self.CurrentPrice > dma].index # tickers where price > DMA
        self.SMAPosition.loc[self.CurrentTime, priceGrtrDMA] = 1 # assigning  trigger 1
        priceLessDMA = dma[self.CurrentPrice < dma].index
        self.SMAPosition.loc[self.CurrentTime, priceLessDMA] = -1
        
        # tickers where last RSI Position was -1 or Strating Position was -1
        startNeg = set(self.StartingPosition[self.StartingPosition == -1].dropna().index)
        prevRSINeg = set(rsi[(self.RSIPosition.loc[self.LastTime, :] == -1)].dropna().index)
        prevRSIPosNeg = set.union(prevRSINeg, startNeg)
        # where last RSI < 10 and Current RSI> 10, then assign RSI Position 1, if it was in upper set also
        rsiSignalChg = rsi[(self.RSI.loc[:self.LastTime].iloc[-1] < 10) & (rsi > 10)].index
        
        self.RSIPosition.loc[self.CurrentTime, list(set.intersection(prevRSIPosNeg, rsiSignalChg))] = 1 # RSI signal 1 where previous signal was -1 and currently signal have changed based on RSI
        self.StartingPosition.loc[list(set.intersection(prevRSIPosNeg, rsiSignalChg))] = 0
        
        noChangeTickers = set.intersection(set.difference(prevRSIPosNeg, set(rsiSignalChg)), startNeg)
        self.RSIPosition.loc[self.CurrentTime, noChangeTickers] = -1 # carry on last position if no change in RSI position
        
        maintainPos = set.difference(set.difference(prevRSIPosNeg, rsiSignalChg), noChangeTickers)
        self.RSIPosition.loc[self.CurrentTime, maintainPos] = self.RSIPosition.loc[self.LastTime, maintainPos]
        
        prevRSIPos = set(rsi[(self.RSIPosition.loc[self.LastTime, :] == 1)].dropna().index)
        rsiSignalChg2 = rsi[(self.RSI.loc[:self.LastTime].iloc[-1] > 90) & (rsi < 90)].index
        rsiSignalChg2 = set.intersection(prevRSIPos, prevRSIPos)
        self.RSIPosition.loc[self.CurrentTime, rsiSignalChg2] = -1
        maintainPos2 = set.difference(prevRSIPos, rsiSignalChg2)
        self.RSIPosition.loc[self.CurrentTime, maintainPos2] = self.RSIPosition.loc[self.LastTime, maintainPos2]
        
        # if self.RSIPosition.loc[self.LastTime, self.ticker] == -1 or self.StartingPosition == -1:
        #     if self.RSI.loc[:self.LastTime].iloc[-1] < 10 and self.RSI.loc[:self.CurrentTime].iloc[-1] > 10:
        #         self.RSIPosition.loc[self.CurrentTime, self.ticker] = 1
        #         self.StartingPosition = 0
        #     else:
        #         if self.StartingPosition == -1:
        #             self.RSIPosition.loc[self.CurrentTime, self.ticker] = -1
        #         else:
        #             self.RSIPosition.loc[self.CurrentTime, self.ticker] = self.RSIPosition.loc[self.LastTime, self.ticker]
        
        # elif self.RSIPosition.loc[self.LastTime, self.ticker] == 1:
        #     if self.RSI.loc[:self.LastTime].iloc[-1] > 90 and self.RSI.loc[:self.CurrentTime].iloc[-1] < 90:
        #         self.RSIPosition.loc[self.CurrentTime, self.ticker] = -1
        #     else:
        #         self.RSIPosition.loc[self.CurrentTime, self.ticker] = self.RSIPosition.loc[self.LastTime, self.ticker]
        
        
    def DetectPostionStartDate(self):
        tempList = self.Position.loc[:self.LastTime, self.ticker]
        tempList = tempList.sort_index(ascending = False)
        iDate = tempList.index[0]
        for iTime in list(tempList.index[:20]):
            if tempList.loc[iTime] != tempList.iloc[0]:
                self.TradeTakenDate = iDate
                break
            else:
                iDate = iTime
        
        
    def updateCapAllocation(self):
        smalong = self.SMAPosition.loc[self.CurrentTime][self.SMAPosition.loc[self.CurrentTime] == 1].index        
        rsilong = self.RSIPosition.loc[self.CurrentTime][self.RSIPosition.loc[self.CurrentTime] == 1].index 
        long = set.intersection(set(smalong), set(rsilong))
        self.Position.loc[self.CurrentTime, long] = 1
        
        smashort = self.SMAPosition.loc[self.CurrentTime][self.SMAPosition.loc[self.CurrentTime] == -1].index        
        rsishort = self.RSIPosition.loc[self.CurrentTime][self.RSIPosition.loc[self.CurrentTime] == -1].index
        short = set.intersection(set(smashort), set(rsishort))
        self.Position.loc[self.CurrentTime, short] = 1
        
        others = set.difference(set(self.Position.columns), set.union(set(long), set(short)))
        self.Position.loc[self.CurrentTime, others] = 0
            
        #self.CurrentPosition = self.Position.loc[self.CurrentTime, self.ticker]
        #self.LongNumber = (self.CurrentPosition == 1).sum()
        #self.ShortNumber = (self.CurrentPosition == -1).sum()
        #self.TotalPosition = self.LongNumber  +  self.ShortNumber
        #self.CapitalAllocation.loc[self.CurrentTime,:]= 100
        for ticker in long:
            self.CapitalAllocation.loc[self.CurrentTime,ticker]= 1.0*self.CapitalAllocation.loc[self.LastTime,ticker]#self.CurrentNAV#/self.TotalPosition
        for ticker in short:
            self.CapitalAllocation.loc[self.CurrentTime,ticker]= -1.0*self.CapitalAllocation.loc[self.LastTime,ticker]#self.CurrentNAV#/self.TotalPosition
        #for ticker in others:
        #self.CapitalAllocation.loc[self.CurrentTime,ticker]= 100

                
if __name__=='__main__':
    import pickle
    dataFile = 'Z:/Pickles/BSE100_RSI_DMAFutData26Sep-2022.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    basePath = 'Z:/BacktestsResults/RSI2withTrend/'
    #print('BSE100-'+HighLow[hl2]+'RSI('+rsiDays[rsiDay]+')-'+HighLow[hl]+'Mom('+MomDays[mDay]+')-'+rebal[rPeriod])
    finalDF = pandas.DataFrame()
    
    #for ticker in mydata.Close.columns:
    a = ModelPort(mydata)
    a.run()
    a.ResultFrameWithIndex()
    a.savebacktestresult(basePath+'DailyRSIwithTrendLongShort.xlsx')
    '''
    
    ticker = 'INFO IN'
    ticker = ticker.replace('/', '_')
    backtestName = ticker + '-RSI(2)-50DMA'
    #backtestName = 'BSE100-'+HighLow[hl]+'-Mom('+MomDays[mDay]+')-'+rebal[rPeriod]
    #backtestName = 'BSE100-'+HighLow[hl2]+'RSI('+rsiDays[rsiDay]+')-'+HighLow[hl]+'Mom('+MomDays[mDay]+')-'+rebal[rPeriod]
    
    a.ResultFrameWithIndex()
    #results = a.GetStatsReturns(Riskfree=0.05)
    
    filepath = basePath+backtestName+'_'+str(datetime.datetime.today().date())+'.xlsx'
    #a.savebacktestresult(filepath)
    backtestName = backtestName.replace(', ', '_').replace(' ', '-')
    navData = a.PlotResult.dropna()
    
    yrsDiff = (navData.index[-1] - navData.index[0]).days/365.0
    cagrRet = (navData.iloc[-1])**(1/yrsDiff) -1
    AverageChurn = 0#int(100*a.Churn.resample('a').sum().mean())
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
    
    a.NAV.columns = [ticker]
    finalDF = pandas.concat([finalDF, a.NAV], axis = 1)
    
    #cagrData.append([backtestName, HighLow[hl], MomDays[mDay], HighLow[hl2], rsiDays[rsiDay], rebal[rPeriod], round(cagrRet.values[0]*100, 1), round(cagrRet.values[1]*100, 1), AverageChurn])
    #pandas.DataFrame(cagrData, columns =['Des', 'HighLow-MoM', 'MomDays', 'HighLow-RSI', 'RSIDays', 'RebalanceFreq', 'StrategyCAGR', 'IndexCAGR', 'Churn']).to_clipboard()
    
    '''