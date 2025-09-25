# -*- coding: utf-8 -*-
"""
Created on 13 Sep 2022

@author: Viren@InCred
Backtesting Long Short RSI and Relative Stgrength
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
import warnings
warnings.filterwarnings("ignore")

class ModelPort(FactoryBackTester):
    def __init__(self,data, Days, mDay, highlowMoM, rsiDay, highlowRSI):
        FactoryBackTester.__init__(self,data)        
        self.Days=Days
        self.mDay = mDay
        self.highlowMoM = highlowMoM
        self.highlowRSI = highlowRSI
        self.rsiDays = rsiDay
        
    def basicdatainitialize(self):        
        self.CurrentTime = pandas.to_datetime('2004-01-02')# '2013-01-07'
        self.GetAllUpdateDates(self.Days)
        #self.CurrentTime = self.BackTestData.ExpiryDates[30]
        #self.UpdateDates = list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.000

    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.BackTestData.Close.loc[self.CurrentTime]
        if self.rsiDays == 5:
            self.RSI = self.BackTestData.RSI5.resample('w-FRI', convention = 'end').last()
        elif self.rsiDays == 14:
            self.RSI = self.BackTestData.RSI14.resample('w-FRI', convention = 'end').last()
        
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True

    def UpdateSpecificStats(self):
        Indexfactor = self.BackTestData.IndexInclusionFactor.loc[:self.CurrentTime].iloc[-1].dropna()
        #last 6months RSI should be greater than 40, should not be in overbought zone or neither oversold
        rsi = self.RSI.loc[self.CurrentTime].dropna()
        rsi = rsi[rsi.index.isin(Indexfactor.index)].dropna().rank(ascending = self.highlowRSI)
        
        # relative strength is the proxy of momentum for cross sectional comparison taking price momentum only
        #10 days Price mom
        priceMom = self.BackTestData.Close.iloc[self.Counter]/self.BackTestData.Close.iloc[self.Counter-self.mDay]
        priceMom = priceMom[priceMom.index.isin(Indexfactor.index)].dropna().rank(ascending = self.highlowMoM)
        
        Finalfactor = rsi+ priceMom#pandas.concat([0.2*value, 0.40*quality, 0.40*momentum], axis = 1).sum(axis = 1)
        
        self.factor = Finalfactor        
        self.factor = self.factor.sort_values(ascending = True)
        
    def updateCapAllocation(self):
        if len(self.factor) > 0:
            self.CapitalAllocation.loc[self.CurrentTime] = 0
            cnt = 10
            longsecs = self.factor[-cnt:].index
            for ticker in longsecs:
                self.CapitalAllocation.loc[self.CurrentTime,ticker]= self.CurrentNAV*(1.0/len(longsecs))
            #print(self.CurrentTime.date(), cnt)
                
if __name__=='__main__':
    import pickle
    dataFile = 'G:/Shared drives/BackTests/Pickles/BSE100_RSIData13Sep-2022.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    rebal = {7: 'WeeklyRebal', 14 : 'FortNightRebal', 28 : 'MonthlyRebal', 91 : 'QuarterlyRebal'}
    MomDays = {5: '1W', 10 : '2W', 21 : '1M', 63: '3M', 126 :'6M', 252 :'1Yr'}
    HighLow = {True :'High', False : 'Low'}
    rsiDays = {5 : '5D', 14 : '14D'}
    basePath = 'G:/Shared drives/BackTests/Results/RSI+MOM/'
    
    cagrData = []
    for rPeriod in rebal.keys():
        for mDay in MomDays.keys():
            for hl in HighLow.keys():
                for rsiDay in rsiDays.keys():
                    for hl2 in HighLow.keys():
                        print('BSE100-'+HighLow[hl2]+'RSI('+rsiDays[rsiDay]+')-'+HighLow[hl]+'Mom('+MomDays[mDay]+')-'+rebal[rPeriod])
                        a = ModelPort(mydata, Days = rPeriod, mDay = mDay, highlowMoM = hl, rsiDay = rsiDay, highlowRSI = hl2)
                        a.run()
                        
                        #backtestName = 'BSE100-High-Mom(1M)-MonthlyRebal'
                        #backtestName = 'BSE100-'+HighLow[hl]+'-Mom('+MomDays[mDay]+')-'+rebal[rPeriod]
                        backtestName = 'BSE100-'+HighLow[hl2]+'RSI('+rsiDays[rsiDay]+')-'+HighLow[hl]+'Mom('+MomDays[mDay]+')-'+rebal[rPeriod]
                        
                        a.ResultFrameWithIndex()
                        #results = a.GetStatsReturns(Riskfree=0.05)
                        
                        filepath = basePath+backtestName+'_'+str(datetime.datetime.today().date())+'.xlsx'
                        #a.savebacktestresult(filepath)
                        backtestName = backtestName.replace(', ', '_').replace(' ', '_')
                        
                        yrsDiff = (a.PlotResult.index[-1] - a.PlotResult.index[0]).days/365.0
                        cagrRet = (a.PlotResult.iloc[-1])**(1/yrsDiff) -1
                        AverageChurn = int(100*a.Churn.resample('a').sum().mean())
                        titlename = 'CAGR-'+ str(["%.1f" % i for i in (cagrRet.values*100)]) + ',Churn-'+str(AverageChurn)+'%, ' +backtestName
                        
                        a.PlotResult.plot(title = titlename, figsize =(18,6))
                        #plt.savefig(basePath+backtestName+'_NAV.jpg')
                        
                        temp = a.PlotResult.pct_change(252)
                        temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
                        temp = pandas.DataFrame(temp)
                        temp.columns = ['Rolling 1Yr Returns ' + titlename]
                        temp['X-Axis'] = 0
                        temp.plot(title = titlename, figsize =(18,6))
                        #plt.savefig(basePath+backtestName+'_RR.jpg')
                        
                        tmp = a.PlotResult.resample('a').last()
                        tmp = tmp.pct_change()
                        tmp.index = tmp.index.year
                        tmp.plot(kind = 'bar', title = titlename, figsize =(18,6))
                        #plt.savefig(basePath+backtestName+'_Y_Plot.jpg')
                        
                        tmp2 = a.PlotResult.resample('q').last()
                        tmp2 = tmp2.pct_change()
                        tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
                        tmp2.plot(kind = 'bar', title = titlename, figsize =(18,6))
                        #plt.savefig(basePath+backtestName+'_Q_Plot.jpg')
                        
                        cagrData.append([backtestName, HighLow[hl], MomDays[mDay], HighLow[hl2], rsiDays[rsiDay], rebal[rPeriod], round(cagrRet.values[0]*100, 1), round(cagrRet.values[1]*100, 1), AverageChurn])
                        pandas.DataFrame(cagrData, columns =['Des', 'HighLow-MoM', 'MomDays', 'HighLow-RSI', 'RSIDays', 'RebalanceFreq', 'StrategyCAGR', 'IndexCAGR', 'Churn']).to_clipboard()