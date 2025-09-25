# -*- coding: utf-8 -*-
"""
Created on Fri Jan  6 13:25:05 2025

@author: Virendra
"""
import importlib
import BackTester
importlib.reload(BackTester)
from BackTester import BackTester
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import pdb

import os
import warnings
warnings.filterwarnings("ignore")

class Size(BackTester):
    def __init__(self,data, Bucket = 1):
        BackTester.__init__(self, data)
        self.Bucket = Bucket # 1 Means Top Bucket (Lowest price to Book Value)
      
    def basicdatainitialize(self):        
        startDate = dt.date(2004, 12, 25)
        quarterlyData = self.BackTestData.Close.resample('q').last()
        quarterlyStartDate = quarterlyData.loc[startDate:, :].index[0]
        self.CurrentTime = self.Close.loc[startDate:quarterlyStartDate, :].index[-1]
      
        self.UpdateDates = [self.Close.loc[:it, :].index[-1] for it in quarterlyData.loc[self.CurrentTime :, :].index]
        self.TransactionCostRate = 0.000#25# Total 2 bps Transaction Charges        
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
      
    def UpdateSpecificStats(self):
        indexConstituents = self.GetLatestIndexComponents()
        mCap = self.BackTestData.MarketCap.loc[:self.CurrentTime, : ].iloc[-1].dropna()
        mCap = mCap.loc[mCap.index.isin(indexConstituents)]
      
        mCap = mCap.rank(ascending = True)# Lower Market Cap is given Lowest Rank (Lowest 1 is assumed Best) 
        mCap = mCap.sort_values()
        self.FactorRanks = mCap[(self.Bucket-1)*int(0.2*len(mCap)):self.Bucket*int(0.2*len(mCap))]
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.CapitalAllocation.loc[self.CurrentTime, self.FactorRanks.index] = self.CurrentNAV*1.0/len(self.FactorRanks)        
        #print(self.CurrentTime.date(), len(self.FactorRanks), "Bucket:",self.Bucket, sep = ' ')

if __name__=='__main__':
    import pickle
    dataFile = 'Data20250102.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    current_dir = os.getcwd()#'G:/Shared drives/BackTests/pycode/LongOnly'#os.getcwd()
    mydata.Factors = pd.DataFrame()
    for bucket in range(1, 6):
        print("Bucket-", bucket, " Processing!")
        model = Size(mydata, Bucket = bucket)
        model.run()
        
        model.ResultFrameWithIndex()
        backtestName = 'Size-'+ str(bucket)
        model.SavePlotsData(current_dir, backtestName = backtestName, fullData = False)
        
        mydata.Factors = pd.concat([mydata.Factors, model.NAV.rename(columns=lambda x: x.replace('NAV', backtestName))], axis = 1)
        # model.savebacktestresult(os.path.join(current_dir,'Files/'+ backtestName+'.xlsx'), fullData = False)
        # backtestName = backtestName.replace(', ', '_').replace(' ', '_')
        # navData = model.PlotResult.dropna()
        
        # yrsDiff = (navData.index[-1] - navData.index[0]).days/365.0
        # cagrRet = (navData.iloc[-1])**(1/yrsDiff) -1
        # AverageChurn = int(100*model.Churn.resample('a').sum().mean())
        # titlename = 'CAGR-'+ str(["%.1f" % i for i in (cagrRet.values*100)]) + ',Churn-'+str(AverageChurn)+'%, ' +backtestName
    
        # navData.plot(title = titlename, figsize =(18,6))
        # plt.savefig(os.path.join(current_dir,'Figures/'+backtestName+'_NAV.jpg')) 
        
        # temp = navData.pct_change(252)
        # temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
        # temp = pd.DataFrame(temp)
        # temp.columns = ['Rolling 1Yr Returns ' + titlename]
        # temp['X-Axis'] = 0
        # temp.plot(title = titlename, figsize =(18,6))
        # plt.savefig(os.path.join(current_dir,'Figures/'+backtestName+'_RR.jpg'))
        
        # tmp = navData.resample('a').last()
        # tmp = tmp.pct_change()
        # tmp.index = tmp.index.year
        # tmp.plot(kind = 'bar', title = titlename, figsize =(18,6))
        # plt.savefig(os.path.join(current_dir,'Figures/'+backtestName+'_Y_Plot.jpg'))
        
        # tmp2 = navData.resample('q').last()
        # tmp2 = tmp2.pct_change()
        # tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
        # tmp2.plot(kind = 'bar', title = titlename, figsize =(18,6))
        # plt.savefig(os.path.join(current_dir,'Figures/'+backtestName+'_Q_Plot.jpg'))