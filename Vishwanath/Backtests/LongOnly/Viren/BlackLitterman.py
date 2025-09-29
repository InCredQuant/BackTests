# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 20:15:32 2025

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
from GetData import *
import os
import warnings
warnings.filterwarnings("ignore")

class BlackLitterman(BackTester):
    def __init__(self,data, lambda_risk = 2):
        BackTester.__init__(self, data)
        self.lambda_risk = lambda_risk
      
    def basicdatainitialize(self):        
        startDate = dt.date(2004, 12, 25)
        quarterlyData = self.BackTestData.Close.resample('q').last()
        quarterlyStartDate = quarterlyData.loc[startDate:, :].index[0]
        self.CurrentTime = self.Close.loc[startDate:quarterlyStartDate, :].index[-1]
      
        self.UpdateDates = [self.Close.loc[:it, :].index[-1] for it in quarterlyData.loc[self.CurrentTime :, :].index]
        self.TransactionCostRate = 0.0003#25# Total 3 bps Transaction Charges        
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
      
    def UpdateSpecificStats(self):
        # First we have to make the views suppose with some condifence interval
        # For 
        targetPrice = self.BackTestData.TargetPrice.loc[:self.CurrentTime, :].iloc[-1]#.dropna()
        closePrice = self.Close.loc[self.CurrentTime, targetPrice.index]#, targetPrice.index]
        
        absTarget = targetPrice/closePrice -1
        
        views = np.array([[0 for _ in range(len(self.BackTestData.TargetPrice.columns))] for _ in range(4)]) #Plan is to have 4 views about the assets, 2 relative and 2 absolute
        view_returns = [0, 0, 0, 0]
        
        # Suppose First two are absolute views
        #First is View aboute the Nifty
        views[0][0] = 0 if np.isnan(absTarget.loc['NIFTY INDEX']) else 1
        view_returns[0] = 0 if np.isnan(absTarget.loc['NIFTY INDEX']) else absTarget.loc['NIFTY INDEX'] 
        
        
        # Second view is about the NSE Bank Index
        views[1][5] = 0 if np.isnan(absTarget.loc['NSEBANK INDEX'])  else 1
        view_returns[1] = 0 if np.isnan(absTarget.loc['NSEBANK INDEX']) else absTarget.loc['NSEBANK INDEX']
        
        
        # Third is about Relative of Mid Cap vs Small Cap
        views[2][1] = 0 if np.isnan(absTarget.loc['NSEMCAP INDEX'] - absTarget.loc['NSESMCP INDEX']) else 1
        views[2][2] = 0 if np.isnan(absTarget.loc['NSEMCAP INDEX'] - absTarget.loc['NSESMCP INDEX']) else -1
        
        view_returns[2] = 0 if np.isnan(absTarget.loc['NSEMCAP INDEX'] - absTarget.loc['NSESMCP INDEX']) else (absTarget.loc['NSEMCAP INDEX'] - absTarget.loc['NSESMCP INDEX'])
        
        
        # Fourth is About Auto & Pharma Comparison
        views[3][3] = 0 if np.isnan(absTarget.loc['NSEAUTO INDEX'] - absTarget.loc['NSEPHRM INDEX']) else 1
        views[3][9] = 0 if np.isnan(absTarget.loc['NSEAUTO INDEX'] - absTarget.loc['NSEPHRM INDEX']) else -1
        
        view_returns[3] = 0 if np.isnan(absTarget.loc['NSEAUTO INDEX'] - absTarget.loc['NSEPHRM INDEX']) else (absTarget.loc['NSEAUTO INDEX'] - absTarget.loc['NSEPHRM INDEX'])
        
        
        # 4 confidence levels (one for each view), for Absolute Returns, we will have lower confidence interval -> higher 1- alpha
        view_confidences = np.array([0.05, 0.1, 0.02, 0.03])
        
        n = len(closePrice.index)
        prior_weights = np.array([1/n] * n)
        
        sigma = 252*self.Close.loc[:self.CurrentTime, targetPrice.index].pct_change().iloc[-252:].cov()
        
        rf = self.BackTestData.RiskFreeAsset.loc[:self.CurrentTime].pct_change(252).iloc[-1]
        self.opt_Weight = self.BlackLittermanOptimizer(prior_weights, sigma, rf, self.lambda_risk, views, view_confidences, tau=0.025, margin_req=0.0, max_lever=1.5, min_pos=-0.1, max_pos=0.3)        
        
        self.FactorRanks = pd.Series(self.opt_Weight, index = sigma.index)#self.Close.loc[self.CurrentTime].dropna()#priceMom[(self.Bucket-1)*int(0.2*len(priceMom)):self.Bucket*int(0.2*len(priceMom))]
        #self.FactorRanks = self.FactorRanks/self.FactorRanks.sum()
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.CapitalAllocation.loc[self.CurrentTime] = self.CurrentNAV*self.FactorRanks#1.0/len(self.FactorRanks)
        #self.CapitalAllocation.loc[self.CurrentTime, 'Lquid'] = self.CurrentNAV*(1- self.FactorRanks.abs().sum())

if __name__=='__main__':
    import pickle
    dataFile = 'G:/Shared drives/BackTests/pycode/LongOnly/Viren/ProjectData.pkl'#'Viren/ProjectData.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()

    Indices = ['NIFTY INDEX', 'NSEMCAP INDEX', 'NSESMCP INDEX', 'NSEAUTO INDEX', 'NSEBANK INDEX', 'NSEFMCG INDEX', 'NSEINFR INDEX', 'NSEIT INDEX',  'NSEMET INDEX', 'NSEPHRM INDEX']
    
    modeldata = MyBacktestData()
    modeldata.TargetPrice = mydata.TargetPrice.loc[:, Indices]
    Indices.append('Liquid')
    modeldata.Close = mydata.Assets.loc[:, Indices]    
    modeldata.indexprice = mydata.indexprice
    modeldata.RiskFreeAsset = mydata.Asset.loc[:, 'Liquid']
    
    current_dir = os.getcwd()
    print("BL Processing!")
    for lambD in [ 1, 2, 3, 4, 5, 6]:
        model = BlackLitterman(modeldata, lambD)
        model.run()
        
        model.ResultFrameWithIndex()
        backtestName = 'BlackLitterMan_'+str(lambD)
        model.SavePlotsData(current_dir, backtestName = backtestName, fullData = False)        
        #mydata.Factors = pd.concat([mydata.Factors, model.NAV.rename(columns=lambda x: x.replace('NAV', backtestName))], axis = 1)