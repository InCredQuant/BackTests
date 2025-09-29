# -*- coding: utf-8 -*-
"""
Created on Fri Jan  6 13:45:05 2025

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

class Volatility(BackTester):
    def __init__(self,data, Bucket = 1):
        BackTester.__init__(self, data)
        self.Bucket = Bucket # 1 Means Top Bucket (Lowest price to Book Value)
      
    def basicdatainitialize(self):        
        startDate = dt.date(2004, 12, 25)
        quarterlyData = self.BackTestData.Close.resample('q').last()
        quarterlyStartDate = quarterlyData.loc[startDate:, :].index[0]
        self.CurrentTime = self.Close.loc[startDate:quarterlyStartDate, :].index[-1]
      
        self.UpdateDates = [self.Close.loc[:it, :].index[-1] for it in quarterlyData.loc[self.CurrentTime :, :].index]
        self.TransactionCostRate = 0.0003#25# Total 3 bps Transaction Charges
        self.Volatility = np.sqrt(252)*(self.Close.pct_change().rolling(window = 252).std())
        
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
      
    def UpdateSpecificStats(self):
        indexConstituents = self.GetLatestIndexComponents()
        priceVol = self.Volatility.loc[self.CurrentTime].dropna()
        
        priceVol = priceVol.loc[priceVol.index.isin(indexConstituents)]
      
        priceVol = priceVol.rank(ascending = True)# Volatility is last 1 Yr Price Volatiltiy
        #Lowest Price Volatility during this period is given Best Rank( Bucket-1 is best  Bucket)
        priceVol = priceVol.sort_values()
        self.FactorRanks = priceVol[(self.Bucket-1)*int(0.2*len(priceVol)):self.Bucket*int(0.2*len(priceVol))]
            
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
    current_dir = os.getcwd()
    mydata.Factors = pd.DataFrame()
    for bucket in range(1, 6):
        print("Bucket-", bucket, " Processing!")
        model = Volatility(mydata, Bucket = bucket)
        model.run()
        
        model.ResultFrameWithIndex()
        backtestName = 'Vol-B'+ str(bucket)
        model.SavePlotsData(current_dir, backtestName = backtestName, fullData = False)        
        mydata.Factors = pd.concat([mydata.Factors, model.NAV.rename(columns=lambda x: x.replace('NAV', backtestName))], axis = 1)