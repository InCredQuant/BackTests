# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 19:57:59 2025

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

class EqualWeight(BackTester):
    def __init__(self,data):
        BackTester.__init__(self, data)
      
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
        self.FactorRanks = self.Close.loc[self.CurrentTime].dropna()#priceMom[(self.Bucket-1)*int(0.2*len(priceMom)):self.Bucket*int(0.2*len(priceMom))]
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.CapitalAllocation.loc[self.CurrentTime, self.FactorRanks.index] = self.CurrentNAV*1.0/len(self.FactorRanks)        

if __name__=='__main__':
    import pickle
    dataFile = 'G:/Shared drives/BackTests/pycode/LongOnly/Viren/ProjectData.pkl'#'Viren/ProjectData.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()

    Indices = ['NIFTY INDEX', 'NSEMCAP INDEX', 'NSESMCP INDEX', 'NSEAUTO INDEX', 'NSEBANK INDEX', 'NSEFMCG INDEX', 'NSEINFR INDEX', 'NSEIT INDEX',  'NSEMET INDEX', 'NSEPHRM INDEX']
    
    modeldata = MyBacktestData()
    modeldata.Close = mydata.Assets.loc[:, Indices]
    modeldata.TargetPrice = mydata.TargetPrice.loc[:, Indices]
    modeldata.indexprice = mydata.indexprice
    
    current_dir = os.getcwd()
    print("Equal Weight Processing!")
    model = EqualWeight(modeldata)
    model.run()
    
    
    model.ResultFrameWithIndex()
    backtestName = 'EqulWeight-Indices'
    model.SavePlotsData(current_dir, backtestName = backtestName, fullData = False)        
    #mydata.Factors = pd.concat([mydata.Factors, model.NAV.rename(columns=lambda x: x.replace('NAV', backtestName))], axis = 1)