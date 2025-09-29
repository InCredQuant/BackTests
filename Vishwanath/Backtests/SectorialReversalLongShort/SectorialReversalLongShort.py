# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 10:06:09 2022
@author: Viren@Incred

Select Top/Bottom 2 names from each sector based on some defined Reversal. Select only those Sectors where Consitutes count is more than 10
Sectors are GICS Based
For Reversal- 1Months reversal and 2 Weeks Reversal, Highest recent Out performers are taken short positions and recent underperformers are taken long positions

Finalizing the Sectorial Long Short based on 2 Week Price Reversal, 
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

LiqAllstocks = ['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN', 'TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'NFIL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'GUJGA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN']
excluded = ['ADE IN', 'ADSEZ IN', 'Z IN']#ACC & Ambuja Stocks are added back, 28 april 2023#['ACC IN', 'ADE IN', 'ADSEZ IN', 'ACEM IN'] # Adani holding Stocsk are Removed //1 Feb 2023
for tckr in excluded:
    if tckr in LiqAllstocks:
        LiqAllstocks.remove(tckr)


class SectorialReversalLongShort(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self,data)
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2022-12-27')#('2012-12-27')#('2022-12-27')
        #self.UpdateDates = []
        #ok = [self.UpdateDates.extend([self.Close.loc[:pandas.to_datetime(it) - datetime.timedelta(14), :].index[-1], it]) for it in self.ExpiryDates if pandas.to_datetime(it) >= self.CurrentTime]
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
        
    def UpdateSpecificStats(self): 
        stocksList = list(self.Close.loc[self.CurrentTime].dropna().index)
        #indexFactor = pandas.DataFrame(self.BackTestData.IndexInclusionFactor.loc[:self.CurrentTime].iloc[-1].dropna())#.index
        #stocksList = set.intersection(set(stocksList), set(indexFactor.index))
        indexFactor = pandas.DataFrame(LiqAllstocks, columns = ['Name'], index = LiqAllstocks)
        indexFactor = indexFactor.loc[indexFactor.index.isin(stocksList)]
        indexFactor['Sector'] = [self.BackTestData.GICS[ind] for ind in indexFactor.index]
        sectorGroup = indexFactor.groupby('Sector').count()
        sectorGroup = sectorGroup[sectorGroup>=10].dropna()
        
        self.FactorRanks = pandas.DataFrame()
        for iSector in sectorGroup.index:
            iSectorStockList = indexFactor[indexFactor['Sector'] == iSector]
            ReversalMom = pandas.DataFrame(self.Close.pct_change(10, fill_method = None).loc[self.CurrentTime, iSectorStockList.index].dropna())# Finalized 2 Weeks Returns for Reversal
            ReversalMom.columns = [iSector]
            ReversalMom = ReversalMom.drop_duplicates().rank(ascending = False) # Lower is better, Reversal
            self.FactorRanks = pandas.concat([self.FactorRanks, ReversalMom], axis = 1)
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.Position.loc[self.CurrentTime] = 0
        StocksCount = 4*len(self.FactorRanks.columns)
        for iSector in self.FactorRanks.columns:
            factorRanks = self.FactorRanks.loc[:, iSector].dropna()
            factorRanks = factorRanks.sort_values()
            longStocks = factorRanks.iloc[-2:].index # Top 10 Long
            shortStocks = factorRanks.iloc[:2].index # bottom 10 Short            
            for ticker in longStocks:
                self.CapitalAllocation.loc[self.CurrentTime, ticker] = self.CurrentNAV/StocksCount
                self.Position.loc[self.CurrentTime, ticker] = 1
            for ticker in shortStocks:
                self.CapitalAllocation.loc[self.CurrentTime, ticker] = -self.CurrentNAV/StocksCount
                self.Position.loc[self.CurrentTime, ticker] = -1
                
        CurrentPosition = self.Position.loc[self.CurrentTime]
        lastPosition = self.Position.loc[self.LastTime]
        position_to_Neutral = set.intersection(set(lastPosition[lastPosition !=0].dropna().index), set(CurrentPosition[CurrentPosition == 0].dropna().index))#1. Current Becomes Neutral, previous was Long or Short
        short_to_Long = set.intersection(set(lastPosition[lastPosition == -1].dropna().index), set(CurrentPosition[CurrentPosition == 1].dropna().index))#2. Current becomes Long, Previous was Short or neutral
        long_to_Short = set.intersection(set(lastPosition[lastPosition == 1].dropna().index), set(CurrentPosition[CurrentPosition == -1].dropna().index))#3. Current Becomes Short, Previous was Long or Neutral
        Changes = position_to_Neutral.union(short_to_Long, long_to_Short)  
        for iTicker in Changes:
            position = self.Position.loc[self.LastTime, iTicker]
            entry_date = self.DetectPostionStartDate(iTicker, self.Position)
            self.OrderPosition(iTicker, entry_date, position, strategyID = 'STFMSEC2W')
        if self.CurrentTime == max(self.UpdateDates):
            CurrentPosition = CurrentPosition[CurrentPosition != 0].dropna()
            for iTicker in CurrentPosition.index:
                position = self.Position.loc[self.CurrentTime, iTicker]
                entry_date = self.DetectPostionStartDate(iTicker, self.Position)
                self.OrderPosition(iTicker, entry_date, position, strategyID = 'STFMSEC2W', exit_date = self.CurrentTime)
        print(self.CurrentTime.date(), StocksCount)
                
if __name__=='__main__':
    import pickle
    
    picklePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
    
    dataFile = picklePath+'STFMSEC_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    
    basePath = 'G:/Shared drives/QuantFunds/Liquid1/LiveModels/ModelFiles/' 
    model = SectorialReversalLongShort(mydata)#iDict
    model.run()
    #a.ResultFrameWithIndex()
    backtestName = 'STFMSEC2W_'#
    filepath = basePath+backtestName+datetime.datetime.today().date().strftime('%Y%m%d')+'.xlsx'
    model.savebacktestresult(filepath)
        
    
    '''
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
    '''

