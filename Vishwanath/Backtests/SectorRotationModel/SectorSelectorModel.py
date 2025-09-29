# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 16:26:16 2022

@author: Viren@Incred

Sector Selector based on different Momentum and reversal parameters

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

class SectorSelectorModel(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self,data)
        
    def basicdatainitialize(self):        
        self.CurrentTime = pandas.to_datetime('2012-12-27')#.date()
        #self.GetAllUpdateDates()
        self.UpdateDates = self.ExpiryDates#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.000#25# Total 2 bps Transaction Charges        
        self.order = Order()
        self.trade_reg = TradeRegister()
        self.CloseIndices = self.BackTestData.CloseIndices
        self.Sectors = {}
        self.DMA20Indices = MyTechnicalLib.MovingAverage(self.BackTestData.CloseIndices, 20)
        self.DMA50Indices = MyTechnicalLib.MovingAverage(self.BackTestData.CloseIndices, 50)
        self.DMA200Indices = MyTechnicalLib.MovingAverage(self.BackTestData.CloseIndices, 200)
        
        self.DMA20 = MyTechnicalLib.MovingAverage(self.BackTestData.CloseStocks, 20)
        self.DMA50 = MyTechnicalLib.MovingAverage(self.BackTestData.CloseStocks, 50)
        self.DMA200 = MyTechnicalLib.MovingAverage(self.BackTestData.CloseStocks, 200)
        
            
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
    
    def UpdateOrderPositions(self):        
        CurrentPosition = self.Position.loc[self.CurrentTime]
        lastPosition = self.Position.loc[self.LastTime]
        position_to_Neutral = set.intersection(set(lastPosition[lastPosition !=0].dropna().index), set(CurrentPosition[CurrentPosition == 0].dropna().index))#1. Current Becomes Neutral, previous was Long or Short
        short_to_Long = set.intersection(set(lastPosition[lastPosition == -1].dropna().index), set(CurrentPosition[CurrentPosition == 1].dropna().index))#2. Current becomes Long, Previous was Short or neutral
        long_to_Short = set.intersection(set(lastPosition[lastPosition == 1].dropna().index), set(CurrentPosition[CurrentPosition == -1].dropna().index))#3. Current Becomes Short, Previous was Long or Neutral
        Changes = position_to_Neutral.union(short_to_Long, long_to_Short)  
        for iTicker in Changes:
            position = self.Position.loc[self.LastTime, iTicker]
            entry_date = self.DetectPostionStartDate(iTicker, self.Position)
            self.OrderPosition(iTicker, entry_date, position)
        if self.CurrentTime == max(self.UpdateDates):
            CurrentPosition = CurrentPosition[CurrentPosition != 0].dropna()
            for iTicker in CurrentPosition.index:
                position = self.Position.loc[self.CurrentTime, iTicker]
                entry_date = self.DetectPostionStartDate(iTicker, self.Position)
                self.OrderPosition(iTicker, entry_date, position, self.CurrentTime)
    
    def GetCrossMomRanks(self, ClosePrice, stocksList):
        SixMonthsMom = ClosePrice.pct_change(126, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        SixMonthsStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-126:].dropna().std()#*numpy.sqrt(252)        
        
        SixMonthsMom = SixMonthsMom.rank(ascending = True) # Higher is better
        SixMonthsStd = SixMonthsStd.rank(ascending = False) # Lower is better
        
        ThreeMonthsMom = ClosePrice.pct_change(63, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        ThreeMonthsStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-63:].dropna().std()#*numpy.sqrt(252)        
        
        ThreeMonthsMom = ThreeMonthsMom.rank(ascending = True) # Higher is better
        ThreeMonthsStd = ThreeMonthsStd.rank(ascending = False) # Lower is better
        
        TwoWkMom = ClosePrice.pct_change(11, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        TwoWkMom = TwoWkMom.rank(ascending = False) # Lower is better, Reversal
        TwoWkMom = TwoWkMom.sort_values()
              
        combinedMomRank = pandas.concat([SixMonthsMom, SixMonthsStd, ThreeMonthsMom], axis = 1)#SixMonthsStd, ThreeMonthsMom        
        combinedMomRank = combinedMomRank.mean(axis = 1).dropna().rank(ascending = True)
        combinedMomRank = combinedMomRank.sort_values()
        ReversalNames = combinedMomRank.iloc[-int(len(combinedMomRank)/2):].index
        self.TopCombinedMomRank = combinedMomRank
        self.TwoWkReversal = TwoWkMom.loc[ReversalNames].sort_values()
        
    
    def UpdateSpecificStats(self):        
        sectorList = list(self.CloseIndices.columns)
        if 'NIFTY INDEX' in sectorList:
            sectorList.remove('NIFTY INDEX')
        ChosenSectorList = []
        for iSector in sectorList:
            if len(self.BackTestData.IndexInclusionFactor[iSector].loc[:self.CurrentTime].iloc[-1].dropna())>0:
                ChosenSectorList.append(iSector)
        sectorList = ChosenSectorList
        
        self.GetCrossMomRanks(self.CloseIndices, sectorList)
        RevSector = list(self.TwoWkReversal.iloc[-1:].index)
        MomSector = self.TopCombinedMomRank.loc[~self.TopCombinedMomRank.index.isin(RevSector)]
        MomSector = MomSector.sort_values()
        MomSector = list(MomSector.iloc[-3:].index)
        
        MomSector.extend(RevSector)
        self.FactorStocks = self.TopCombinedMomRank.loc[MomSector]
        
        tsMom = self.CloseIndices.loc[self.CurrentTime, sectorList]/self.DMA50Indices.loc[self.CurrentTime, sectorList]
        tsMom = tsMom.rank(ascending = True)
        tsMom = tsMom.sort_values()
        
        SectorDMARatio = {}
        for iSector in sectorList:
            SectorUniverse = set(self.BackTestData.IndexInclusionFactor[iSector].loc[:self.CurrentTime].iloc[-1].dropna().index)
            SectorUniverse = set.intersection(SectorUniverse, set(self.BackTestData.CloseStocks.columns))
            dmaRatio = self.BackTestData.CloseStocks.loc[self.CurrentTime, SectorUniverse]/self.DMA200.loc[self.CurrentTime, SectorUniverse]
            dmaRatio = dmaRatio.dropna()
            dmaRatio = dmaRatio[dmaRatio>1].count()/dmaRatio.count()
            SectorDMARatio[iSector] = dmaRatio
        tsMomCount = pandas.Series(SectorDMARatio).dropna()
           
    def updateCapAllocation(self):
        indexName = 'NIFTY INDEX'  
        self.CapitalAllocation.loc[self.CurrentTime] = 0
        self.Position.loc[self.CurrentTime, indexName] = -1              
        if len(self.FactorStocks) >0 :
            self.Position.loc[self.CurrentTime] = 0
            self.FactorStocks = self.FactorStocks.sort_values()
            StocksDF = self.FactorStocks.iloc[-4:].index
            for ticker in StocksDF:#.index:
                self.CapitalAllocation.loc[self.CurrentTime, ticker] = self.CurrentNAV*1.0/len(StocksDF)
                self.Position.loc[self.CurrentTime, ticker] = 1
            self.Position.loc[self.CurrentTime, indexName] = -1
            self.CapitalAllocation.loc[self.CurrentTime, indexName] = -self.CurrentNAV
        self.UpdateOrderPositions()        
        print(self.CurrentTime.date(), len(self.FactorStocks), sep = ' ')
                
if __name__=='__main__':
    import pickle
    dirPath = 'Z:/'#'G:/Shared drives/BackTests/'# 'Z:/'
    dataFile = dirPath +'Pickles/SectorRotation_Data24Nov-2022.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    basePath = dirPath+'BacktestsResults/SectorRotation/Sectors/'
    
    mydata.CloseStocks = mydata.Close
    mydata.Close = mydata.CloseIndices
    
    a = SectorSelectorModel(mydata)
    a.run()
    a.ResultFrameWithIndex()
    backtestName = 'Sectors-Model'
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
