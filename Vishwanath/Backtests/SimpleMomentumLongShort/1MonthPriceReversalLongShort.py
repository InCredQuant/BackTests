# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 11:30:00 2022
@author: Viren@Incred
Selecting Top 10 for Long  and Bottom 10 for Short:
    Based on 6 Months Cross Sectional price Momentum
Finalised- 6Months Price Momentum, 1yr Low Vol, and 1 Months Price Reversal
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
excluded = ['ADE IN', 'ADSEZ IN', 'Z IN']
for tckr in excluded:
    if tckr in LiqAllstocks:
        LiqAllstocks.remove(tckr)
        
class SimpleMomenutmLongShort(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self,data)
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2022-12-27')#('2022-12-27')#
        self.UpdateDates = self.ExpiryDates#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.000#25# Total 2 bps Transaction Charges  
        self.StopLossLimit = -0.10
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.order = Order()
        self.trade_reg = TradeRegister()
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
    '''
    def StopLossHandler(self):
        self.PositionWOSL.loc[self.CurrentTime] = self.PositionWOSL.loc[self.LastTime]
        positionWOSL = self.PositionWOSL.loc[self.CurrentTime]
        tempDF = positionWOSL[positionWOSL != 0]
        
        for ticker in tempDF.index:
            #self.StopLoss(ticker, self.StopLossLimit)
            self.StopLossTrail(ticker, self.StopLossLimit)
        
        self.Position.loc[self.CurrentTime] = self.PositionWOSL.loc[self.CurrentTime]
        for ticker in tempDF.index:
            TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL)
            checkExitPosition = self.PositionExitDF.loc[TradeTakenDate:self.CurrentTime, ticker].dropna()
            if len(checkExitPosition) >0:
                oldPosNifty = self.Position.loc[self.CurrentTime, 'NIFTY INDEX']
                oldPosTicker = self.Position.loc[self.CurrentTime, ticker]
                newPos = oldPosNifty+ oldPosTicker
                
                self.Position.loc[self.CurrentTime, 'NIFTY INDEX'] = numpy.sign(newPos)
                if numpy.sign(oldPosNifty)*numpy.sign(oldPosTicker) == -1:
                    self.Position.loc[self.CurrentTime, 'Cash'] = 1
                    self.CapitalAllocation.loc[self.CurrentTime, 'Cash'] += (2*numpy.minimum(numpy.abs(self.CapitalAllocation.loc[self.CurrentTime, ticker]), numpy.abs(self.CapitalAllocation.loc[self.CurrentTime, 'NIFTY INDEX'])))
                self.CapitalAllocation.loc[self.CurrentTime, 'NIFTY INDEX'] += self.CapitalAllocation.loc[self.CurrentTime, ticker]
                self.CapitalAllocation.loc[self.CurrentTime, ticker] = 0
                self.Position.loc[self.CurrentTime, ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, ticker] = self.PositionWOSL.loc[self.CurrentTime, ticker]
    '''
    def GetMomRanks(self, ClosePrice, stocksList):       
        # OneYrEx1MMom = ClosePrice.pct_change(231, fill_method = None).loc[self.ExpiryDates[list(self.ExpiryDates).index(self.CurrentTime)-1], stocksList].dropna()
        
        # OneYrMom = ClosePrice.pct_change(252, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        #OneYrStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-252:].dropna().std()*numpy.sqrt(252)        
        
        # OneYrMom = OneYrMom.rank(ascending = True) # Higher is better
        #OneYrStd = OneYrStd.rank(ascending = False) # Lower is better
        
        #SixMonthsMom = ClosePrice.pct_change(126, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        # SixMonthsStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-126:].dropna().std()*numpy.sqrt(252)        
        
        # SixMonthsMom = SixMonthsMom.rank(ascending = True) # Higher is better
        # SixMonthsStd = SixMonthsStd.rank(ascending = False) # Lower is better
        
        # ThreeMonthsMom = ClosePrice.pct_change(63, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        # ThreeMonthsStd = ClosePrice.pct_change(fill_method = None).loc[:self.CurrentTime, stocksList].iloc[-63:].dropna().std()#*numpy.sqrt(252)        
        
        # ThreeMonthsMom = ThreeMonthsMom.rank(ascending = True) # Higher is better
        # ThreeMonthsStd = ThreeMonthsStd.rank(ascending = False) # Lower is better
        
        OneMonthMom = ClosePrice.pct_change(21, fill_method = None).loc[self.CurrentTime, stocksList].dropna()
        OneMonthMom = OneMonthMom.rank(ascending = False) # Lower is better, Reversal
        OneMonthMom = OneMonthMom.sort_values()
        
        self.MomentumRanks = OneMonthMom
        #self.VolRanks = OneYrStd
        
    def UpdateSpecificStats(self):
        stocksList = list(self.Close.columns)
        #indexNames = self.BackTestData.IndexInclusionFactor.loc[:self.CurrentTime].iloc[-1].dropna().index
        indexNames = LiqAllstocks
        stocksList = set.intersection(set(stocksList), set(indexNames))      
        self.GetMomRanks(self.Close, stocksList)        
        self.FactorRanks = self.MomentumRanks
            
    def updateCapAllocation(self):
        self.CapitalAllocation.loc[self.CurrentTime] = 0           
        if len(self.FactorRanks) >0:
            self.FactorRanks = self.FactorRanks.sort_values()
            longStocks = self.FactorRanks.iloc[-10:].index # Top 10 Long
            shortStocks = self.FactorRanks.iloc[:10].index # bottom 10 Short
            self.Position.loc[self.CurrentTime] = 0
            self.PositionWOSL.loc[self.CurrentTime] = 0
            #for ticker in longStocks:
            self.CapitalAllocation.loc[self.CurrentTime, longStocks] = self.CurrentNAV/(len(longStocks) + len(shortStocks))
            self.Position.loc[self.CurrentTime, longStocks] = 1
            self.PositionWOSL.loc[self.CurrentTime, longStocks] = 1
            
            #for ticker in shortStocks:
            self.CapitalAllocation.loc[self.CurrentTime, shortStocks] = -self.CurrentNAV/(len(longStocks) + len(shortStocks))
            self.Position.loc[self.CurrentTime, shortStocks] = -1
            self.PositionWOSL.loc[self.CurrentTime, shortStocks] = -1
            
        self.UpdateOrderBook(strategyID = 'STFMREV1M')
        print(self.CurrentTime.date(), len(longStocks) + len(shortStocks))
                
if __name__=='__main__':
    import pickle
    #dirPath = 'Z:/'#'G:/Shared drives/BackTests/'# 'Z:/'
    picklePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
    f = open(picklePath+'STFM_Stocks_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl', 'rb')    
    #dataFile = dirPath +'Pickles/BSE100_FutsData_SimpleMomentumLongShort18Apr-2023.pkl'
    #f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    #basePath = dirPath+'BacktestsResults/SimpleMomLongShort/'
    
    basePath = 'G:/Shared drives/QuantFunds/Liquid1/LiveModels/ModelFiles/' 
    backtestName = 'STFMREV1M_'#
    
    model = SimpleMomenutmLongShort(mydata)
    model.run()
    #model.ResultFrameWithIndex()
    #backtestName = 'BSE100Futs_1MPriceReversal_LongShort'
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