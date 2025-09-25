# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30 12:10:24 2022

@author: Viren@InCred
Dynamic Selection-Live Model
"""

from FactoryBackTester import FactoryBackTester
import MyTechnicalLib
import pandas
import numpy
import datetime
import pdb
import matplotlib.pyplot as plt
import copy
from random import choice
import pdb
import warnings
warnings.filterwarnings("ignore")

from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter
TRADE_REGISTER = 'AllTrades_TradesRegister2025-09-03.xlsx'

#liqList = ['ABB IN', 'ABFRL IN', 'ADE IN', 'ADSEZ IN', 'AL IN', 'APHS IN', 'ASTRA IN', 'AUBANK IN', 'AXSB IN', 'BAF IN', 'BHARTI IN', 'BHE IN', 'BHFC IN', 'BIL IN', 'BJFIN IN', 'BOB IN', 'BRIT IN', 'CIFC IN', 'COAL IN', 'COFORGE IN', 'DIVI IN', 'DIXON IN', 'DLFU IN', 'DN IN', 'DRRD IN', 'GCPL IN', 'GPL IN', 'GRASIM IN', 'GUJGA IN', 'HAVL IN', 'HCLT IN', 'HDFCAMC IN', 'HDFCB IN', 'HNAL IN', 'HNDL IN', 'HUVR IN', 'ICICIBC IN', 'ICICIGI IN', 'IDFCFB IN', 'IGL IN', 'IH IN', 'IIB IN', 'INDIGO IN', 'INFO IN', 'INFOE IN', 'IOCL IN', 'IRCTC IN', 'JSP IN', 'JSTL IN', 'KMB IN', 'LT IN', 'LTFH IN', 'LTIM IN', 'LTTS IN', 'MM IN', 'MMFS IN', 'MPHL IN', 'MRCO IN', 'MSIL IN', 'MTCL IN', 'MUTH IN', 'NEST IN', 'NTPC IN', 'OBER IN', 'ONGC IN', 'PI IN', 'PIDI IN', 'POLYCAB IN', 'PSYS IN', 'RIL IN', 'SBICARD IN', 'SBILIFE IN', 'SBIN IN', 'SHTF IN', 'SHFL IN', 'SIEM IN', 'SRCM IN', 'SRF IN', 'TATA IN', 'TATACONS IN', 'TCOM IN', 'TCS IN', 'TECHM IN', 'TPWR IN', 'TRENT IN', 'TTAN IN', 'TTMT IN', 'UPLL IN', 'VEDL IN', 'WPRO IN', 'ZYDUSLIF IN']
#liqList = ['HDFCB IN', 'RIL IN', 'ADE IN', 'AXSB IN', 'INFO IN', 'SBIN IN', 'ACEM IN', 'TCS IN', 'KMB IN', 'BAF IN', 'HDFC IN', 'ADSEZ IN', 'ICICIBC IN', 'BOB IN', 'TTMT IN', 'MSIL IN', 'TATA IN', 'CBK IN', 'WPRO IN', 'IIB IN', 'BHARTI IN', 'HNDL IN', 'PNB IN', 'LT IN', 'ITC IN', 'APNT IN', 'HUVR IN', 'MM IN', 'JSTL IN', 'TTAN IN', 'HCLT IN', 'JSP IN', 'UTCEM IN', 'TPWR IN', 'SUNP IN', 'TECHM IN', 'BJFIN IN', 'IDFCFB IN', 'FB IN', 'DLFU IN', 'UPLL IN', 'BANDHAN IN', 'UNSP IN', 'COAL IN', 'EIM IN', 'SRF IN', 'BHEL IN', 'TVSL IN', 'ACC IN', 'HNAL IN', 'BHFC IN', 'SAIL IN', 'DIVI IN', 'HMCL IN', 'AL IN', 'IRCTC IN', 'NTPC IN', 'MMFS IN', 'MUTH IN', 'GRASIM IN', 'INDIGO IN', 'APHS IN', 'CIPLA IN', 'HDFCLIFE IN', 'LICHF IN', 'BJAUT IN', 'BRIT IN', 'BHE IN', 'IH IN', 'LPC IN', 'JUBI IN', 'PWGR IN', 'INFOE IN', 'CCRI IN', 'AUBANK IN', 'ONGC IN', 'GPL IN', 'PIDI IN', 'CIFC IN', 'POWF IN', 'LTTS IN', 'PSYS IN', 'DABUR IN', 'POLYCAB IN', 'RECL IN', 'LTFH IN', 'BPCL IN', 'SRCM IN', 'MRF IN', 'SBILIFE IN', 'HAVL IN', 'GAIL IN', 'KKC IN', 'ABB IN', 'ARBP IN', 'ABCAP IN', 'SIEM IN', 'COFORGE IN', 'BIL IN', 'NEST IN', 'BIOS IN', 'GCPL IN', 'PI IN', 'IOCL IN', 'HPCL IN', 'DN IN', 'SBICARD IN', 'TCOM IN', 'IPRU IN', 'GUJGA IN', 'ABFRL IN', 'ZYDUSLIF IN', 'HDFCAMC IN', 'DALBHARA IN', 'SUNTV IN', 'BRGR IN', 'MAXF IN', 'OBER IN', 'ASTRA IN', 'PAG IN', 'LAURUS IN', 'BOS IN', 'ICICIGI IN', 'BATA IN']
#excluded = ['ADE IN', 'ADSEZ IN']#ACC & Ambuja Stocks are added back, 28 april 2023#['ACC IN', 'ADE IN', 'ADSEZ IN', 'ACEM IN'] # Adani holding Stocsk are Removed //1 Feb 2023
#for tckr in excluded:
#    if tckr in liqList:
#        liqList.remove(tckr)
LiqAllstocks = []#['HDFCB IN', 'RIL IN', 'ICICIBC IN', 'SBIN IN', 'AXSB IN', 'KMB IN', 'INFO IN', 'BAF IN', 'TTMT IN', 'IIB IN', 'TCS IN', 'ITC IN', 'BOB IN', 'LT IN', 'MSIL IN', 'ACEM IN', 'BHARTI IN', 'TATA IN', 'CBK IN', 'DLFU IN', 'HNAL IN', 'MM IN', 'APNT IN', 'HUVR IN', 'IDFCFB IN', 'TECHM IN', 'VEDL IN', 'HCLT IN', 'TTAN IN', 'POWF IN', 'BJAUT IN', 'BJFIN IN', 'CIFC IN', 'UTCEM IN', 'HNDL IN', 'TPWR IN', 'AUBANK IN', 'JSTL IN', 'SUNP IN', 'BANDHAN IN', 'EIM IN', 'NTPC IN', 'PNB IN', 'FB IN', 'JSP IN', 'Z IN', 'UPLL IN', 'DIVI IN', 'HMCL IN', 'CIPLA IN', 'TVSL IN', 'DRRD IN', 'WPRO IN', 'MMFS IN', 'COAL IN', 'BHEL IN', 'PSYS IN', 'SIEM IN', 'GRASIM IN', 'HDFCLIFE IN', 'APTY IN', 'RECL IN', 'POLYCAB IN', 'IDFC IN', 'LTIM IN', 'IH IN', 'JUBI IN', 'KKC IN', 'ACC IN', 'RBK IN', 'ABB IN', 'ARBP IN', 'INDIGO IN', 'BHFC IN', 'AL IN', 'GCPL IN', 'GAIL IN', 'ONGC IN', 'COFORGE IN', 'SRF IN', 'APHS IN', 'SBILIFE IN', 'BHE IN', 'PWGR IN', 'IGL IN', 'BRIT IN', 'SAIL IN', 'NEST IN', 'BIOS IN', 'UNSP IN', 'GPL IN', 'VOLT IN', 'MUTH IN', 'SBICARD IN', 'ABCAP IN', 'LICHF IN', 'BPCL IN', 'LTTS IN', 'LPC IN', 'HAVL IN', 'SRCM IN', 'DABUR IN', 'IEX IN', 'PIEL IN', 'TATACONS IN', 'LTFH IN', 'TTCH IN', 'PI IN', 'SHFL IN', 'MPHL IN', 'PIDI IN', 'IPRU IN', 'MAHGL IN', 'DIXON IN', 'GNP IN', 'LAURUS IN', 'INFOE IN', 'IOCL IN', 'GMRI IN', 'MGFL IN', 'IRCTC IN', 'HPCL IN', 'MRF IN', 'ESCORTS IN', 'ASTRA IN', 'HDFCAMC IN', 'ABFRL IN', 'INDUSTOW IN', 'PAG IN', 'NMDC IN', 'NACL IN', 'NFIL IN', 'BRGR IN', 'DALBHARA IN', 'DN IN', 'CCRI IN', 'OBER IN', 'TRENT IN', 'BRCM IN', 'MOTHERSO IN', 'EXID IN', 'TRCL IN', 'BSOFT IN', 'ZYDUSLIF IN', 'GNFC IN', 'MRCO IN', 'BATA IN', 'GUJGA IN', 'DELTA IN', 'TCOM IN', 'BIL IN', 'BOS IN', 'ICICIGI IN', 'MAXF IN']
        
class DailyTradingSignals(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self,data)
        
    def basicdatainitialize(self):
        SpecialExpiryDates = {'2023-06-29': '2023-06-28'}#
        self.TempExpiryDates = copy.deepcopy(self.BackTestData.ExpiryDates)
        for key in SpecialExpiryDates.keys():
            if key in self.TempExpiryDates:
                self.TempExpiryDates[self.TempExpiryDates.index(key)] = SpecialExpiryDates[key]
                
        self.CurrentTime = pandas.to_datetime('2011-12-29')#('2023-03-29')#pandas.to_datetime('2022-12-29')#('2022-08-31')# '2013-01-07'
        self.UpdateDates = list(self.Close.index)#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.0010#2# Total 2 bps Transaction Charges        
        self.order = Order()
        self.StopLossLimit = -0.10# Stop loss Limit at Individual level
        self.MODELS = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 3, '20WMA_MACD' : 4, 'BodyOutSideBand' : 5, 'ROCMA' : 6, 'RegressionCrossOver' : 7, 'Vortex' : 8, 'Oscillator' : 9, 'RSI50': 10, 'AssymetricWeekly' : 11, 'Seasoning' : 12}#, 'AssymetricDaily' : 13}
        self.ModelLookBack = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 2, '20WMA_MACD' : 2, 'BodyOutSideBand' : 2, 'ROCMA' : 2, 'RegressionCrossOver' : 1, 'Vortex' : 3, 'Oscillator' : 4, 'RSI50': 2, 'AssymetricWeekly' : 5, 'Seasoning' : 2, 'AssymetricDaily' : 2}# lookback period in Number of Quarters
        self.MODELSELECTOR = {}        
        try:
            self.TRADE_REGISTER = pandas.read_excel(TRADE_REGISTER, sheet_name = 'Trades', header = 0, index_col =0)
            self.TRADE_REGISTER.index = self.TRADE_REGISTER['EXIT_DATE']
            self.TRADE_REGISTER = self.TRADE_REGISTER.sort_index()
        except:
            pass
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
        
        self.CloseWeekly = self.Close.resample('w-FRI', convention = 'end').last()
        self.SMA10Weekly = MyTechnicalLib.MovingAverage(self.CloseWeekly, 10)
        self.SMA20Weekly = MyTechnicalLib.MovingAverage(self.CloseWeekly, 20)
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
        indexNames = set(self.BackTestData.IndexInclusionFactorBSE200.loc[:self.CurrentTime].iloc[-1].dropna().index)
        #indexNames = set(LiqAllstocks)
        indexNames = set.intersection(indexNames, set(self.Close.loc[self.CurrentTime].dropna().index))
        if hasattr(self, 'TRADE_REGISTER') and str(self.CurrentTime.date()) in self.TempExpiryDates and self.CurrentTime.month in [3, 6, 9, 12]:#self.BackTestData.ExpiryDates
            PastTrades = self.TRADE_REGISTER.loc[self.CurrentTime-datetime.timedelta(2*365):self.CurrentTime - datetime.timedelta(1), :]
            if 'AssymetricDaily' in self.MODELS.keys():
                self.MODELS.pop('AssymetricDaily')
            PastTrades = PastTrades[[i in self.MODELS.keys() for i in PastTrades['STRATEGY_ID']]]
            gg = PastTrades.groupby('STRATEGY_ID')
            self.HistProfitFactor = pandas.DataFrame()
            for grp in gg.groups.keys():
                tempHistTrades = gg.get_group(grp)
                tempHistTrades = tempHistTrades.loc[self.CurrentTime - datetime.timedelta(90*self.ModelLookBack[grp]):, :]#datetime.timedelta(90*self.ModelLookBack[grp]*3)
                stats_obj = Stats(tempHistTrades)
                statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL).transpose()
                statsSymbolDF = statsSymbolDF[statsSymbolDF['Total Trades']>= 5]#to capture significant counts, we take those models where atleast there are 3 trades per quarter
                statsSymbolDF['Stratregy'] = grp
                self.HistProfitFactor = pandas.concat([self.HistProfitFactor, statsSymbolDF], axis = 0)
            self.HistProfitFactor = self.HistProfitFactor.sort_values('Profit Factor', ascending = False)
            tempProfitFactor = copy.deepcopy(self.HistProfitFactor)
            #tempProfitFactor['UID'] = range(len(tempProfitFactor))            
            tempProfitFactor = tempProfitFactor[~tempProfitFactor.index.duplicated(keep = 'first')]
            tempProfitFactor = tempProfitFactor[tempProfitFactor['Profit Factor']>=1.5]
            
            LiqAllstocks = indexNames#self.Close.loc[self.CurrentTime].dropna().index
            tempProfitFactor = tempProfitFactor.loc[set.intersection(set(LiqAllstocks), set(tempProfitFactor.index))]
            
            tempProfitFactor = tempProfitFactor[tempProfitFactor['Stratregy'] == 'Seasoning']
            tempProfitFactor = tempProfitFactor.sort_values('Profit Factor', ascending = False)
            tempProfitFactor = tempProfitFactor.iloc[:25]# Selecting top 25 for Seasonality, remaining will be selected from other models
            
            self.HistProfitFactor = self.HistProfitFactor[self.HistProfitFactor['Stratregy'] != 'Seasoning']
            self.HistProfitFactor = self.HistProfitFactor[~self.HistProfitFactor.index.duplicated(keep='first')]
            self.HistProfitFactor = self.HistProfitFactor[self.HistProfitFactor['Profit Factor']>= 1.7]
            self.HistProfitFactor = self.HistProfitFactor[~self.HistProfitFactor.index.isin(tempProfitFactor.index)]
            
            self.HistProfitFactor = pandas.concat([self.HistProfitFactor, tempProfitFactor], axis = 0)
            bb = self.HistProfitFactor.groupby('Stratregy')
            self.MODELSELECTOR = {}
            for gp in bb.groups.keys():
                self.MODELSELECTOR[self.MODELS[gp]] = list(bb.get_group(gp).index)
            print(self.CurrentTime.date(), '-Model Selector Updated')
        
        switcher = {1: self.MD1_RSIwith50SMATrend, 2: self.MD2_RSI50SMA, 3: self.MD3_Series3, 4: self.MD4_20WMA_MACD, 5: self.MD5_BodyOutSideBand, 6: self.MD6_ROCMA, 
                    7: self.MD7_RegressionCrossOver, 8: self.MD8_Vortex, 9: self.MD9_Oscillator, 10: self.MD10_RSI50, 11: self.MD11_AssymetricWeekly, 12: self.MD12_Seasoning, 13: self.MD11_AssymetricDaily}
        
        modelSelector = list(self.MODELSELECTOR.keys())
        modelSelector.sort()
        
        for funcNum in modelSelector:
            tickers = set.intersection(set(self.MODELSELECTOR[funcNum]), indexNames)#, set(LiqAllstocks))
            func = switcher.get(funcNum, lambda: 'Invalid Model')
            try:
                func(tickers)
            except:
                pdb.set_trace()
        self.indexNames = indexNames
    
    def updateCapAllocation(self):
        #self.populateOrderPosition()
        self.CapitalAllocation.loc[self.CurrentTime] = 0       
        positionWOSL = self.PositionWOSL.loc[self.CurrentTime]
        tempDF = positionWOSL[positionWOSL != 0]
        
        for ticker in tempDF.index:
            self.StopLoss(ticker, self.StopLossLimit)
            #self.StopLossTrail(ticker, self.StopLossLimit)        
        
        self.Position.loc[self.CurrentTime] = self.PositionWOSL.loc[self.CurrentTime]
        for ticker in tempDF.index:
            TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL, forStopLoss= True)
            checkExitPosition = self.PositionExitDF.loc[TradeTakenDate:self.CurrentTime, ticker].dropna()
            if len(checkExitPosition) >0:
                self.Position.loc[self.CurrentTime, ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, ticker] = self.PositionWOSL.loc[self.CurrentTime, ticker]

        MaxWT = 3.5/100
        indexName = 'NIFTY INDEX'#self.BackTestData.indexprice.columns[0]
        self.Strategy.loc[self.CurrentTime, indexName] = 'Hedge'
        CurrentPosition = self.Position.loc[self.CurrentTime]
        PositionWithSL = CurrentPosition[CurrentPosition != 0]
        netPosition = CurrentPosition.sum()
        
        GrossExposure = (PositionWithSL.abs().sum() + abs(netPosition))*MaxWT
        if GrossExposure > 2.0:#1.9
            MaxWT = 2.0/(PositionWithSL.abs().sum() + abs(netPosition))
        
        #PositionWithSL = PositionWithSL[PositionWithSL<0]# Long Positions, Comment it in normal Full Fund Case
        for iTicker in PositionWithSL.index:
            self.CapitalAllocation.loc[self.CurrentTime,iTicker]= PositionWithSL.loc[iTicker]*self.CurrentNAV*MaxWT
        #if netPosition !=0:# Hedge the Net Position with Index
        #     self.Position.loc[self.CurrentTime, indexName] = -numpy.sign(netPosition)
        #     self.CapitalAllocation.loc[self.CurrentTime, indexName]= -netPosition*self.CurrentNAV*MaxWT
        if 'Cash' in self.Close.columns:# Allocate Remain to Cash
            self.CapitalAllocation.loc[self.CurrentTime, 'Cash'] = self.CurrentNAV*(1 - (PositionWithSL.abs().sum() + abs(netPosition))*MaxWT)
            self.Position.loc[self.CurrentTime, 'Cash'] = numpy.sign(self.CapitalAllocation.loc[self.CurrentTime, 'Cash'])
        self.UpdateOrderBook()
        pdb.set_trace()
        ll = []
        kk = [[ll.append(j) for j in i] for i in self.MODELSELECTOR.values()]
        print(self.CurrentTime.date(), len(set.intersection(self.indexNames, set(ll))))#, set(LiqAllstocks))))
                
if __name__=='__main__':
    import pickle
    
    picklePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
    #f = open(basePath+'STFDMOM_'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
    #dirPath = 'Z:/'
    #dataFile = 'Z:/Pickles/Hist_FutsData18Apr-2023.pkl'
    #dataFile = picklePath+'STFDMOM_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl'
    #dataFile = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/STFDMOM_All20250902.pkl'
    dataFile = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/STFDMOM_20250902.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    
    basePath = 'G:/Shared drives/QuantFunds/Liquid1/LiveModels/ModelFiles/'
    model = DailyTradingSignals(mydata)#iDict
    model.run()
    #a.ResultFrameWithIndex()
    backtestName = 'STFDMOM_woHedge_'#
    filepath = basePath+backtestName+datetime.datetime.today().date().strftime('%Y%m%d')+'_BSE200_OriginalLive.xlsx'#_ShortOnly_WithOutHedge
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
        
    tradeDF = pandas.DataFrame()
    df = a.trade_reg.get_trade_register()
    tradeDF = pandas.concat([tradeDF, df], axis = 0)
        
    filepath = basePath+backtestName+'_TradesRegister.xlsx'#_TradesRegister
    stats_obj = Stats(tradeDF)
    statsSymbolDF = stats_obj.create_stats(filter_by = Filter.SYMBOL)
    statsPositionDF = stats_obj.create_stats(filter_by = Filter.POSITION)
    statsStrategyDF = stats_obj.create_stats(filter_by = Filter.STRATEGY_ID)
    
    writer = pandas.ExcelWriter(filepath)
    tradeDF.to_excel(writer,'Trades')
    statsSymbolDF.transpose().to_excel(writer,'Stats-Symbol')
    statsStrategyDF.transpose().to_excel(writer,'Stats-Strategy')
    statsPositionDF.to_excel(writer,'Stats-Position')    
    
    writer.save()
    writer.close()
    
    models = []
    ok = [[models.append((it, jt)) for jt in temp[it]] for it in list(temp.keys())] # to get Models Name for the Stocks
    '''