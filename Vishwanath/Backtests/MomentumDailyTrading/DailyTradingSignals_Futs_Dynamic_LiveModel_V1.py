# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30 12:10:24 2022

@author: Viren@InCred
Dynamic Selection-Live Model
Changes in This Version
1. We moved from BBG ticker to NSE Scrip based
2. For All signal Genration, we are taking Spots Data
3. Roll over Impact we have overcome, using same expiry tickers price
4. 
"""

from FactoryBackTester_V1 import FactoryBackTester
import MyTechnicalLib as mylib
import pandas
import numpy
import datetime
import pdb
#import matplotlib.pyplot as plt
#import copy
#from random import choice
import pdb
import warnings
warnings.filterwarnings("ignore")

from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter
TRADE_REGISTER = 'AllTrades_TradesRegister2023-04-18.xlsx'

# Liquidity List is defined in NSE Scrips
liqList = ['ABB', 'ABCAPITAL', 'ABFRL', 'ACC', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BALRAMCHIN', 'BANDHANBNK', 'BANKBARODA', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BHEL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSOFT', 'CANBK', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'CONCOR', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK', 'GAIL', 'GLENMARK', 'GMRINFRA', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRASIM', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDPETRO', 'HINDUNILVR', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDFC', 'IDFCFIRSTB', 'IEX', 'IGL', 'INDHOTEL', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'IOC', 'IRCTC', 'ITC', 'JINDALSTEL', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'L&TFH', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTIM', 'LTTS', 'LUPIN', 'M&M', 'M&MFIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MFSL', 'MGL', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NESTLEIND', 'NMDC', 'NTPC', 'OBEROIRLTY', 'ONGC', 'PAGEIND', 'PEL', 'PERSISTENT', 'PFC', 'PIDILITIND', 'PIIND', 'PNB', 'POLYCAB', 'POWERGRID', 'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SHRIRAMFIN', 'SIEMENS', 'SRF', 'SUNPHARMA', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TRENT', 'TVSMOTOR', 'ULTRACEMCO', 'UPL', 'VEDL', 'VOLTAS', 'WIPRO', 'ZEEL', 'ZYDUSLIFE']
excluded = ['ADANIENT', 'ADANIPORTS']#ACC & Ambuja Stocks are added back, 28 april 2023#['ACC IN', 'ADE IN', 'ADSEZ IN', 'ACEM IN'] # Adani holding Stocsk are Removed //1 Feb 2023
for tckr in excluded:
    if tckr in liqList:
        liqList.remove(tckr)
        
class DailyTradingSignals(FactoryBackTester):
    def __init__(self,data):
        FactoryBackTester.__init__(self,data)
        
    def basicdatainitialize(self):        
        self.CurrentTime = pandas.to_datetime('2011-12-29')#pandas.to_datetime('2022-12-29')#('2022-08-31')# '2013-01-07'
        self.UpdateDates = list(self.Close.index)#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.000#2# Total 2 bps Transaction Charges        
        self.order = Order()
        self.StopLossLimit = -0.10# Stop loss Limit at Individual level
        self.MODELS = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 3, '20WMA_MACD' : 4, 'BodyOutSideBand' : 5, 'ROCMA' : 6, 'RegressionCrossOver' : 7, 'Vortex' : 8, 'Oscillator' : 9, 'RSI50': 10, 'AssymetricWeekly' : 11, 'Seasoning' : 12}#, 'AssymetricDaily' : 13}
        self.ModelLookBack = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 2, '20WMA_MACD' : 2, 'BodyOutSideBand' : 2, 'ROCMA' : 2, 'RegressionCrossOver' : 1, 'Vortex' : 3, 'Oscillator' : 4, 'RSI50': 2, 'AssymetricWeekly' : 5, 'Seasoning' : 2, 'AssymetricDaily' : 2}# lookback period in Number of Quarters
        self.MODELSELECTOR = {}
        
        try:
            self.TRADE_REGISTER = pandas.read_excel(TRADE_REGISTER, sheet_name = 'Trades', header = 0, index_col = 0)
            self.TRADE_REGISTER.index = self.TRADE_REGISTER['EXIT_DATE']
            self.TRADE_REGISTER = self.TRADE_REGISTER.sort_index()
        except:
            pass
        
        self.PositionExitDF = pandas.DataFrame(numpy.nan,index=self.Close.index,columns=self.Close.columns)# it tracks for the exit records, StopLoss, Target or anything which might be added 
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)# It tracks before applying anytype of StopLoss or target
        self.trade_reg = TradeRegister()
        self.Strategy = pandas.DataFrame(numpy.nan, index=self.Close.index,columns=self.Close.columns)
        
        self.RSI2Position = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.SMA50Position = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.SMA100Position = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.DirectionPosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.TradePosition = pandas.DataFrame(numpy.zeros_like(self.BackTestData.Index.Close),index=self.BackTestData.Index.Close.index,columns=self.BackTestData.Index.Close.columns)
        self.RetsHigh = self.BackTestData.Index.High.pct_change(fill_method = None)
        self.RetsLow = self.BackTestData.Index.Low.pct_change(fill_method = None)
        self.RetsClose = self.BackTestData.Index.Close.pct_change(fill_method = None)
        
        self.MACDDiff = numpy.subtract(self.BackTestData.Index.MACD, self.BackTestData.MACDSignal)#self.BackTestData.Index.MACDSignal)
        self.PriceAboveWMA = numpy.subtract(self.BackTestData.Index.Close, self.BackTestData.Index.WMA20)
        self.ATRRatio = numpy.subtract(self.BackTestData.Index.Close, self.BackTestData.Index.SMA24)/self.BackTestData.Index.ATR24
        self.CloseDiffMinSMA = numpy.subtract(self.BackTestData.Index.Close, numpy.minimum(self.BackTestData.Index.SMA10, self.BackTestData.Index.SMA20))
        
        self.CloseWeekly = self.BackTestData.Index.Close.resample('w-FRI', convention = 'end').last()
        self.SMA10Weekly = mylib.MovingAverage(self.CloseWeekly, 10)
        self.SMA20Weekly = mylib.MovingAverage(self.CloseWeekly, 20)
        self.CloseDiffMinSMAWeekly = numpy.subtract(self.CloseWeekly, numpy.minimum(self.SMA10Weekly, self.SMA20Weekly))
        
        self.PviNviRatio = numpy.subtract(self.BackTestData.Index.PVI, self.BackTestData.Index.NVI)
        
        
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
            
    def UpdateSpecificStats(self):        
        if hasattr(self, 'TRADE_REGISTER') and str(self.CurrentTime.date()) in self.BackTestData.ExpiryDates and self.CurrentTime.month in [3, 6, 9, 12]:
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
            self.HistProfitFactor = self.HistProfitFactor[~self.HistProfitFactor.index.duplicated(keep='first')]
            self.HistProfitFactor = self.HistProfitFactor[self.HistProfitFactor['Profit Factor']>= 1.7]
            bb = self.HistProfitFactor.groupby('Stratregy')
            self.MODELSELECTOR = {}
            for gp in bb.groups.keys():
                self.MODELSELECTOR[self.MODELS[gp]] = list(bb.get_group(gp).index)
            print(self.CurrentTime.date(), '-Model Selector Updated')
        
        indexNames = self.GetLatestIndexComponents()#set(self.BackTestData.IndexInclusionFactorBSE200.loc[:self.CurrentTime].iloc[-1].dropna().index)
        indexNames = [it.replace(' IN', '') for it in indexNames]
        indexNames = [self.BackTestData.NSEDict[it] for it in indexNames if it in self.BackTestData.NSEDict.keys()]
        indexNames = set.intersection(set(indexNames), set(self.BackTestData.Index.Close.loc[self.CurrentTime].dropna().index))
        
        switcher = {1: self.MD1_RSIwith50SMATrend, 2: self.MD2_RSI50SMA, 3: self.MD3_Series3, 4: self.MD4_20WMA_MACD, 5: self.MD5_BodyOutSideBand, 6: self.MD6_ROCMA, 
                    7: self.MD7_RegressionCrossOver, 8: self.MD8_Vortex, 9: self.MD9_Oscillator, 10: self.MD10_RSI50, 11: self.MD11_AssymetricWeekly, 12: self.MD12_Seasoning, 13: self.MD11_AssymetricDaily}
        
        ExpiryDates = [it for it in self.BackTestData.ExpiryDates if datetime.datetime.strptime(it, '%Y-%m-%d').date() >= self.CurrentTime.date()]# NearExpiry is the date where we have to take the position any day
        if self.CurrentTime in self.ExpiryDates:
            self.NearExpiry = ExpiryDates[1]
        else:
            self.NearExpiry = ExpiryDates[0]
        self.NearExpiry = datetime.datetime.strftime(datetime.datetime.strptime(self.NearExpiry, '%Y-%m-%d'), '%d%b%y').upper()
            
        modelSelector = list(self.MODELSELECTOR.keys())
        modelSelector.sort()
        for funcNum in modelSelector:
            pdb.set_trace()
            tickers = set.intersection(set([self.BackTestData.NSEDict[it.replace(' IN', '')] for it in self.MODELSELECTOR[funcNum] if it not in ['NIFTY INDEX', 'NSEBANK INDEX']]), indexNames, set(liqList))
            tickers = indexNames
            tickers = [it for it in tickers if it+self.NearExpiry+'XX0' in self.BackTestData.Close.columns]
            
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
            
        for iTicker in PositionWithSL.index:
            self.CapitalAllocation.loc[self.CurrentTime,iTicker]= PositionWithSL.loc[iTicker]*self.CurrentNAV*MaxWT
        if netPosition !=0:# Hedge the Net Position with Index
             self.Position.loc[self.CurrentTime, indexName] = -numpy.sign(netPosition)
             self.CapitalAllocation.loc[self.CurrentTime, indexName]= -netPosition*self.CurrentNAV*MaxWT
        if 'Cash' in self.Close.columns:# Allocate Remain to Cash
            self.CapitalAllocation.loc[self.CurrentTime, 'Cash'] = self.CurrentNAV*(1 - (PositionWithSL.abs().sum() + abs(netPosition))*MaxWT)
            self.Position.loc[self.CurrentTime, 'Cash'] = numpy.sign(self.CapitalAllocation.loc[self.CurrentTime, 'Cash'])
        self.UpdateOrderBook()
        ll = []
        kk = [[ll.append(j) for j in i] for i in self.MODELSELECTOR.values()]
        print(self.CurrentTime.date(), len(set.intersection(self.indexNames, set(ll), set(liqList))))
                
if __name__=='__main__':
    import pickle
    
    picklePath = 'G:/Shared drives/QuantFunds/Liquid1/DataPickles/'
    #f = open(basePath+'STFDMOM_'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl', 'wb')
    #dirPath = 'Z:/'
    dataFile = picklePath+'STFDMOM_V2_20230621.pkl'
    #dataFile = picklePath+'STFDMOM_'+ datetime.datetime.today().date().strftime('%Y%m%d') +'.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    
    #basePath = 'G:/Shared drives/QuantFunds/Liquid1/LiveModels/ModelFiles/'
    basePath = 'G:/Shared drives/BackTests/BackTestsResults/DailyMomentum_RollOverVersion/'
    model = DailyTradingSignals(mydata)#iDict
    model.run()
    #a.ResultFrameWithIndex()
    backtestName = 'STFDMOM_V2'#
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