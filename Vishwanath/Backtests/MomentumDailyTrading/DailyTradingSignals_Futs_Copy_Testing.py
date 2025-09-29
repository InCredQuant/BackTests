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
TRADE_REGISTER = 'Z:/BacktestsResults/DailyTradingSignals_V3_Dec2022_AfterRecheck/AllTrades_TradesRegister.xlsx'

#MODELSELECTOR = {1: 'RSIwith50SMATrend', 2: 'RSI50SMA', 3: 'Series3', 4: '20WMA_MACD', 5: 'BodyOutSideBand', 6: 'ROCMA', 7: 'RegressionCrossOver', 8: 'Vortex', 9: 'Oscillator', 10: 'RSI50', 111: 'AssymetricWeekly', 112: 'AssymetricDaily', 12: 'Seasoning'}
liqList = ['ABB IN', 'ABFRL IN', 'ADE IN', 'ADSEZ IN', 'AL IN', 'APHS IN', 'ASTRA IN', 'AUBANK IN', 'AXSB IN', 'BAF IN', 'BHARTI IN', 'BHE IN', 'BHFC IN', 'BIL IN', 'BJFIN IN', 'BOB IN', 'BRIT IN', 'CIFC IN', 'COAL IN', 'COFORGE IN', 'DIVI IN', 'DIXON IN', 'DLFU IN', 'DN IN', 'DRRD IN', 'GCPL IN', 'GPL IN', 'GRASIM IN', 'GUJGA IN', 'HAVL IN', 'HCLT IN', 'HDFCAMC IN', 'HDFCB IN', 'HNAL IN', 'HNDL IN', 'HUVR IN', 'ICICIBC IN', 'ICICIGI IN', 'IDFCFB IN', 'IGL IN', 'IH IN', 'IIB IN', 'INDIGO IN', 'INFO IN', 'INFOE IN', 'IOCL IN', 'IRCTC IN', 'JSP IN', 'JSTL IN', 'KMB IN', 'LT IN', 'LTFH IN', 'LTIM IN', 'LTTS IN', 'MM IN', 'MMFS IN', 'MPHL IN', 'MRCO IN', 'MSIL IN', 'MTCL IN', 'MUTH IN', 'NEST IN', 'NTPC IN', 'OBER IN', 'ONGC IN', 'PI IN', 'PIDI IN', 'POLYCAB IN', 'PSYS IN', 'RIL IN', 'SBICARD IN', 'SBILIFE IN', 'SBIN IN', 'SHTF IN', 'SHFL IN', 'SIEM IN', 'SRCM IN', 'SRF IN', 'TATA IN', 'TATACONS IN', 'TCOM IN', 'TCS IN', 'TECHM IN', 'TPWR IN', 'TRENT IN', 'TTAN IN', 'TTMT IN', 'UPLL IN', 'VEDL IN', 'WPRO IN', 'ZYDUSLIF IN']

class DailyTradingSignals(FactoryBackTester):
    def __init__(self,data, modelNum = 0):
        FactoryBackTester.__init__(self,data)
        self.ModelNum = modelNum
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2021-12-27')#('2022-08-31')# '2013-01-07'
        self.UpdateDates = list(self.Close.index)#list(self.BackTestData.Close.index)#.values()
        self.TransactionCostRate = 0.000#2# Total 2 bps Transaction Charges        
        self.order = Order()
        self.StopLossLimit = -0.10# Stop loss Limit at Individual level
        #self.TargetLimit = 0.15
        self.MODELS = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 3, '20WMA_MACD' : 4, 'BodyOutSideBand' : 5, 'ROCMA' : 6, 'RegressionCrossOver' : 7, 'Vortex' : 8, 'Oscillator' : 9, 'RSI50': 10, 'AssymetricWeekly' : 11, 'Seasoning' : 12, 'AssymetricDaily' : 13}
        #self.ModelLookBack = {'RSIwith50SMATrend' : 1, 'RSI50SMA' :2, 'Series3' : 2, '20WMA_MACD' : 2, 'BodyOutSideBand' : 3, 'ROCMA' : 2, 'RegressionCrossOver' : 1, 'Vortex' : 3, 'Oscillator' : 4, 'RSI50': 3, 'AssymetricWeekly' : 6, 'Seasoning' : 2, 'AssymetricDaily' : 2}# lookback period in Number of Quarters
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
        if self.ModelNum == 12:
            self.Close = self.Close.div(self.BackTestData.indexprice.loc[:, 'NZ1 INDEX'], axis = 0)
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True
            
    def UpdateSpecificStats(self):        
        if hasattr(self, 'TRADE_REGISTER') and str(self.CurrentTime.date()) in self.BackTestData.ExpiryDates and self.CurrentTime.month in [3, 6, 9, 12]:#[3, 6, 9, 12]
            PastTrades = self.TRADE_REGISTER.loc[self.CurrentTime-datetime.timedelta(3*365):self.CurrentTime - datetime.timedelta(1), :]
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
        
        indexNames = set(self.BackTestData.IndexInclusionFactorBSE200.loc[:self.CurrentTime].iloc[-1].dropna().index)
        indexNames = set.intersection(indexNames, set(self.Close.loc[self.CurrentTime].dropna().index))
        
        switcher = {1: self.MD1_RSIwith50SMATrend, 2: self.MD2_RSI50SMA, 3: self.MD3_Series3, 4: self.MD4_20WMA_MACD, 5: self.MD5_BodyOutSideBand, 6: self.MD6_ROCMA, 
                    7: self.MD7_RegressionCrossOver, 8: self.MD8_Vortex, 9: self.MD9_Oscillator, 10: self.MD10_RSI50, 11: self.MD11_AssymetricWeekly, 12: self.MD12_Seasoning, 13: self.MD11_AssymetricDaily}
        
        '''
        indexNames = set(self.Close.loc[self.CurrentTime].dropna().index)
        self.indexNames = indexNames
        tickers = indexNames
        func = switcher.get(self.ModelNum, lambda: 'Invalid Model')
        try:
            func(tickers)
        except:
            pdb.set_trace()
        '''
        modelSelector = list(self.MODELSELECTOR.keys())
        modelSelector.sort()
        for funcNum in modelSelector:
            tickers = set.intersection(set(self.MODELSELECTOR[funcNum]), indexNames)
            func = switcher.get(funcNum, lambda: 'Invalid Model')
            try:
                func(tickers)
            except:
                pdb.set_trace()        
        self.indexNames = indexNames
        
    def UpdateOrderBook(self, strategyID = ''):
        '''
        Parameters
        ----------
        Position Matrix: required.
        
        strategyID : TYPE, optional, Tells the Strategy Description
            DESCRIPTION. The default is ''.
        
        Description
        -----------
        The method takes an optional argument strategyID, which is not used if it is not provided.
        The method begins by extracting the current and previous positions from a Position dataframe. Then, it uses set operations to identify cases where the current position has changed compared to the previous position:
        
        position_to_Neutral: positions that were either long or short in the previous time step, but are now neutral (i.e., 0) in the current time step
        short_to_Long: positions that were short or neutral in the previous time step, but are now long in the current time step
        long_to_Short: positions that were long or neutral in the previous time step, but are now short in the current time step
        The method then combines these sets of positions into a single set called Changes. For each ticker in Changes, the method looks up the position and entry date for the ticker in the Position dataframe, and then calls the OrderPosition method with these values, along with the current time step and the strategyID.
        
        Finally, if the current time step is the last time step in the UpdateDates sequence, the method looks up the non-zero positions in the Position dataframe and calls the OrderPosition method for each of these positions, again passing in the position, entry date, current time step, and strategyID.
        '''
        CurrentPosition = self.Position.loc[self.CurrentTime]
        lastPosition = self.Position.loc[self.LastTime]
        position_to_Neutral = set.intersection(set(lastPosition[lastPosition !=0].dropna().index), set(CurrentPosition[CurrentPosition == 0].dropna().index))#1. Current Becomes Neutral, previous was Long or Short
        short_to_Long = set.intersection(set(lastPosition[lastPosition == -1].dropna().index), set(CurrentPosition[CurrentPosition == 1].dropna().index))#2. Current becomes Long, Previous was Short or neutral
        long_to_Short = set.intersection(set(lastPosition[lastPosition == 1].dropna().index), set(CurrentPosition[CurrentPosition == -1].dropna().index))#3. Current Becomes Short, Previous was Long or Neutral
        Changes = position_to_Neutral.union(short_to_Long, long_to_Short)
        for iTicker in Changes:
            position = self.Position.loc[self.LastTime, iTicker]
            entry_date = self.DetectPostionStartDate(iTicker, self.Position)
            try:
                strategyID = str(self.Strategy.loc[entry_date, iTicker])
            except:
                strategyID = str(iTicker)
            self.OrderPosition(iTicker, entry_date, position, strategyID)
            
        if self.CurrentTime == max(self.UpdateDates):
            CurrentPosition = CurrentPosition[CurrentPosition != 0].dropna()
            for iTicker in CurrentPosition.index:
                position = self.Position.loc[self.CurrentTime, iTicker]
                entry_date = self.DetectPostionStartDate(iTicker, self.Position)
                try:
                    strategyID = str(self.Strategy.loc[entry_date, iTicker])
                except:
                    strategyID = str(iTicker)
                self.OrderPosition(iTicker, entry_date, position, strategyID, self.CurrentTime)
    
    
    def updateCapAllocation(self):
        #self.populateOrderPosition()
        self.CapitalAllocation.loc[self.CurrentTime] = 0       
        positionWOSL = self.PositionWOSL.loc[self.CurrentTime]
        tempDF = positionWOSL[positionWOSL != 0]
        
        for ticker in tempDF.index:
               self.StopLossTrail(ticker, self.StopLossLimit)
        
        self.Position.loc[self.CurrentTime] = self.PositionWOSL.loc[self.CurrentTime]
        for ticker in tempDF.index:
            TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL)
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
        if GrossExposure > 1.9:
            MaxWT = 1.9/(PositionWithSL.abs().sum() + abs(netPosition))
            
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
        print(self.CurrentTime.date(), len(set.intersection(self.indexNames, set(ll))))
                
if __name__=='__main__':
    import pickle
    dirPath = 'Z:/'#'G:/Shared drives/BackTests/'# 'Z:/'
    dataFile = 'Z:/Pickles/Hist_FutsDataDec-2022.pkl'#'Z:/Pickles/Hist_FutsData_DailyMomentum_12Dec-2022.pkl'
    f = open(dataFile, 'rb')
    mydata = pickle.load(f)
    f.close()
    basePath = dirPath+'BacktestsResults/DailyTradingSignals_V4/'
    
    a = DailyTradingSignals(mydata, modelNum = 0)#iDict
    a.run()
    a.ResultFrameWithIndex()
    backtestName = 'DynamicModel_BSE200_Dec22_'#
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
    
    tradeDF = a.trade_reg.get_trade_register()
        
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