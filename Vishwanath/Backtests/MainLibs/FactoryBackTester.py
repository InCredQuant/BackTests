

import numpy
import datetime
import time
import pandas
import math
import copy
import pdb
import re
from GetData import getdatelist
from order_base import Order, Position, Segment, OptionType

### parent class for backtesting
#class orders():
#    def __init__(self,Symbol=[],ordertype=[],ordervalue=[],priceparameter=[],validfrom=[],goodtill=[]):
#        self.Symbol = Symbol
#        self.ordertype = ordertype
#        self.ordervalue = ordervalue
#        self.priceparameter = priceparameter
#        self.triggerprice = []
#        self.validfrom = validfrom
#        self.goodtill = goodtill

def NameDecorator(Class):
    oldInit=Class.__init__
    def newinit(self,*arg,**kwarg):
       #self.Name=str(Class)[9:]
       self.Parameters=[]
       self.ParameterValues=[]
       #s=str(Class)[9:]
       s=str(Class)
       s=s[s.find('.')+1:]
       for name,value in kwarg.items():
           self.Parameters.append(name)
           self.ParameterValues.append(value)
           s=s+'_'+name+'_'+str(value)
       self.Name=s
       oldInit(self,*arg,**kwarg)
    Class.__init__=newinit
    return Class


class FactoryBackTester():
    ### initialise data and empty matrices to track statistics
    def __init__(self, data):
        self.BackTestData = data
        #expects Close attribute in data.BackTestData
        self.Close = copy.deepcopy(data.Close)
        self.symbols=self.Close.columns#BackTestData
        self.FromTime= self.Close.index[0]#BackTestData
        if hasattr(self.BackTestData, 'ExpiryDates'):
            self.ExpiryDates = pandas.DatetimeIndex(self.BackTestData.ExpiryDates)
        self.CurrentTime = self.FromTime
        self.EndTime = self.Close.index[-1]#BackTestData
        self.LastTime=self.CurrentTime
        self.TransactionCostRate=0.00#25#2.5 bps
        self.CapitalAllocation=pandas.DataFrame(numpy.zeros_like(self.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.MTM=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.TradeLog=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.TradeLogQty=pandas.DataFrame(numpy.zeros_like(self.BackTestData.Close),index=self.BackTestData.Close.index,columns=self.BackTestData.Close.columns)
        self.Position=pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.TempCapitalAllocation=[]
        self.TempQuantityAllocation=[]
        self.Dates=self.BackTestData.Close.index
        #self.Iterator=self.BackTestData.Close.iterrows()
        self.Counter=-1
        self.NAV=pandas.DataFrame(columns=['NAV'],index=self.BackTestData.Close.index).fillna(100)
        self.TransactionCost=pandas.DataFrame(columns=['TransactionCost'],index=self.BackTestData.Close.index).fillna(0)
        if hasattr(self, 'TradedValue'):
            self.StartingNAV = self.TradedValue
        else:
            self.StartingNAV = 100
        self.NAV['NAV'][self.CurrentTime] = self.StartingNAV        
        self.AllOrders = []
        #self.OptionTickerRegEx = r'(?P<symbol>[A-Z]+)(?P<expiry_date>\d+[A-Z]+\d+)(?P<option_type>[A-Z]+)(?P<strike>\d+(\.\d+)?)'
        self.OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d+[A-Z]+\d+)(?P<option_type>[A-Z]+)(?P<strike>\d+(\.\d+)?)'
        self.PairTrades = False

    ## main loop
    def run(self):
        #Called once only in beginning
        self.basicdatainitialize()
        if self.CurrentTime > self.FromTime:
            self.CurrentTime = max(self.Dates[self.Dates < self.CurrentTime])
            self.Counter = list(self.Dates).index(self.CurrentTime)
            self.StartCounter = self.Counter
            self.FromTime = self.CurrentTime
            
        while self.CurrentTime < self.EndTime :
            self.moveonesteptime()
        
        self.ResultFrameWithIndex()
        return self

    ## increment of loop(time ++)

    def GetAllUpdateDates(self, days):
        self.UpdateDates = []
        # if hasattr(self, 'ExpiryDates'):
        #     self.UpdateDates.append(min(self.ExpiryDates[self.ExpiryDates >= self.CurrentTime]))
        #     tempList = self.ExpiryDates[self.ExpiryDates >= self.CurrentTime]
        #     for iDate in tempList:
        #         if iDate in self.Dates:
        #             self.UpdateDates.append(iDate)
                
        # else:
        self.UpdateDates.append(min(self.Dates[self.Dates >= self.CurrentTime]))
        temptime = self.CurrentTime + datetime.timedelta(days)            
        for i in range(len(self.Dates)):
            if self.Dates[i] >= temptime and self.Dates[i-1] <= temptime:# and temptime <=  datetime.datetime.today().date():
                self.UpdateDates.append(self.Dates[i])
                temptime = self.Dates[i] + datetime.timedelta(days)
                #temptime = temptime + datetime.timedelta(days)
            
    def GetAllRebalanceTimes(self, timeDiff): # This required for all the stats calculations
        self.RebalanceTimes = []
        self.IndexTimes = self.BackTestData.indexprice.loc[self.CurrentTime - datetime.timedelta(minutes = 1000*timeDiff):, :].index       
        self.RebalanceTimes.append(min(self.IndexTimes[self.IndexTimes >= self.CurrentTime]))
        
        temptime = self.CurrentTime + datetime.timedelta(minutes = timeDiff)            
        for i in range(len(self.IndexTimes)):
            if self.IndexTimes[i] >= temptime and self.IndexTimes[i-1] <= temptime:# and temptime <=  datetime.datetime.today().date():
                self.RebalanceTimes.append(self.IndexTimes[i])
                temptime = self.IndexTimes[i] + datetime.timedelta(minutes = timeDiff)
                #temptime = temptime + datetime.timedelta(days)
        
        temptime = self.RebalanceTimes[0] - datetime.timedelta(minutes = timeDiff)
        N = len(self.IndexTimes)
        for i in range(N-1, 0, -1):            
            if self.IndexTimes[i-1] <= temptime and self.IndexTimes[i] >= temptime:# and temptime <=  datetime.datetime.today().date():
                self.RebalanceTimes.insert(0, self.IndexTimes[i-1])
                temptime = self.IndexTimes[i-1] - datetime.timedelta(minutes = timeDiff)
            
    def GetCurrentCapitalAllocation(self, myarg):
        if type(myarg) is str:
            myarg = pandas.to_datetime(myarg)
        
        usedate = max(self.Dates[self.Dates <= self.CurrentTime])
        currentallocation = self.CapitalAllocation.ix[usedate][self.CapitalAllocation.ix[myarg] != 0]
        return currentallocation
    
    def GetLatestIndexComponents(self):
        if hasattr(self.BackTestData, 'indexcomponents'):
            return self.BackTestData.indexcomponents[max([i for i in self.BackTestData.indexcomponents.keys() if i < self.CurrentTime])]
        elif hasattr(self, 'indexcomponents'):
            return self.indexcomponents[max([i for i in self.indexcomponents.keys() if i < self.CurrentTime])]
        

    def moveonesteptime(self):        
        self.Counter +=1
        self.LastTime=self.CurrentTime
        self.CurrentTime=self.Dates[self.Counter]
        #self.CurrentTime=self.Iterator.next()[0]
        if hasattr(self, 'StrategyType') and self.StrategyType == 'OP':
            self.updatePnLandNAVhandlerOptions()
        else:
            self.updatePnLandNAVhandler()
        self.declarecurrentvariables()
        self.StopLossHandler()
        #self.orderhandler()
        #pdb.set_trace()
        if self.detectupdatedate():
            self.UpdateSpecificStats()
            self.updateCapAllocation()
        #if hasattr(self, 'StrategyType') and self.StrategyType == 'OP':
        #    self.HedgePositions()
    
    
        
        
    def ResultFrameWithIndex(self):
        if hasattr(self.BackTestData,'indexprice'):
            tempframe = pandas.concat([self.NAV, self.BackTestData.indexprice.fillna(method = 'pad')], axis = 1)
            for i in range(len(self.NAV)-1):
                if self.NAV.iloc[i+1].values != self.StartingNAV:
                    self.PlotResult = tempframe.loc[self.NAV[i:].index[0]:]
                    break
            self.PlotResult = self.PlotResult.ffill()
#            tempframe = tempframe.resample('A', how = 'last')
#            for i in range(len(tempframe)-1):
#                if tempframe.ix[i+1].ix['NAV'] != 100:
#                    self.BarPlot = tempframe[i:]
#                    break
#            self.BarPlot = 100.0*self.BarPlot.pct_change()[1:]
#            self.BarPlot = self.BarPlot[self.BarPlot.columns[0]] - self.BarPlot[self.BarPlot.columns[1]]
#            self.BarPlot = pandas.DataFrame(self.BarPlot,columns = ['Yearly Perf Diff'])
#            self.BarPlot.index = [datetime.datetime.strftime(i, '%Y') for i in self.BarPlot.index]
            if hasattr(self,'PlotResult'):
                self.PlotResult = self.PlotResult / self.PlotResult.iloc[0]            
#            self.orderhandler()

    ## calculate nav, transaction cost, mtm etc
    def updatePnLandNAVhandler(self):
        #pdb.set_trace()
        if len(self.TempCapitalAllocation)!=0:
             # carry forward last position by default
            self.Position.loc[self.CurrentTime] = self.Position.loc[self.LastTime]
            # get capital allocation of last loop
            self.YestDayEnd = copy.deepcopy(self.CapitalAllocation.loc[self.LastTime])
            # temp capital allocation save current capital allocation while yestdayend is capital allocation at last time but querried at present
            # so yestdayend carries captial allocation post trade while tempcap alloc carries capital allocations before any trading
            # their difference amounts to traded value
            self.TradeLog.loc[self.LastTime,:]= numpy.subtract(self.YestDayEnd,self.TempCapitalAllocation)
            # today before trade is change is allocated capital due to price change. present value of allocated capital due to effect of market activity
            LongCurrentPriceRatio = numpy.divide(self.Close.loc[self.CurrentTime],self.Close.loc[self.LastTime])
            CurrentPriceRatio = numpy.divide(self.Close.loc[self.LastTime], self.Close.loc[self.CurrentTime])
            LongCurrentPriceRatio = LongCurrentPriceRatio.where(~numpy.isnan(LongCurrentPriceRatio), other = 1)#, inplace =True)#CurrentPriceRatio.where(~numpy.isnan(CurrentPriceRatio), other = 1)#
            CurrentPriceRatio = CurrentPriceRatio.where(~numpy.isnan(CurrentPriceRatio), other = 1)#, inplace = True)
            
            CurrentPriceRatio = CurrentPriceRatio.where(self.Position.loc[self.LastTime] ==-1, LongCurrentPriceRatio)#, inplace = True)
            
            self.TodayBeforeTrade = numpy.multiply(self.CapitalAllocation.loc[self.LastTime],CurrentPriceRatio)
            # update capital allocation for today
            #if self.CurrentTime == datetime.datetime(2013, 6, 30):
            #    pdb.set_trace()
            self.CapitalAllocation.loc[self.CurrentTime,:]=(self.TodayBeforeTrade)
            # mtm due to last days capital allocation
            self.MTM.loc[self.CurrentTime,:]=numpy.subtract(numpy.abs(self.TodayBeforeTrade),numpy.abs(self.YestDayEnd))
            self.TransactionCost.loc[self.CurrentTime,'TransactionCost']=numpy.sum((numpy.abs(self.TradeLog.loc[self.LastTime])))*(self.TransactionCostRate)
            self.NAV.loc[self.CurrentTime,'NAV']=self.NAV.loc[self.LastTime,'NAV'] + numpy.sum(self.MTM.loc[self.CurrentTime]) -self.TransactionCost.loc[self.CurrentTime,'TransactionCost']
        self.TempCapitalAllocation = copy.deepcopy(self.CapitalAllocation.loc[self.CurrentTime])
#        self.drawdownstoplosshandler()


    def updatePnLandNAVhandlerOptions(self):
        #pdb.set_trace()
        if len(self.TempQuantityAllocation)!=0:            
            # we have to calculate the NAV based on PNL
            # Suppose starting NAv is 5Cr, we take exposure based on this money, then calculated quantity based on exposure
            # NAV will represent the total money exposure  +- any PNL
            #First carry forward the last Quantity position, Current periond P&L will depend on Previous Quantity Position
            # This system is based on Quantity, rather than Position
            #pdb.set_trace()
            self.Quantity.loc[self.CurrentTime] = self.Quantity.loc[self.LastTime]
            CalQty = self.Quantity.loc[self.CurrentTime]
            
            tempDF = pandas.DataFrame([re.match(self.OptionTickerRegEx, iTicker).groupdict() for iTicker in CalQty.index], index = CalQty.index)
            tempDF.strike = tempDF.strike.astype('float')
           
            # for Short Optins, Strikes are considered as Exposure, 
            # If it is pair trade then average otherwise full strike is exposure
            Price = self.Close.loc[self.LastTime]            
            NegExposure = numpy.multiply(CalQty, tempDF.strike)/(1+ self.PairTrades)            
            NegExposure = NegExposure[NegExposure<0]
            
            #For Long Positions, Exposure is last price paid/ premium paid
            PosExposure = numpy.multiply(CalQty, Price).dropna()
            PosExposure = PosExposure[PosExposure>0]#.sum()
            GrossExposure = pandas.concat([NegExposure, PosExposure], axis = 0)  
            
            self.Exposure.loc[self.CurrentTime, :] = GrossExposure
            # carry forward last position by default            
            #self.Position.loc[self.CurrentTime] = self.Position.loc[self.LastTime]
            #calExposure = 
            # get capital allocation of last loop
            self.YestDayEndQty = copy.deepcopy(self.Quantity.loc[self.LastTime])
            # temp capital allocation save current capital allocation while yestdayend is capital allocation at last time but querried at present
            # so yestdayend carries captial allocation post trade while tempcap alloc carries capital allocations before any trading
            # their difference amounts to traded value
            
            self.TradeLogQty.loc[self.LastTime,:]= numpy.subtract(self.YestDayEndQty,self.TempQuantityAllocation)
            
            cashTicker = tempDF[tempDF.option_type == 'CASH']
            if len(cashTicker) >0:
                self.TradeLogQty.loc[self.LastTime,cashTicker.index[0]] = 0.0
            # today before trade is change is allocated capital due to price change. present value of allocated capital due to effect of market activity
            #LongCurrentPriceRatio = numpy.divide(self.Close.loc[self.CurrentTime],self.Close.loc[self.LastTime])
            #CurrentPriceRatio = numpy.divide(self.Close.loc[self.LastTime], self.Close.loc[self.CurrentTime])
            #LongCurrentPriceRatio = LongCurrentPriceRatio.where(~numpy.isnan(LongCurrentPriceRatio), other = 1)#, inplace =True)#CurrentPriceRatio.where(~numpy.isnan(CurrentPriceRatio), other = 1)#
            #CurrentPriceRatio = CurrentPriceRatio.where(~numpy.isnan(CurrentPriceRatio), other = 1)#, inplace = True)
            
            #CurrentPriceRatio = CurrentPriceRatio.where(self.Position.loc[self.LastTime] ==-1, LongCurrentPriceRatio)#, inplace = True)
            
            #self.TodayBeforeTradeQty = numpy.multiply(self.CapitalAllocation.loc[self.LastTime],CurrentPriceRatio)
            # update capital allocation for today
            #self.CapitalAllocation.loc[self.CurrentTime,:]=(self.TodayBeforeTrade)
            
            # mtm due to last days capital allocation
            PriceChg = numpy.subtract(self.Close.loc[self.CurrentTime, :], self.Close.loc[self.LastTime, :])
            PriceChg = PriceChg.where(~numpy.isnan(PriceChg), other = 0)
            self.MTM.loc[self.CurrentTime, :] = numpy.multiply(PriceChg, CalQty)
            #pdb.set_trace()
            
            self.TransactionCost.loc[self.CurrentTime,'TransactionCost'] = numpy.sum(numpy.abs(numpy.multiply(self.TradeLogQty.loc[self.LastTime], Price*self.TransactionCostRate).dropna())) if len(NegExposure) !=0 else 0
            tempChg = 1 + numpy.divide(numpy.sum(self.MTM.loc[self.CurrentTime]) - self.TransactionCost.loc[self.CurrentTime,'TransactionCost'], GrossExposure.abs().sum())
            self.NAV.loc[self.CurrentTime,'NAV'] = self.NAV.loc[self.LastTime,'NAV']*(1 if numpy.isnan(tempChg) else tempChg)#-self.TransactionCost.loc[self.CurrentTime,'TransactionCost']
        self.TempQuantityAllocation = copy.deepcopy(self.Quantity.loc[self.CurrentTime])
        
    ## generate order and ordern handling(never used and untested, not that useful)
#    def ordercreator(self, thisorder, updateflag = False):
#        for order in self.AllOrders:
#            if order.Symbol == thisorder.Symbol and updateflag == True:
#                self.AllOrders.remove(order)
#                self.AllOrders.append(thisorder)
#                break
#            elif order.Symbol == thisorder.Symbol and updateflag == False:
#                break
#            elif len(self.AllOrders) - self.AllOrders.index(order) == 1:
#                self.AllOrders.append(thisorder)
#
#
#    def orderhandler(self):
#        staleorders = [order for order in self.AllOrders if order.goodtill < self.CurrentTime]
#        for order in staleorders:
#            self.AllOrders.remove(order)
#
#        currentvalidorders = [order for order in self.AllOrders if order.validfrom <= self.CurrentTime and order.goodtill >= self.CurrentTime]
#        self.AllOrders = currentvalidorders
#
#        for order in self.AllOrders:
#            self.CurrentCapitalOnThisStock = self.CapitalAllocation.ix[self.CurrentTime , order.Symbol]
#            self.CurrentStockPrice = self.Close.ix[self.CurrentTime , order.Symbol]
#            if order.ordertype == 'MO':
#                self.CapitalAllocation.ix[self.CurrentTime , order.Symbol] = order.ordervalue
#                self.AllOrders.remove(order)
#            elif order.ordertype == 'LO':
#                if order.orderpriceparameter >= self.CurrentStockPrice and order.ordervalue > 0:
#                    self.CapitalAllocation.ix[self.CurrentTime , order.Symbol] = order.ordervalue
#                    self.AllOrders.remove(order)
#                elif order.orderpriceparameter <= self.CurrentStockPrice and order.ordervalue < 0:
#                    self.CapitalAllocation.ix[self.CurrentTime , order.Symbol] = order.ordervalue
#                    self.AllOrders.remove(order)
    


    ## save statistics of simulation in a excel file
    def savebacktestresult_minimal(self, filepath, backtest_tickers, fullData=True):
        t1 = time.time()
        writer = pandas.ExcelWriter(filepath, engine='xlsxwriter')
        if fullData:
            self.BackTestData.Close = self.BackTestData.Close[backtest_tickers]
            self.BackTestData.Close.to_excel(writer, 'Prices')  # , engine = 'xlsxwriter')
            self.MTM = self.MTM[backtest_tickers]
            self.MTM.to_excel(writer, 'MTM')  # , engine = 'xlsxwriter')
            self.TradeLog = self.TradeLog[backtest_tickers]
            self.TradeLog.to_excel(writer, 'TradeLog')  # , engine = 'xlsxwriter')
            self.TransactionCost.to_excel(writer, 'TransactionCost')  # , engine = 'xlsxwriter')
            self.CapitalAllocation = self.CapitalAllocation[backtest_tickers]
            self.CapitalAllocation.to_excel(writer, 'Capital Allocation')  # , engine = 'xlsxwriter')
            if hasattr(self, 'Quantity'):
                self.Quantity = self.Quantity[backtest_tickers]
                self.Quantity.to_excel(writer, 'Quantity')  # , engine = 'xlsxwriter')
                self.Exposure = self.Exposure[backtest_tickers]
                self.Exposure.to_excel(writer, 'Exposure')
        self.NAV.to_excel(writer, 'NAV')  # , engine = 'xlsxwriter')
        self.Position = self.Position[backtest_tickers]
        self.Position.to_excel(writer, 'Position')  # , engine = 'xlsxwriter')
        self.Churn = pandas.DataFrame(self.TradeLog[self.TradeLog > 0].sum(axis=1), columns=['NAV']) / self.NAV
        self.Churn.columns = ['Churn']
        self.Weight = numpy.divide(self.CapitalAllocation, self.NAV)
        latestPos = self.Weight.iloc[-1]  # .transpose()
        latestPos = latestPos[latestPos != 0]
        latestPos = latestPos.sort_values()
        latestPos.to_excel(writer, 'LastPosition')
        if hasattr(self, 'Weights'):
            self.Weights = self.Weighs[backtest_tickers]
            self.Weights.to_excel(writer, 'Weights')  # , engine = 'xlsxwriter')
        if hasattr(self, 'PositionWOSL'):
            # pdb.set_trace()
            self.PositionWOSL = self.PositionWOSL[backtest_tickers]
            self.PositionWOSL.to_excel(writer, 'PositionWOSL')
        if hasattr(self, 'TradedValue') and hasattr(self.BackTestData, 'LotSize'):
            lotSize = self.BackTestData.LotSize.iloc[-2:, :]
            Price = self.Close.iloc[-2:, :]  # Ideally this price should be Next Month Fut price during expiry date
            RecentPositionValue = self.Weight.iloc[-2:, :] * self.TradedValue
            LotValue = numpy.multiply(lotSize, Price)
            self.Lots = numpy.round(numpy.divide(RecentPositionValue, LotValue), 0)
            # self.LotsR = numpy.round(self.Lots, 0)
            ActualValue = numpy.multiply(numpy.multiply(self.Lots, Price), lotSize)
            NetPosition = ActualValue[ActualValue.columns[~ActualValue.columns.isin(['Cash', self.IndexName])]].sum(
                axis=1)
            self.Lots.loc[:, self.IndexName] = numpy.round(numpy.divide(-NetPosition, LotValue.loc[:, self.IndexName]),
                                                           0)
            Chg = pandas.DataFrame(self.Lots.diff().iloc[-1])
            Chg.columns = ['Trades']
            Chg['LotSize'] = lotSize.iloc[-1]
            Chg['Transaction'] = numpy.multiply(Chg['Trades'], Chg['LotSize'])
            self.Lots = self.Lots.transpose()
            self.Lots = pandas.concat([self.Lots, Chg], axis=1)
            self.Lots['Ticker'] = self.Lots.index
            self.Lots = self.Lots.loc[self.Lots.index.isin(self.modelTable.index), :]
            self.Lots['Symbol'] = [self.modelTable.loc[i, 'NSE'] for i in self.Lots.index]
            self.Lots['Buy/Sell'] = [1 if i > 0 else i for i in self.Lots['Transaction']]
            self.Lots['Buy/Sell'] = [2 if i < 0 else i for i in self.Lots['Buy/Sell']]
            self.Lots['Quantity'] = self.Lots['Transaction'].abs()
            self.Lots['Price'] = Price.iloc[-1]
            self.Lots['Instrument Name'] = ['FUTIDX' if i == 'NIFTY' else 'FUTSTK' for i in self.Lots['Symbol']]
            self.Lots = self.Lots[backtest_tickers]
            self.Lots.to_excel(writer, 'Transactions')
        # self.Churn.to_excel(writer,'Churn')
        writer.save()
        t2 = time.time()
        print('File Saved in:', round((t2 - t1) / 60, 1), 'Mins', sep=' ')


    def savebacktestresult(self, filepath, fullData = True):
        t1 = time.time()
        writer = pandas.ExcelWriter(filepath, engine = 'xlsxwriter')
        if fullData:
            self.BackTestData.Close.to_excel(writer,'Prices')#, engine = 'xlsxwriter')
            self.MTM.to_excel(writer,'MTM')#, engine = 'xlsxwriter')
            self.TradeLog.to_excel(writer,'TradeLog')#, engine = 'xlsxwriter')
            self.TransactionCost.to_excel(writer,'TransactionCost')#, engine = 'xlsxwriter')
            self.CapitalAllocation.to_excel(writer,'Capital Allocation')#, engine = 'xlsxwriter')
            if hasattr(self, 'Quantity'):
                self.Quantity.to_excel(writer,'Quantity')#, engine = 'xlsxwriter')
                self.Exposure.to_excel(writer,'Exposure')
            
        self.NAV.to_excel(writer,'NAV')#, engine = 'xlsxwriter')
        self.Position.to_excel(writer,'Position')#, engine = 'xlsxwriter')        
        self.Churn = pandas.DataFrame(self.TradeLog[self.TradeLog >0].sum(axis = 1), columns = ['NAV'])/self.NAV
        self.Churn.columns = ['Churn']
        self.Weight = numpy.divide(self.CapitalAllocation, self.NAV)
        latestPos = self.Weight.iloc[-1]#.transpose()
        latestPos = latestPos[latestPos !=0]
        latestPos = latestPos.sort_values()
        latestPos.to_excel(writer, 'LastPosition')
        if hasattr(self, 'Weights'):
            self.Weights.to_excel(writer,'Weights')#, engine = 'xlsxwriter')
        if hasattr(self, 'PositionWOSL'):
            #pdb.set_trace()
            self.PositionWOSL.to_excel(writer,'PositionWOSL')
        
        if hasattr(self, 'FactorRanks'):
            self.FactorRanks.to_excel(writer,'FactorRanks')
            
        if hasattr(self, 'TradedValue') and hasattr(self.BackTestData, 'LotSize'):
            lotSize = self.BackTestData.LotSize.iloc[-2:, :]
            Price = self.Close.iloc[-2:, :]# Ideally this price should be Next Month Fut price during expiry date
            RecentPositionValue = self.Weight.iloc[-2:, :]*self.TradedValue
            LotValue = numpy.multiply(lotSize, Price)
            self.Lots = numpy.round(numpy.divide(RecentPositionValue, LotValue), 0)
            #self.LotsR = numpy.round(self.Lots, 0)
            ActualValue = numpy.multiply(numpy.multiply(self.Lots, Price), lotSize)
            NetPosition = ActualValue[ActualValue.columns[~ActualValue.columns.isin(['Cash', self.IndexName])]].sum(axis = 1)            
            self.Lots.loc[:, self.IndexName] = numpy.round(numpy.divide(-NetPosition, LotValue.loc[:, self.IndexName]), 0)
            Chg = pandas.DataFrame(self.Lots.diff().iloc[-1])
            Chg.columns = ['Trades']
            Chg['LotSize'] = lotSize.iloc[-1]
            Chg['Transaction'] = numpy.multiply(Chg['Trades'], Chg['LotSize'])
            self.Lots = self.Lots.transpose()
            self.Lots = pandas.concat([self.Lots, Chg], axis = 1)
            self.Lots['Ticker'] = self.Lots.index            
            self.Lots = self.Lots.loc[self.Lots.index.isin(self.modelTable.index), :]
            self.Lots['Symbol'] = [self.modelTable.loc[i, 'NSE'] for i in self.Lots.index]
            self.Lots['Buy/Sell'] = [1 if i> 0 else i for i in self.Lots['Transaction']]
            self.Lots['Buy/Sell'] = [2 if i< 0 else i for i in self.Lots['Buy/Sell']]
            self.Lots['Quantity'] = self.Lots['Transaction'].abs()
            self.Lots['Price'] = Price.iloc[-1]
            self.Lots['Instrument Name'] = ['FUTIDX' if i == 'NIFTY' else 'FUTSTK' for i in self.Lots['Symbol']]
            self.Lots.to_excel(writer, 'Transactions')
        #self.Churn.to_excel(writer,'Churn')
        writer.save()
        t2 = time.time()
        print('File Saved in:', round((t2-t1)/60, 1), 'Mins', sep = ' ')

    def DetectPostionStartDate(self, ticker, positionMat, forStopLoss = False):
         '''positionMat: self.Position/self.PositionWOSL
         This function have dual use case:
             1. If we have to detect the Posiiton Start Date for the Currently Closing Position- Used in OrderBook Genration
             For this use case we have to use the last position change date, previous to the last date.
             2. To Detect the Current Position, for using it in Stop Loss Function.
             For Stop Loss use we have to compare the current position to the previous positions
             
         '''         
         TradeTakenDate = self.CurrentTime
         tempList = positionMat.loc[:self.CurrentTime, ticker]
         tempList = tempList.sort_index(ascending = False)
         iDate = tempList.index[0]
         if forStopLoss:
             PositionToCompare = tempList.iloc[0]
         else:
             PositionToCompare = tempList.iloc[1]
            
         #if tempList.iloc[0] != tempList.iloc[1]:
         #    return TradeTakenDate
         try:
             if tempList.iloc[1] != 0:
                 for iTime in list(tempList.index[1:]):
                     if tempList.loc[iTime] != PositionToCompare: #tempList.iloc[1]:
                         TradeTakenDate = iDate
                         break
                     else:
                         if iTime == tempList.index[-1] and tempList.iloc[-1] == PositionToCompare:#tempList.iloc[1]:
                             TradeTakenDate = iTime
                         iDate = iTime
         except:
            pass
         return TradeTakenDate

    def OrderPosition(self, iTicker, entry_date, position, strategyID = '', exit_date='', options = None):        
        self.order = Order()
        self.order.symbol = iTicker
        self.order.segment = Segment.FT
        self.order.quantity = 1
        
        self.order.strategy_id = strategyID
        if position == 1:
            self.order.position = Position.LONG
        elif position == -1:
            self.order.position = Position.SHORT
        self.order.entry_date = entry_date
        if hasattr(self, 'Quantity'):
            self.order.quantity = int(abs(self.Quantity.loc[entry_date, iTicker]))
        self.order.entry_price = self.Close.loc[entry_date, iTicker]
        if exit_date != '':
            exit_date = exit_date# Do nothing
        elif numpy.isnan(self.Close.loc[self.CurrentTime, iTicker]):
            try:
                exit_date = self.Close.loc[entry_date:self.CurrentTime, iTicker].dropna().index[-1]
            except:
                exit_date = self.LastTime
        else:
            exit_date = self.CurrentTime
        self.order.exit_date = exit_date
        self.order.exit_price = self.Close.loc[exit_date, iTicker]
        if options is not None:
            optionDetail = re.match(self.OptionTickerRegEx, iTicker).groupdict()
            #optionDetail = re.match(r'(?P<symbol>[A-Z]+)(?P<strike>\d+)(?P<option_type>[A-Z]+)(?P<expiry_date>\d+[A-Z]+\d+)', iTicker).groupdict()
            self.order.segment = Segment.OP
            #self.order.symbol = optionDetail['symbol']
            self.order.expiry_date = datetime.datetime.strptime(optionDetail['expiry_date'], '%d%b%y') +datetime.timedelta(hours = 16)
            if optionDetail['option_type'] == 'CE':
                self.order.option_type = OptionType.CE
            elif optionDetail['option_type'] == 'PE':
                self.order.option_type = OptionType.PE
            self.order.strike_price = float(optionDetail['strike'])
        self.trade_reg.append_trade(self.order)
    
    def StopLoss_Advanced(self, trail: bool = False, byGroup: bool = False, StopLossLimit: float = -0.2, groupDict: dict = {}):
        '''
        Parameters
        ----------
        trail : bool, [True, False]
            DESCRIPTION. The default is False.
        byGroup : bool, [True, False]
            DESCRIPTION. The default is False.
        StopLossLimit : float, optional
            DESCRIPTION. The Default is -20%.
        groupDict : dict, optional
            DESCRIPTION. The default is {}.
        Returns
        -------
        None. It Update the Position/Quantity DF
        '''
        # trail = True
        # byGroup = False
        # StopLossLimit = -0.2 
        # groupDict = {} # Optional
        #pdb.set_trace()
        CurrentPosition = self.Position.loc[self.CurrentTime]
        CurrentPosition = CurrentPosition[CurrentPosition !=0]   
        if len(CurrentPosition) ==0:
            return
        tradeStartDates = pandas.Series({iTicker : self.DetectPostionStartDate(iTicker, self.Position) for iTicker in CurrentPosition.index})
        if byGroup:
            if len(groupDict) == 0:
                # code to detect all tickers, groupby NSE scrips( we are using Regex to get the scrips)
                tempDF = pandas.DataFrame([re.match(self.OptionTickerRegEx, iTicker).groupdict() for iTicker in CurrentPosition.index], index = CurrentPosition.index)
                groupDict = {symbol: tempDF[tempDF['symbol'] == symbol].index for symbol in tempDF['symbol'].unique()}            
            for symbol, tickersList in groupDict.items():
                TradeTakenDate = tradeStartDates.loc[tickersList].max()
                EntryPrice = numpy.multiply(self.Close.loc[TradeTakenDate, tickersList], self.Position.loc[TradeTakenDate, tickersList]).sum()
                
                SLPrice = EntryPrice*(1+numpy.sign(EntryPrice)*StopLossLimit)                        
                CurPrice = numpy.multiply(self.Close.loc[self.CurrentTime, tickersList], self.Position.loc[self.CurrentTime, tickersList]).sum()
                if trail:
                    TrailEntryPrice = numpy.multiply(self.Close.loc[TradeTakenDate:self.LastTime, tickersList], self.Position.loc[TradeTakenDate, tickersList]).sum(axis = 1)
                    TrailEntryPrice = TrailEntryPrice.max() if EntryPrice <= 0 else TrailEntryPrice.min()
                    SLPrice = TrailEntryPrice*(1+numpy.sign(TrailEntryPrice)*StopLossLimit)
                if CurPrice <= SLPrice:
                    self.Quantity.loc[self.CurrentTime, tickersList] = 0
                    self.Position.loc[self.CurrentTime, tickersList] = 0
                    self.PositionExitDF.loc[self.CurrentTime, tickersList] = -1
        elif not byGroup:
            for iTicker in CurrentPosition.index:
                TradeTakenDate = tradeStartDates.loc[iTicker]
                EntryPrice = numpy.multiply(self.Close.loc[TradeTakenDate, iTicker], self.Position.loc[TradeTakenDate, iTicker]).sum()                
                SLPrice = EntryPrice*(1+numpy.sign(EntryPrice)*StopLossLimit)                        
                CurPrice = numpy.multiply(self.Close.loc[self.CurrentTime, iTicker], self.Position.loc[self.CurrentTime, iTicker]).sum()
                if trail:
                    if EntryPrice<= 0:
                        TrailEntryPrice = numpy.multiply(self.Close.loc[TradeTakenDate:self.LastTime, iTicker], self.Quantity.loc[TradeTakenDate, iTicker]).max()
                    else:
                        TrailEntryPrice = numpy.multiply(self.Close.loc[TradeTakenDate:self.LastTime, iTicker], self.Quantity.loc[TradeTakenDate, iTicker]).min()
                    SLPrice = TrailEntryPrice*(1+numpy.sign(TrailEntryPrice)*StopLossLimit)
                if CurPrice <= SLPrice:
                    self.Quantity.loc[self.CurrentTime, iTicker] = 0
                    self.Position.loc[self.CurrentTime, iTicker] = 0
                    self.PositionExitDF.loc[self.CurrentTime, iTicker] = -1
    
    def StopLossTrail(self, ticker, StopLossLimit):
        '''
        Parameters
        ----------
        ticker : TYPE
            DESCRIPTION.
        StopLossLimit : TYPE
            DESCRIPTION.
        
        Description:
        -----------
        This code is a class that implements a stop-loss trailing mechanism for a given ticker. The method takes two arguments: ticker, which is the ticker for which the stop-loss mechanism is being applied, and StopLossLimit, which is the stop-loss threshold in terms of the percentage difference between the current price and the maximum or minimum price since the trade was taken.
        The method begins by using the DetectPostionStartDate method to find the date on which the trade was taken for the given ticker. If this date is the same as the current time, the method returns without doing anything. Otherwise, the method looks up the current position for the ticker in the PositionWOSL dataframe and calculates two quantities:

        AwayFromLow: the difference between the Current low and the maximum of the close price at the trade date and the high prices since the trade date, divided by the maximum of these two quantities and expressed as a percentage
        AwayFromHigh: the difference between the minimum of the close price at the trade date and the low prices since the trade date and the current high price, divided by the current high price and expressed as a percentage
        If the position is long (i.e., 1) and AwayFromLow is less than the stop-loss limit, or if the position is short (i.e., -1) and AwayFromHigh is less than the stop-loss limit, the method updates the PositionExitDF dataframe by setting the position to -1 at the current time step for the given ticker.
        
        Working
        -------
        Short Positon: if Current High price is above SL(10%) of the Minimum from entry price
        Long Position: if Current Low Price is Below SL (10%) of the Maximum form the entry price
        '''
        TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL, forStopLoss = True)
        if TradeTakenDate == self.CurrentTime:
            return
        position = self.PositionWOSL.loc[self.CurrentTime, ticker]
        try:
            #Current Low away than High from Trade Date
            AwayFromLow = self.BackTestData.Low.loc[self.CurrentTime, ticker]/max(self.Close.loc[TradeTakenDate, ticker], self.BackTestData.High.loc[TradeTakenDate+datetime.timedelta(1):self.CurrentTime, ticker].max()) -1
            # Current High Away than Low From Trade Date
            AwayFromHigh = -(self.BackTestData.High.loc[self.CurrentTime, ticker]/min(self.Close.loc[TradeTakenDate, ticker], self.BackTestData.Low.loc[TradeTakenDate+datetime.timedelta(1):self.CurrentTime, ticker].min()) -1)
            if (position == 1 and AwayFromLow < StopLossLimit) or (position == -1 and AwayFromHigh < StopLossLimit):
                self.PositionExitDF.loc[self.CurrentTime, ticker] = -1
                if position == 1:
                    # it replaces the Close Price Data to the Stop Loss Price
                    self.Close.loc[self.CurrentTime, ticker] = (1+ StopLossLimit*position)*max(self.Close.loc[TradeTakenDate, ticker], self.BackTestData.High.loc[TradeTakenDate+datetime.timedelta(1):self.CurrentTime, ticker].max())
                elif position == -1:
                    self.Close.loc[self.CurrentTime, ticker] = (1+ StopLossLimit*position)*min(self.Close.loc[TradeTakenDate, ticker], self.BackTestData.Low.loc[TradeTakenDate+datetime.timedelta(1):self.CurrentTime, ticker].min())
        except:
            print('Error with Stop Loss!', ticker, self.CurrentTime.strftime('%d%b%y'))
    
    def StopLossTrail_Minutes(self, ticker, StopLossLimit):
        TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL, forStopLoss = True)
        if TradeTakenDate == self.CurrentTime:
            return
        position = self.PositionWOSL.loc[self.CurrentTime, ticker]
        try:
            #Current Low away than High from Trade Date
            AwayFromLow = self.BackTestData.Low.loc[self.CurrentTime, ticker]/max(self.Close.loc[TradeTakenDate, ticker], self.BackTestData.High.loc[TradeTakenDate+datetime.timedelta(1):self.CurrentTime, ticker].max()) -1
            # Current High Away than Low From Trade Date
            AwayFromHigh = -(self.BackTestData.High.loc[self.CurrentTime, ticker]/min(self.Close.loc[TradeTakenDate, ticker], self.BackTestData.Low.loc[TradeTakenDate+datetime.timedelta(1):self.CurrentTime, ticker].min()) -1)
            if (position == 1 and AwayFromLow < StopLossLimit) or (position == -1 and AwayFromHigh < StopLossLimit):
                self.PositionExitDF.loc[self.CurrentTime, ticker] = -1
        except:
            print('Error with Stop Loss!', ticker, self.CurrentTime.strftime('%d%b%y'))
    
    def StopLoss(self, ticker, StopLossLimit):
         TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL, forStopLoss = True)
         if TradeTakenDate == self.CurrentTime:
             return
         try:
             rets = (self.Close.loc[self.CurrentTime, ticker]/self.Close.loc[TradeTakenDate, ticker]) -1
             if rets*self.PositionWOSL.loc[self.CurrentTime, ticker] < StopLossLimit:
                 self.PositionExitDF.loc[self.CurrentTime, ticker] = -1
                 self.Close.loc[self.CurrentTime, ticker] = (1+ StopLossLimit*self.PositionWOSL.loc[self.CurrentTime, ticker])*self.Close.loc[TradeTakenDate, ticker]
                 # it replaces the Close Price Data to the Stop Loss Price
                 #self.EndOrderPosition(self.CurrentTime)
         except:
             print('Error with Stop Loss!', ticker, self.CurrentTime.strftime('%d%b%y'))

    def Target(self, ticker, TargetLimit):
         TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL, forStopLoss = True)        
         try:
             rets = (self.Close.loc[self.CurrentTime, ticker]/self.Close.loc[TradeTakenDate, ticker]) -1
             if rets*self.PositionWOSL.loc[self.CurrentTime, ticker] >= TargetLimit:
                 self.PositionExitDF.loc[self.CurrentTime, ticker] = -1
                 self.Close.loc[self.CurrentTime, ticker] = (1+ TargetLimit*self.PositionWOSL.loc[self.CurrentTime, ticker])*self.Close.loc[TradeTakenDate, ticker]
                 # it replaces the Close Price Data to the Target Price
                 #self.EndOrderPosition(self.CurrentTime)
         except:
             print('Error with Target!', ticker, self.CurrentTime.strftime('%d%b%y'))

    def UpdateOrderBook(self, strategyID = '', options = None):
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
            if hasattr(self, 'Strategy'):
                strategyID = str(self.Strategy.loc[entry_date, iTicker])            
                #['Name', 'StrikePrice', 'Call_Or_Put', 'ExpiryDate']
            self.OrderPosition(iTicker, entry_date, position, strategyID, options = options)
            
        if self.CurrentTime == max(self.UpdateDates):
            CurrentPosition = CurrentPosition[CurrentPosition != 0].dropna()
            for iTicker in CurrentPosition.index:
                position = self.Position.loc[self.CurrentTime, iTicker]
                entry_date = self.DetectPostionStartDate(iTicker, self.Position)
                if hasattr(self, 'Strategy'):
                    strategyID = str(self.Strategy.loc[entry_date, iTicker])
                self.OrderPosition(iTicker, entry_date, position, strategyID, self.CurrentTime, options = options)

    def MD1_RSIwith50SMATrend(self, tickersList):
        # THis function have bug, Suppose there is some time period where security is not being traded, then restarted trading, then This model will not show any trade signal,
        # Due to Starting Position not being negative.
        '''
        Model:
        -----
        RSI+ 50SMA Trend
        
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION: Updates PositionWOSL DF
        
        Description:
        ------------
        This code is a method in a class that implements a trading strategy based on the relative strength index (RSI) and 50-day simple moving average (SMA). The method takes a single argument tickersList, 
        which is a list of tickers for which the strategy is to be applied. The method first initializes a dictionary called StartingPosition, which will be used to store the starting positions for each 
        ticker. If the StartingPosition attribute does not yet exist, the method creates it and sets the starting position for each ticker in tickersList to -1. If the StartingPosition attribute already exists, 
        the method adds any tickers in tickersList that are not already in the dictionary and sets their starting positions to -1.
        The method then retrieves the 2RSI and 50-day SMA data for the tickersList from the BackTestData dataframe, and the close prices for the current time step from the Close dataframe. It updates the 
        SMA50Position dataframe to reflect whether the current close price for each ticker is above or below its 50-day SMA.
        The method then defines a set of tickers for which the last RSI signal was negative or the starting position is negative, and a set of tickers for which the last RSI was less than 10 and the current 
        RSI is greater than 10. It updates the RSI2Position dataframe to reflect a positive RSI signal for these tickers, and also updates the StartingPosition dictionary to reflect a neutral starting position 
        for these tickers.
        The method then defines a set of tickers for which the last RSI signal was positive, and a set of tickers for which the last RSI was greater than 90 and the current RSI is less than 90. It updates the 
        RSI2Position dataframe to reflect a negative RSI signal for these tickers.
        Finally, the method updates the RSI2Position dataframe for tickers that do not fall into any of the above categories, setting their positions to the same as in the previous time step or to negative if 
        their starting position is negative. The method also updates the Strategy dataframe to reflect the strategy being used for each ticker at the current time step.
        '''
        if not hasattr(self, 'StartingPosition'):
            self.StartingPosition = {}
            for ticker in tickersList:
                self.StartingPosition[ticker] = -1        
        elif hasattr(self, 'StartingPosition'):
            for ticker in tickersList:
                if ticker not in self.StartingPosition.keys() or numpy.isnan(self.BackTestData.SMA50.loc[self.CurrentTime, ticker]):
                    self.StartingPosition[ticker] = -1
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'RSIwith50SMATrend'
                
        RSI = self.BackTestData.RSI2.loc[:, tickersList]
        SMA = self.BackTestData.SMA50.loc[self.CurrentTime, tickersList]
        Price = self.Close.loc[self.CurrentTime, tickersList]
        self.SMA50Position.loc[self.CurrentTime, Price[Price > SMA].dropna().index] = 1 #price is above SMA
        self.SMA50Position.loc[self.CurrentTime, Price[Price < SMA].dropna().index] = -1 #price is below SMA        
        
        #securities where last RSI signal is -1 or starting RSI position is -1
        #lastRSISignalNeg = set.union(set(Price[self.RSI2Position.loc[self.LastTime, tickersList] == -1].dropna().index), set([item for item in self.StartingPosition if self.StartingPosition[item] == -1]))
        lastRSISignalNeg = set.intersection(set.union(set(Price[self.RSI2Position.loc[self.LastTime, tickersList] == -1].dropna().index), set([item for item in self.StartingPosition if self.StartingPosition[item] == -1])), set(Price.index))
        #securitis where last RSI was less than 10 and Current Greater than 10 out of the above listed securities
        lastRSIless10CurGrtr10 = set.intersection(set(Price.loc[lastRSISignalNeg][RSI.loc[self.LastTime, RSI.columns.isin(lastRSISignalNeg)]< 10].dropna().index), set(Price.loc[lastRSISignalNeg][RSI.loc[self.CurrentTime, RSI.columns.isin(lastRSISignalNeg)]> 10].dropna().index))
        
        self.RSI2Position.loc[self.CurrentTime, lastRSIless10CurGrtr10] = 1# RSI Signal Positive for these type of securities
        for it in list(lastRSIless10CurGrtr10):
            self.StartingPosition[it] = 0 #Change starting Position signal also to be neutral from negative
        #RSI negative for Securties after removing the names where we got the RSI signal
        self.RSI2Position.loc[self.CurrentTime, lastRSISignalNeg.difference(lastRSIless10CurGrtr10)] = 1
        self.RSI2Position.loc[self.CurrentTime, self.RSI2Position.columns.isin([item for item in self.StartingPosition if self.StartingPosition[item] == -1])] = -1
        # from the last rsi negative stocks remove above stocks with current RSI positive or still no signal is generated
        temptickrs1 = lastRSISignalNeg.difference(lastRSIless10CurGrtr10, set([item for item in self.StartingPosition if self.StartingPosition[item] == -1]))
        self.RSI2Position.loc[self.CurrentTime, temptickrs1] = self.RSI2Position.loc[self.LastTime, temptickrs1]
        
        #securities where last RSI signal is Positive
        lastRSISignalPos = set(Price[self.RSI2Position.loc[self.LastTime, tickersList] == 1].dropna().index)
        #securities where last RSI was greater than 90 and Current less than 90 out of the above securities
        lastRSIgrtr90CurLess90 = set.intersection(set(Price.loc[lastRSISignalPos][RSI.loc[self.LastTime, RSI.columns.isin(lastRSISignalPos)]> 90].dropna().index), set(Price.loc[lastRSISignalPos][RSI.loc[self.CurrentTime, RSI.columns.isin(lastRSISignalPos)]< 90].dropna().index))
        self.RSI2Position.loc[self.CurrentTime, lastRSIgrtr90CurLess90] = -1# RSI Signal Negative for these type of securities
        temptickrs2 = lastRSISignalPos.difference(lastRSIgrtr90CurLess90)
        self.RSI2Position.loc[self.CurrentTime, temptickrs2] = self.RSI2Position.loc[self.LastTime, temptickrs2]
        
        SignalTickers = Price[self.SMA50Position.loc[self.CurrentTime, tickersList] == self.RSI2Position.loc[self.CurrentTime, tickersList]].dropna().index
        if len(SignalTickers)>0:
            self.PositionWOSL.loc[self.CurrentTime, SignalTickers] = self.SMA50Position.loc[self.CurrentTime, SignalTickers]#.values#[0]
        self.PositionWOSL.loc[self.CurrentTime, tickersList.difference(SignalTickers)] = 0
        
        for iTicker in SignalTickers:
            if hasattr(self, 'StopLossLimit'):
                self.StopLoss(iTicker, self.StopLossLimit)
            else:
                self.StopLoss(iTicker, -0.08)
            #self.StopLossTrail(iTicker, -0.08)# -5% StopLoss
    

    def MD2_RSI50SMA(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description:
        ------------
        This is a method to implement a trading strategy based on the relative strength index (RSI) and the 50-day simple moving average (SMA). The method takes a single argument, tickersList, which is a list of tickers for which the strategy is to be applied.
        The method first updates the Strategy dataframe to reflect the strategy being used for each ticker at the current time step. It then retrieves the RSI and 50-day SMA data for the tickers in tickersList from the BackTestData dataframe, and the close prices for the current time step from the Close dataframe. It updates the SMA50Position dataframe to reflect whether the current close price for each ticker is above or below its 50-day SMA.
        The method then updates the RSI2Position dataframe to reflect a positive RSI signal for tickers with an RSI less than 10 and a negative RSI signal for tickers with an RSI greater than 90. It then defines a set of tickers, SignalTickers, for which the RSI signal and the SMA50Position are the same. If there are any tickers in SignalTickers, the method updates the PositionWOSL dataframe to reflect the RSI signal for these tickers.
        For tickers not in SignalTickers, the method defines three sets of tickers based on certain conditions: cond1Tickers, cond2Tickers, and cond3Tickers. It then defines a set called conditionTickers as the union of cond1Tickers and the intersection of cond2Tickers and cond3Tickers. If there are any tickers in conditionTickers, the method updates the PositionWOSL dataframe to reflect a neutral position for these tickers. For tickers not in conditionTickers, the method updates the PositionWOSL dataframe to reflect the position at the previous time step.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'RSI50SMA'
        
        RSI = self.BackTestData.RSI2.loc[:, tickersList]
        SMA = self.BackTestData.SMA50.loc[self.CurrentTime, tickersList]
        Price = self.Close.loc[self.CurrentTime, tickersList]
        self.SMA50Position.loc[self.CurrentTime, Price[Price > SMA].dropna().index] = 1 #price is above SMA
        self.SMA50Position.loc[self.CurrentTime, Price[Price < SMA].dropna().index] = -1 #price is below SMA        
        
        self.RSI2Position.loc[self.CurrentTime, Price[RSI.loc[self.CurrentTime] < 10].dropna().index] = 1
        self.RSI2Position.loc[self.CurrentTime, Price[RSI.loc[self.CurrentTime] > 90].dropna().index] = -1
        
        SignalTickers = Price[self.RSI2Position.loc[self.CurrentTime, tickersList] == self.SMA50Position.loc[self.CurrentTime, tickersList]].dropna().index
        if len(SignalTickers)>0:
            self.PositionWOSL.loc[self.CurrentTime, SignalTickers] = self.RSI2Position.loc[self.CurrentTime, SignalTickers]#.values[0]
        #otherwise-> IF Previous Position is not same as Current SMA Position or (Current RSI Position is not neutral and Last Position is different than Current RSI Position) then Neutral
        RemainTickersMD2 = tickersList.difference(SignalTickers)
        #Last Position is not matching to Current SMA 50 Position
        cond1Tickers = Price.loc[RemainTickersMD2][self.PositionWOSL.loc[self.LastTime, RemainTickersMD2] != self.SMA50Position.loc[self.CurrentTime, RemainTickersMD2]].dropna().index
        # Current RSI Position is not zero and Last Position is not matching to Current RSI Position
        cond2Tickers = Price.loc[RemainTickersMD2][self.RSI2Position.loc[self.CurrentTime, RemainTickersMD2] != 0].dropna().index
        cond3Tickers = Price.loc[RemainTickersMD2][self.RSI2Position.loc[self.CurrentTime, RemainTickersMD2] != self.PositionWOSL.loc[self.LastTime, RemainTickersMD2]].dropna().index
        #conditionTickers = {}
        conditionTickers = set(cond1Tickers).union(set.intersection(set(cond2Tickers), set(cond3Tickers)))
        if len(conditionTickers)>0:
            self.PositionWOSL.loc[self.CurrentTime, conditionTickers] = 0
        self.PositionWOSL.loc[self.CurrentTime, RemainTickersMD2.difference(conditionTickers)] = self.PositionWOSL.loc[self.LastTime, RemainTickersMD2.difference(conditionTickers)]
        PositionTickers = self.PositionWOSL.loc[self.CurrentTime]
        PositionTickers = PositionTickers[PositionTickers !=0].dropna()
        for iTicker in PositionTickers.index:
            self.StopLoss(iTicker, -0.05)# -5% StopLoss
        
    def MD3_Series3(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.
            
        Returns
        -------
        None.
        
        Description:
        -----------
        The strategy appears to have several components, including a simple moving average (SMA) crossover and a multi-condition filter based on returns of close, high and low prices.
        The first thing the function does is check if a 'Strategy' attribute already exists, and if it does, the function labels the current time period with the string 'Series3' for all the tickers in the input list.
        Then, it calculates a 100-day simple moving average (SMA) for all the tickers in the list, and assigns a position of 1 (long) or -1 (short) depending on whether the current close price is above or below the 100-day SMA, respectively.
        Next, the function filters the tickers from the input list based on whether the tickers meet specific conditions related to the returns of the close, high and low prices for the last three time periods. The first set of conditions looks for tickers whose returns for close, high and low prices for the last three time periods are all greater than zero. The second set of conditions looks for tickers whose returns for close, high and low prices for the last three time periods are all less than zero.
        If a ticker meets the conditions from either the first or the second set of conditions, the function assigns a position of 1 to the 'DirectionPosition' attribute for that ticker.
        Finally, it looks for tickers that meet two more conditions, the first is that it is not in the current position and second, it meets the condition from the third step. For those, the function assigns the position (1 or -1) according to the SMA100Position in the SignalTicker set.
        For the remaining tickers which do not meet any of the above conditions, the function holds the position from the last period and applies a stop loss of -3% and target of 10% if the ticker is still in the position.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'Series3'
        SMA = self.BackTestData.SMA100.loc[self.CurrentTime, tickersList]
        Price = self.Close.loc[self.CurrentTime, tickersList]
        self.SMA100Position.loc[self.CurrentTime, Price[Price > SMA].dropna().index] = 1 #price is above SMA
        self.SMA100Position.loc[self.CurrentTime, Price[Price < SMA].dropna().index] = -1 #price is below SMA
                
        c11 = Price[(self.RetsHigh.loc[:self.CurrentTime, tickersList][-3:] >0).sum(axis = 0) == 3].dropna().index
        c12 = Price[(self.RetsLow.loc[:self.CurrentTime, tickersList][-3:] >0).sum(axis = 0) == 3].dropna().index
        c13 = Price[(self.RetsClose.loc[:self.CurrentTime, tickersList][-3:] >0).sum(axis = 0) == 3].dropna().index
        Cond1Tickers = set.intersection(set(c11), set(c12), set(c13))# stocks list meeting all 3 conditions
        
        c21 = Price[(self.RetsHigh.loc[:self.CurrentTime, tickersList][-3:] <0).sum(axis = 0) == 3].dropna().index
        c22 = Price[(self.RetsLow.loc[:self.CurrentTime, tickersList][-3:] <0).sum(axis = 0) == 3].dropna().index
        c23 = Price[(self.RetsClose.loc[:self.CurrentTime, tickersList][-3:] <0).sum(axis = 0) == 3].dropna().index
        Cond2Tickers = set.intersection(set(c21), set(c22), set(c23))
        self.DirectionPosition.loc[self.CurrentTime, set.union(Cond1Tickers, Cond2Tickers)] = 1
        
        SignalTickers = set.intersection(set(Price[self.PositionWOSL.loc[self.LastTime, tickersList] == 0].dropna().index), set(Price[self.DirectionPosition.loc[self.CurrentTime, tickersList] ==1].dropna().index))
        self.PositionWOSL.loc[self.CurrentTime, SignalTickers] = self.SMA100Position.loc[self.CurrentTime, SignalTickers]
        RemainTickers = tickersList.difference(SignalTickers)
        if len(RemainTickers) > 0:
            self.PositionWOSL.loc[self.CurrentTime, RemainTickers] = self.PositionWOSL.loc[self.LastTime, RemainTickers]
            for iTicker in list(RemainTickers):
                #self.ticker = iTicker
                TradeTakenDate = self.DetectPostionStartDate(iTicker, self.PositionWOSL)
                tradingDays = len(self.Close.loc[TradeTakenDate:self.CurrentTime, iTicker].dropna().index)
                if tradingDays > 15 or self.SMA100Position.loc[self.CurrentTime, iTicker] != self.PositionWOSL.loc[self.LastTime, iTicker]:
                     self.PositionWOSL.loc[self.CurrentTime, iTicker] = 0
                # else:
                #     self.PositionWOSL.loc[self.CurrentTime, iTicker] = self.PositionWOSL.loc[self.LastTime, iTicker]
                if self.PositionWOSL.loc[self.CurrentTime, iTicker] in [-1, 1]:
                      self.StopLoss(iTicker, -0.03)# -3% StopLoss
                      self.Target(iTicker, 0.10)# +10% Target
    
    def MD4_20WMA_MACD(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        -----------
        The function first checks if a 'Strategy' attribute already exists and if it does, it labels the current time period with the string '20WMA_MACD' for all the tickers in the input list.
        Then it gets the close price of all the tickers in the list and assigns positions of 1 (long) or -1 (short) or 0 (do nothing) based on the intersection of two signals:
        The first signal is the difference between the 26-day and 12-day exponential moving averages of the closing prices, the MACDDiff, which should be greater than 0 to be a buy signal and less than 0 to be a sell signal.
        The second signal is the whether the current close price is above or below the 20-day weighted moving average (WMA), which is calculated by the attribute 'PriceAboveWMA' . This should be greater than 0 to be a buy signal and less than 0 to be a sell signal.
        Finally, the function assigns the position (1, -1, or 0) based on the intersection of the above-mentioned signals to 'PositionWOSL' attribute for all the tickers.
        In summary this model is a combination of two moving averages , 20 WMA and MACD, which uses the intersection of two signals to determine the position.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = '20WMA_MACD'
        Price = self.Close.loc[self.CurrentTime, tickersList]        
        PosSignal = set.intersection(set(Price[self.MACDDiff.loc[self.CurrentTime, tickersList] > 0].dropna().index), set(Price[self.PriceAboveWMA.loc[self.CurrentTime, tickersList] > 0].dropna().index))
        NegSignal = set.intersection(set(Price[self.MACDDiff.loc[self.CurrentTime, tickersList] < 0].dropna().index), set(Price[self.PriceAboveWMA.loc[self.CurrentTime, tickersList] < 0].dropna().index))
        self.PositionWOSL.loc[self.CurrentTime, PosSignal] = 1
        self.PositionWOSL.loc[self.CurrentTime, NegSignal] = -1
        self.PositionWOSL.loc[self.CurrentTime, tickersList.difference(PosSignal, NegSignal)] = 0
    
    def MD5_BodyOutSideBand(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        -----------
        The function first checks if a 'Strategy' attribute already exists and if it does, it labels the current time period with the string 'BodyOutSideBand' for all the tickers in the input list.
        Then it calculates the close price, high price, and low price of all the tickers in the list and assigns a position of 1 (long) or -1 (short) to the attribute 'TradePosition' for the tickers that meets the following conditions:
        If a ticker's low price is greater than 1.01 times the 21-day exponential moving average of closing prices (EMA21) of last time, a long position is taken
        If a ticker's high price is less than 0.99 times the 21-day exponential moving average of closing prices (EMA21) of last time, a short position is taken.
        Next, the function checks for any newly generated position signal, where the last time signal was neutral and current time signal is not neutral. If it finds any such tickers, it assigns the current position signal to the attribute 'PositionWOSL'
        The function then checks if the current high price or low price has crossed the band (1.01EMA or 0.99EMA respectively), if it finds any such tickers, it exits from the positions (assigns position 0)
        Finally, for the tickers that are still in the position, it assigns a stop loss of -3% and a target of 10% and uses the helper functions StopLoss() and Target() to do so.
        In summary, this model uses two moving averages, 21-day EMA and high, low price of the current tickers. The function checks whether the high and low price have broken the band of 1.01 and 0.99 times the 21-day EMA and assigns the position accordingly, and it also includes an exit mechanism once the prices cross the band.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'BodyOutSideBand'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        High = self.BackTestData.High.loc[self.CurrentTime, tickersList]
        Low = self.BackTestData.Low.loc[self.CurrentTime, tickersList]
        EMA = self.BackTestData.EMA21.loc[self.LastTime, tickersList]
        self.TradePosition.loc[self.CurrentTime, Price[Low > 1.01*EMA].dropna().index] = 1
        self.TradePosition.loc[self.CurrentTime, Price[High < 0.99*EMA].dropna().index] = -1
        # If Current TradeSignal is not Neutral and Last TradeSignal is Neutral, then Signal as Current TradeSignal
        # if High or Low Crosses the band then Close the Position
        # therwise hits StopLoss - -2%, or  target 6%, then Exit Position
        LastNeutralCurPos = set.intersection(set(Price[self.TradePosition.loc[self.LastTime, tickersList] == 0].dropna().index), set(Price[self.TradePosition.loc[self.CurrentTime, tickersList] != 0].dropna().index))
        if len(LastNeutralCurPos)>0:
            self.PositionWOSL.loc[self.CurrentTime, LastNeutralCurPos] = self.TradePosition.loc[self.CurrentTime, LastNeutralCurPos]#.values
        
        LastPos = Price[self.PositionWOSL.loc[self.LastTime, tickersList] == 1].dropna().index
        exitLong = Price.loc[LastPos][High.loc[LastPos] < 1.01*EMA.loc[LastPos]].dropna().index
        #exitLong = Price.loc[LastPos][self.BackTestData.High.loc[self.CurrentTime, LastPos] < 1.01*self.BackTestData.EMA21.loc[self.LastTime, LastPos]].dropna().index
        LastNeg = Price[self.PositionWOSL.loc[self.LastTime, tickersList] == -1].dropna().index
        exitShort = Price.loc[LastNeg][Low.loc[LastNeg] > 0.99*EMA.loc[LastNeg]].dropna().index
        self.PositionWOSL.loc[self.CurrentTime, exitLong.union(exitShort)] = 0# Exited both Long and Short which goies outside band Range
        self.PositionWOSL.loc[self.CurrentTime, LastPos.difference(exitLong).union(LastNeg.difference(exitShort))] = self.PositionWOSL.loc[self.LastTime, LastPos.difference(exitLong).union(LastNeg.difference(exitShort))]# Carry same Position if Band is not Broken
        #self.PositionWOSL.loc[self.CurrentTime, LastNeutralCurPos.difference(LastPos, LastNeg)] = self.PositionWOSL.loc[self.LastTime, LastNeutralCurPos.difference(LastPos, LastNeg)]# Carry Same Position for which No new signal is generated
        for iTicker in tickersList:
            if self.PositionWOSL.loc[self.CurrentTime, iTicker] in [-1, 1]:
                  self.StopLoss(iTicker, -0.03)# -3% StopLoss
                  self.Target(iTicker, 0.10)# +10% Target
            
    def MD6_ROCMA(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description:
        -----------   
        The function first checks if a 'Strategy' attribute already exists and if it does, it labels the current time period with the string 'ROCMA' for all the tickers in the input list.
        Then it calculates the close price of all the tickers in the list, and it assigns positions of 1 (long) or -1 (short) based on the following conditions:
        It calculates the 27-day rate of change of closing prices (ROC27D) and 18-day rate of change of moving averages (ROCMA18D) and looks for tickers whose ROC27D is greater than ROCMA18D, and assigns a position of 1 to those tickers.
        It looks for tickers whose ROC27D is less than ROCMA18D, and assigns a position of -1 to those tickers.
        Finally, it assigns the position (1, -1) based on the above-mentioned conditions to 'PositionWOSL' attribute for all the tickers.
        In summary, this model uses two rate of change indicators , 27-day rate of change of closing prices and 18-day rate of change of moving averages, which uses the comparison of the two indicators to determine the position.
        '''
        # Defining Strategy
        #pdb.set_trace()
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'ROCMA'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        PosSignal = Price[self.BackTestData.ROC27D.loc[self.CurrentTime, tickersList] > self.BackTestData.ROCMA18D.loc[self.CurrentTime, tickersList]].dropna().index
        NegSignal = Price[self.BackTestData.ROC27D.loc[self.CurrentTime, tickersList] <= self.BackTestData.ROCMA18D.loc[self.CurrentTime, tickersList]].dropna().index
        self.PositionWOSL.loc[self.CurrentTime, PosSignal] = 1
        self.PositionWOSL.loc[self.CurrentTime, NegSignal] = -1
    
    def MD7_RegressionCrossOver(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        -----------
        The function first checks if a 'Strategy' attribute already exists and if it does, it labels the current time period with the string 'RegressionCrossOver' for all the tickers in the input list.
        Then it calculates the close price of all the tickers in the list and 10-day simple moving average(SMA10), least square(LS), least square regression(LR) of the close prices. It assigns positions of 1 (long) or -1 (short) based on the following conditions:
        It looks for tickers whose close price is greater than SMA10 and LS is greater than LR, and assigns a position of 1 to those tickers.
        It looks for tickers whose close price is less than SMA10 and LS is less than LR, and assigns a position of -1 to those tickers.
        For the tickers which were in the previous time period, if it's position has flipped (From Long to Short or vice versa) because of the condition above, it exits the trade.
        It assigns the position (1, -1) based on the above-mentioned conditions to 'PositionWOSL' attribute for all the tickers.
        In summary, this model uses one moving average indicator, 10-day simple moving average(SMA10), and two linear regression indicators, least square(LS) and least square regression(LR) and compares these indicators to determine the position.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'RegressionCrossOver'
        # Positive if above SMA and above LR
        # Negative if below SMA and below LR
        # Previous Positive and Current below SMA or below LR then Exit
        # Previous Negative and Current above SMA or above LR then Exit
        Price = self.Close.loc[self.CurrentTime, tickersList]
        SMA = self.BackTestData.SMA10.loc[self.CurrentTime, tickersList]
        LS = self.BackTestData.LS.loc[self.CurrentTime, tickersList]
        LR = self.BackTestData.LR.loc[self.CurrentTime, tickersList]
        aboveSMA = Price[Price > SMA].dropna().index
        belowSMA = Price[Price < SMA].dropna().index
        aboveLR = Price[LS>LR].dropna().index
        belowLR = Price[LS<LR].dropna().index
        Positive = aboveSMA.intersection(aboveLR)
        Negative = belowSMA.intersection(belowLR)        
        
        PrevPos = Price[self.PositionWOSL.loc[self.LastTime, tickersList] == 1].dropna().index
        PrevNeg = Price[self.PositionWOSL.loc[self.LastTime, tickersList] == -1].dropna().index
        exitPos = PrevPos.intersection(belowSMA.union(belowLR))
        exitNeg = PrevNeg.intersection(aboveSMA.union(aboveLR))
        self.PositionWOSL.loc[self.CurrentTime, exitPos.union(exitNeg)] = 0
        self.PositionWOSL.loc[self.CurrentTime, PrevPos.difference(exitPos)] = self.PositionWOSL.loc[self.LastTime, PrevPos.difference(exitPos)]
        self.PositionWOSL.loc[self.CurrentTime, PrevNeg.difference(exitNeg)] = self.PositionWOSL.loc[self.LastTime, PrevNeg.difference(exitNeg)]
        
        self.PositionWOSL.loc[self.CurrentTime, Positive] = 1
        self.PositionWOSL.loc[self.CurrentTime, Negative] = -1
     
    
    def MD8_Vortex(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        -----------
        The function first checks if a 'Strategy' attribute already exists and if it does, it labels the current time period with the string 'Vortex' for all the tickers in the input list.
        Then it calculates the close price of all the tickers in the list and vortex indicator ratios ('PviNviRatio'). It assigns positions of 1 (long) or -1 (short) based on the following conditions:
        It looks for tickers whose vortex ratio is greater than 0 and assigns a position of 1 to those tickers.
        It looks for tickers whose vortex ratio is less than 0 and assigns a position of -1 to those tickers.
        It assigns the position (1, -1) based on the above-mentioned conditions to 'PositionWOSL' attribute for all the tickers.
        In summary, this model uses the vortex indicator, which is a momentum indicator that helps identify if a security is trending or not and uses that information to determine the position.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'Vortex'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Ratio = self.PviNviRatio.loc[self.CurrentTime, tickersList]
        self.PositionWOSL.loc[self.CurrentTime, Price[Ratio>0].dropna().index] = 1
        self.PositionWOSL.loc[self.CurrentTime, Price[Ratio<0].dropna().index] = -1
        
    def MD9_Oscillator(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        -----------        
        This code defines a function called "MD9_Oscillator" that appears to be implementing a trading strategy using an indicator called the "ATR Ratio". The function takes in a list of tickers (stock symbols) as input, and uses that list to select a subset of rows from the DataFrame objects stored in the class that this function is a method of.
        The function first checks if a DataFrame called 'Strategy' exists and if it does, it sets the strategy for the current time and tickers to "Oscillator".
        The ATR Ratio is then used to identify trading signals, with values less than -1 indicating a short signal and values greater than 1 indicating a long signal. Then the function will exit any existing long position if the ATR ratio is less than 0, and exit any existing short position if the ATR ratio is greater than 0. Finally the function sets the position to -1 for short signal, 1 for long signal, 0 for any exit long or exit short, and carries the position forward for any tickers that do not meet any of the conditions.

        '''
        '''Oscillator'''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'Oscillator'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        ShortSignal = Price[self.ATRRatio.loc[self.CurrentTime, tickersList] < -1].dropna().index
        LongSignal = Price[self.ATRRatio.loc[self.CurrentTime, tickersList] > 1].dropna().index
        ExitLong = set.intersection(set(Price[self.PositionWOSL.loc[self.LastTime, tickersList] == 1].dropna().index), set(Price[self.ATRRatio.loc[self.CurrentTime, tickersList] < 0].dropna().index))
        ExitShort = set.intersection(set(Price[self.PositionWOSL.loc[self.LastTime, tickersList] == -1].dropna().index), set(Price[self.ATRRatio.loc[self.CurrentTime, tickersList] > 0].dropna().index))
        self.PositionWOSL.loc[self.CurrentTime, ShortSignal] = -1
        self.PositionWOSL.loc[self.CurrentTime, LongSignal] = 1
        self.PositionWOSL.loc[self.CurrentTime, ExitLong.union(ExitShort)] = 0        
        self.PositionWOSL.loc[self.CurrentTime, tickersList.difference(LongSignal, ShortSignal, ExitLong, ExitShort)] = self.PositionWOSL.loc[self.LastTime, tickersList.difference(LongSignal, ShortSignal, ExitLong, ExitShort)]
    
    def MD10_RSI50(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        Description
        -----------
        The function MD10_RSI50 is a function that applies the RSI50 strategy to a list of tickers. This strategy involves using the 14-day relative strength index (RSI) to generate trading signals. The strategy is based on the assumption that if the RSI is above 50, it's considered overbought and it might indicate to sell and if the RSI is below 50, it's considered oversold and might indicate to buy.
        The function first checks if the attribute 'Strategy' exists in the class, and if it does, it assigns the strategy name 'RSI50' to the current time and tickers in the list. Then it extracts the closing prices of the tickers at the current time and the 14-day RSI of the tickers at the current time. Using these, it checks for the RSI value of each ticker if it is above 50 or below 50. If it is above 50, it assigns 1 to the 'PositionWOSL' attribute for the ticker, indicating a long position, and if it is below 50, it assigns -1 to the 'PositionWOSL' attribute for the ticker, indicating a short position. This operation is done for all the tickers in the list.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'RSI50'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        RSI = self.BackTestData.RSI14.loc[self.CurrentTime, tickersList]
        self.PositionWOSL.loc[self.CurrentTime, Price[RSI>50].dropna().index] = 1
        self.PositionWOSL.loc[self.CurrentTime, Price[RSI<50].dropna().index] = -1
    
    def MD11_AssymetricDaily(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        ----------
        The function takes one argument tickersList, which is a list of tickers or securities to be evaluated by the strategy.
        The function begins by checking if the instance of the class has an attribute 'Strategy', and if so, it assigns the value 'AssymetricDaily' to the strategy for all tickers in the tickersList.
        Next, the function creates a variable Price that is a subset of the Close attribute of the class that is filtered by the date CurrentTime and the securities in the tickersList.
        Then, it creates two variables Long and Short that contain the tickers that have a positive and non-positive value of the CloseDiffMinSMA attribute of the class at the CurrentTime respectively.
        Finally, the function assigns a position of 1 to all the tickers in Long and -1 to all the tickers in Short in the attribute PositionWOSL at the CurrentTime. This indicates that the algorithm is taking a long position for securities in Long and a short position for securities in Short.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'AssymetricDaily'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Long = Price[self.CloseDiffMinSMA.loc[self.CurrentTime, tickersList] > 0].dropna().index
        Short = Price[self.CloseDiffMinSMA.loc[self.CurrentTime, tickersList] <= 0].dropna().index
        self.PositionWOSL.loc[self.CurrentTime, Long] = 1
        self.PositionWOSL.loc[self.CurrentTime, Short] = -1
    
    def MD11_AssymetricWeekly(self, tickersList):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        Description
        -----------
        The function first checks if the class has an attribute 'Strategy' and if it does, it assigns the strategy name 'AssymetricWeekly' to each ticker in the tickers list.
        Then the code creates two variables, "Long" and "Short" which are sets of tickers that are filtered using the conditions in the square brackets on the right side of the assignment operator. The Long variable is assigned all the tickers that satisfy the condition self.CloseDiffMinSMAWeekly.loc[:self.CurrentTime, tickersList].iloc[-1] > 0 and similarly, the Short variable is assigned all the tickers that satisfy the condition self.CloseDiffMinSMAWeekly.loc[:self.CurrentTime, tickersList].iloc[-1] <= 0
        The code then assigns a position of 1 to the tickers in the Long set and -1 to the tickers in the short set in the PositionWOSL attribute of the class. In the next steps, Some StopLoss or target should be added for managing the risk.
        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'AssymetricWeekly'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Long = Price[self.CloseDiffMinSMAWeekly.loc[:self.CurrentTime, tickersList].iloc[-1] > 0].dropna().index
        Short = Price[self.CloseDiffMinSMAWeekly.loc[:self.CurrentTime, tickersList].iloc[-1] <= 0].dropna().index
        self.PositionWOSL.loc[self.CurrentTime, Long] = 1
        self.PositionWOSL.loc[self.CurrentTime, Short] = -1

    
    def getSeasonalityActiveDaysList(self):
        #pdb.set_trace()
        self.ActiveDaysList = []
        holidays = dict({})
        holidays['2010'] = ['26-01-2010','12-02-2010','27-02-2010','01-03-2010','16-03-2010','24-03-2010','28-03-2010','01-04-2010','02-04-2010','14-04-2010','01-05-2010','27-05-2010','15-08-2010','19-08-2010','10-09-2010','11-09-2010','30-09-2010','02-10-2010','17-10-2010','05-11-2010','07-11-2010','17-11-2010','21-11-2010','17-12-2010','25-12-2010']
        holidays['2011'] = ['26-01-2011','02-03-2011','12-04-2011','14-04-2011','22-04-2011','15-08-2011','31-08-2011','01-09-2011','06-10-2011','26-10-2011','27-10-2011','07-11-2011','10-11-2011','06-12-2011']
        holidays['2012'] = ['26-01-2012','20-02-2012','08-03-2012','05-04-2012','06-04-2012','01-05-2012','15-08-2012','20-08-2012','19-09-2012','02-10-2012','24-10-2012','26-10-2012','13-11-2012','14-11-2012','28-11-2012','25-12-2012']
        holidays['2013'] = ['26-01-2013','10-03-2013','27-03-2013','29-03-2013','14-04-2013','19-04-2013','24-04-2013','01-05-2013','09-08-2013','15-08-2013','09-09-2013','02-10-2013','13-10-2013','16-10-2013','03-11-2013','04-11-2013','14-11-2013','17-11-2013','25-12-2013']
        holidays['2014'] = ['26-01-2013','27-02-2014','17-03-2014','08-04-2014','13-04-2014','14-04-2014','18-04-2014','24-04-2014','01-05-2014','29-07-2014','15-08-2014','29-08-2014','02-10-2014','03-10-2014','06-10-2014','15-10-2014','23-10-2014','24-10-2014','04-11-2014','06-11-2014','25-12-2014']
        holidays['2015'] = ['26-01-2015','17-02-2015','06-03-2015','28-03-2015','02-04-2015','03-04-2015','14-04-2015','01-05-2015','18-07-2015','15-08-2015','17-09-2015','25-09-2015','02-10-2015','22-10-2015','11-10-2015','12-11-2015','25-11-2015','25-12-2015']
        holidays['2016'] = ['26-01-2016','07-03-2016','24-03-2016','25-03-2016','14-04-2016','15-04-2016','19-04-2016','01-05-2016','06-07-2016','15-08-2016','05-09-2016','13-09-2016','02-10-2016','11-10-2016','12-10-2016','30-10-2016','31-10-2016','14-11-2016','25-12-2016']
        holidays['2017'] = ['26-01-2017','24-02-2017','13-03-2017','04-04-2017','09-04-2017','14-04-2017','01-05-2017','26-06-2017','15-08-2017','25-08-2017','02-09-2017','30-09-2017','01-10-2017','02-10-2017','19-10-2017','20-10-2017','04-11-2017','25-12-2017']
        holidays['2018'] = ['26-01-2018','13-02-2018','02-03-2018','25-03-2018','29-03-2018','30-03-2018','14-04-2018','01-05-2018','16-06-2018','15-08-2018','22-08-2018','13-09-2018','20-09-2013','02-10-2018','18-10-2018','07-11-2018','08-11-2018','23-11-2018','25-12-2018']
        holidays['2019'] = ['26-01-2019','04-03-2019','21-03-2019','13-04-2019','14-04-2019','17-04-2019','19-04-2019','29-04-2019','01-05-2019','05-06-2019','12-08-2019','15-08-2019','02-09-2019','10-09-2019','02-10-2019','08-10-2019','21-10-2019','27-10-2019','28-10-2019','12-11-2019','25-12-2019']
        holidays['2020'] = ['26-01-2020','21-02-2020','10-03-2020','02-04-2020','06-04-2020','10-04-2020','14-04-2020','01-05-2020','25-05-2020','01-08-2020','15-08-2020','22-08-2020','30-08-2020','02-10-2020','25-10-2020','14-11-2020','16-11-2020','30-11-2020','25-12-2020']
        holidays['2021'] = ['26-01-2021','11-03-2021','29-03-2021','02-04-2021','14-04-2021','21-04-2021','13-05-2021','21-07-2021','19-08-2021','10-09-2021','15-10-2021','04-11-2021','05-11-2021','19-11-2021']
        holidays['2022'] = ['26-01-2022','01-03-2022','18-03-2022','14-04-2022','15-04-2022','03-05-2022','09-08-2022','15-08-2022','31-08-2022','05-10-2022','24-10-2022','26-10-2022','08-11-2022']
        holidays['2023'] = ['26-01-2023','07-03-2023','30-03-2023','04-04-2023','07-04-2023','14-04-2023','01-05-2023','28-06-2023','15-08-2023','19-09-2023','02-10-2023','24-10-2023','14-11-2023','27-11-2023','25-12-2023']
        holidays['2024'] = ['26-01-2024', '08-03-2024', '25-03-2024', '29-03-2024', '11-04-2024', '17-04-2024', '01-05-2024', '17-06-2024','17-07-2024','15-08-2024','02-10-2024','01-11-2024','15-11-2024','25-12-2024']
        holidays['2025'] = ['26-02-2025', '14-03-2025', '31-03-2025', '10-04-2025', '14-04-2025', '18-04-2025', '01-05-2025', '15-08-2025', '27-08-2025', '02-10-2025', '21-10-2025', '22-10-2025', '05-11-2025', '25-12-2025']
        HolidayList = []
        for k,v in holidays.items():
            ok = [HolidayList.append(datetime.datetime.strptime(date, '%d-%m-%Y').date()) for date in holidays[k]]
            
        def getActiveEntryDate(indate):
            while indate in HolidayList or indate.weekday()>= 5:
                indate -= datetime.timedelta(days=1)
            return indate
        def getActiveExitDate(indate):
            nextDate  = indate + datetime.timedelta(1)
            while nextDate in HolidayList or nextDate.weekday()>= 5:
                nextDate -= datetime.timedelta(days=1)
            return nextDate - datetime.timedelta(1)
        EntryDates = [getActiveEntryDate(it) for it in getdatelist(self.FromTime -datetime.timedelta(30), self.EndTime + datetime.timedelta(30)) if it.day == 24]
        LastActiveDates = [getActiveExitDate(it) for it in getdatelist(self.FromTime, self.EndTime + datetime.timedelta(30)) if it.day == 3]        
        ok = [self.ActiveDaysList.extend(getdatelist(datetime.date(it.year, it.month, 1), it+datetime.timedelta(1))) for it in LastActiveDates if it.day < 12]
        ok = [self.ActiveDaysList.extend(getdatelist(it.date(),  datetime.date(it.year+1, 1, 1) if it.month == 12 else datetime.date(it.year, it.month+1, 1) )) for it in EntryDates if it.day > 15]
        
    def MD12_Seasoning_old(self, tickersList, sl = -0.05, target = 0.15):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        -----------
        The strategy is based on the day of the current time (self.CurrentTime), and whether it falls within a certain range (days 24 to 4). If the current day is within this range, the function sets the position (self.PositionWOSL) to be a long position for all the tickers in the list (tickersList). If the current day is not in this range, no positions are taken. Additionally, the function is also setting stop-loss and target values for all the tickers in the list. The stop loss is set to -5% and target is set to +15%. So it is an momentum strategy which enter the trade on 24th day and exit on 5th Day of the next month with -5% stop loss and +15% Target.

        '''
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'Seasoning'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Long = set(Price.dropna().index)        
        curTime = self.CurrentTime.date()
        # For better implementation, make the Active days list for that month andchek for the entry & Exit points via that List
        A_ = [24, 25, 26, 27, 28, 29, 30, 31, 1, 2, 3] # These are active Days
        B_ = range(4, 15)
        H_ = [5, 6]# Sat/Sun weekdays
        HolidayList = ['26-01-2022', '01-03-2022', '18-03-2022', '14-04-2022', '15-04-2022', '03-05-2022', '09-08-2022', '15-08-2022', '31-08-2022', '05-10-2022', '24-10-2022', '26-10-2022', '08-11-2022', '26-01-2023', '07-03-2023', '30-03-2023', '04-04-2023', '07-04-2023', '14-04-2023', '01-05-2023', '28-06-2023', '15-08-2023', '19-09-2023', '02-10-2023', '24-10-2023', '14-11-2023', '27-11-2023', '25-12-2023']
        HolidayList = [datetime.datetime.strptime(i, '%d-%m-%Y').date() for i in HolidayList]
        #condition1 = curTime.day in A_ or  ((curTime.day+1) in A_ and curTime.weekday()+1 in H_) or ((curTime.day+2) in A_ and curTime.weekday()+2 in H_ and curTime.weekday()+1 in H_)# Checking for if 24 is on weekend, then detect it in advance
        #condition2 = ((curTime.day+1) in B_ and curTime.weekday()+1 in H_) or ((curTime.day+2) in B_ and curTime.weekday()+2 in H_ and curTime.weekday()+1 in H_)# if 4 is on Weekend, then exit before that on friday itself
        #check 3 May 2022, 24 Oct 2022
        # Current is active or (Cur +1 is active & Cur +1 in weekend or Holidays)
       
        condition1 = curTime.day in A_ or  ((curTime.day+1) in A_ and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)) or ((curTime.day+2) in A_ and ((curTime.weekday()+2 in H_ or curTime + datetime.timedelta(2) in HolidayList) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList))) or ((curTime.day+3) in A_ and ((curTime.weekday()+3 in H_ or curTime + datetime.timedelta(3) in HolidayList) and (curTime.weekday()+2 in H_ or curTime + datetime.timedelta(2) in HolidayList) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)))# Checking for if 24 is on weekend, then detect it in advance
        condition2 = ((curTime.day+1) in B_ and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)) or ((curTime.day+2) in B_ and (( curTime.weekday()+2 in H_  or curTime + datetime.timedelta(2) in HolidayList)) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)) or ((curTime.day+3) in B_ and (( curTime.weekday()+3 in H_  or curTime + datetime.timedelta(3) in HolidayList)) and (curTime.weekday()+2 in H_ or curTime + datetime.timedelta(2) in HolidayList) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList))
        # if 4 is on Weekend, then exit before that on friday itself
        if condition1 and not condition2:
            self.PositionWOSL.loc[self.CurrentTime, Long] = 1
        #if self.CurrentTime.date() in [datetime.date(2023, 2, 1), datetime.date(2023, 2, 2), datetime.date(2023, 2, 3)]:
        #    self.PositionWOSL.loc[self.CurrentTime, Long] = 0            
        for iTicker in tickersList:
            self.StopLoss(iTicker, sl)# -3% StopLoss
            self.Target(iTicker, target)# +10% Target
            
    def MD12_Seasoning(self, tickersList, sl = -0.05, target = 0.15):
        '''
        Parameters
        ----------
        tickersList : TYPE
            DESCRIPTION.

        Returns
        -------
        None.
        
        Description
        -----------
        The strategy is based on the day of the current time (self.CurrentTime), and whether it falls within a certain range (days 24 to 4). If the current day is within this range, the function sets the position (self.PositionWOSL) to be a long position for all the tickers in the list (tickersList). If the current day is not in this range, no positions are taken. Additionally, the function is also setting stop-loss and target values for all the tickers in the list. The stop loss is set to -5% and target is set to +15%. So it is an momentum strategy which enter the trade on 24th day and exit on 5th Day of the next month with -5% stop loss and +15% Target.

        '''
        if not hasattr(self, 'ActiveDaysList'):
            self.getSeasonalityActiveDaysList()
        # Defining Strategy
        if hasattr(self, 'Strategy'):
            #for ticker in tickersList:
            self.Strategy.loc[self.CurrentTime, tickersList] = 'Seasoning'
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Long = set(Price.dropna().index)
        if self.CurrentTime.date() in self.ActiveDaysList:
            self.PositionWOSL.loc[self.CurrentTime, Long] = 1
        else:
            self.PositionWOSL.loc[self.CurrentTime, Long] = 0
            
        for iTicker in tickersList:
            self.StopLoss(iTicker, sl)# -3% StopLoss
            self.Target(iTicker, target)# +10% Target
            

            
    ## generate post simulation analytics
    def GetStatsReturns(self, Riskfree=0, BenchmarkIndex=[]):
        '''Generates Post Simulation Analytics'''
        Results=dict()
        Results['RiskfreeDaily'] = (1+Riskfree)**(1/365.0) - 1
        Results['RiskfreeWeekly'] = (1+Riskfree)**(1/52.0) - 1;
        Results['RiskfreeMonthly'] = (1+Riskfree)**(1/12.0) - 1;

        #write the same as below for daily also
        # while writing keep in mind to create a temp time series dataframe and then

        TempSeries = self.NAV.NAV

        Results['DayEndingVals'] = TempSeries
        Results['WeekEndingVals'] = TempSeries.asfreq('w',method='ffill')
        Results['MonthEndingVals'] = TempSeries.asfreq('m',method='ffill')


        Results['DailyReturns'] = Results['DayEndingVals'].pct_change().fillna(0)
        Results['WeeklyReturns'] = Results['WeekEndingVals'].pct_change().fillna(0)
        Results['MonthlyReturns'] = Results['MonthEndingVals'].pct_change().fillna(0)


        Results['AnnualisedDailyReturns']= (1+numpy.average(Results['DailyReturns'])) **365 -1
        Results['AnnualisedWeeklyReturns']= (1+numpy.average(Results['WeeklyReturns']))  **52 -1
        Results['AnnualisedMonthlyReturns']= (1+numpy.average(Results['MonthlyReturns'])) **12 -1

        # write few more lines here

        Results['StartDate'] = TempSeries.index[0]
        Results['EndDate'] = TempSeries.index[-1]

        Results['VolatilityFromDaily'] = numpy.std(Results['DailyReturns'])*numpy.sqrt(365)
        Results['VolatilityFromWeekly'] = numpy.std(Results['WeeklyReturns'])*numpy.sqrt(365.25/7)
        Results['VolatilityFromMonthly'] = numpy.std(Results['MonthlyReturns'])*numpy.sqrt(12)

        Results['SharpeRatioDaily'] = ((numpy.average(Results['DailyReturns']) - Results['RiskfreeDaily'])/numpy.std(Results['DailyReturns'])) * numpy.sqrt(365)
        Results['SharpeRatioWeekly'] = (numpy.average(Results['WeeklyReturns']) - Results['RiskfreeWeekly'])/numpy.std(Results['WeeklyReturns']) * numpy.sqrt(52)
        Results['SharpeRatioMonthly'] = (numpy.average(Results['MonthlyReturns']) - Results['RiskfreeMonthly'])/numpy.std(Results['MonthlyReturns']) * numpy.sqrt(12)


        Results['DailyAvg'] = numpy.average(Results['DailyReturns'])
        Results['DailyMin'] = numpy.min(Results['DailyReturns'])
        Results['DailyMax'] = numpy.max(Results['DailyReturns'])
        Results['DailyNegNo'] = len(Results['DailyReturns']<0)
        Results['DailyNegAvg'] = numpy.average(Results['DailyReturns'][Results['DailyReturns']<0])
        Results['DailyPosNo'] = len(Results['DailyReturns']>0)
        Results['DailyPosAvg'] = numpy.average(Results['DailyReturns'][Results['DailyReturns']>0])

        Results['WeeklyAvg'] = numpy.average(Results['WeeklyReturns'])
        Results['WeeklyMin'] = numpy.min(Results['WeeklyReturns'])
        Results['WeeklyMax'] = numpy.max(Results['WeeklyReturns'])
        Results['WeeklyNegNo'] = len(Results['WeeklyReturns']<0)
        Results['WeeklyNegAvg'] = numpy.average(Results['WeeklyReturns'][Results['WeeklyReturns']<0])
        Results['WeeklyPosNo'] = len(Results['WeeklyReturns']>0)
        Results['WeeklyPosAvg'] = numpy.average(Results['WeeklyReturns'][Results['WeeklyReturns']>0])


        Results['MonthlyAvg'] = numpy.average(Results['MonthlyReturns'])
        Results['MonthlyMin'] = numpy.min(Results['MonthlyReturns'])
        Results['MonthlyMax'] = numpy.max(Results['MonthlyReturns'])
        Results['MonthlyNegNo'] = len(Results['MonthlyReturns']<0)
        Results['MonthlyNegAvg'] = numpy.average(Results['MonthlyReturns'][Results['MonthlyReturns']<0])
        Results['MonthlyPosNo'] = len(Results['MonthlyReturns']>0)
        Results['MonthlyPosAvg'] = numpy.average(Results['MonthlyReturns'][Results['MonthlyReturns']>0])


    #    if not(BenchmarkIndex==[]):
    #        dc = corrcoefp(Results.DailyReturns, BenchmarkReturns)
    #        Results.DailyCorrelation = dc.corr
    #        wc = corrcoefp(Results.WeeklyReturns, BenchmarkReturnsWeekly)
    #        Results.WeeklyCorrelation = wc.corr
    #        mc = corrcoefp(Results.MonthlyReturns, BenchmarkReturnsMonthly)
    #        Results.MonthlyCorrelation = mc.corr
    #        qc = corrcoefp(Results.QuarterlyReturns, BenchmarkReturnsQuarterly)
    #        Results.QuarterlyCorrelation = qc.corr
    #
    #
    #        cdc = GetConditionalCorrelation(Results.DailyReturns, BenchmarkReturns)
    #        cwc = GetConditionalCorrelation(Results.WeeklyReturns, BenchmarkReturnsWeekly)
    #        cmc = GetConditionalCorrelation(Results.MonthlyReturns, BenchmarkReturnsMonthly)
    #        cqc = GetConditionalCorrelation(Results.QuarterlyReturns, BenchmarkReturnsQuarterly)
    #
    #
    #        Results.PropnUpWhenUpDaily = clean(length(find(Results.DailyReturns>0 & BenchmarkReturns>0))./length(find(BenchmarkReturns>0)))
    #        Results.PropnUpWhenDownDaily = clean(length(find(Results.DailyReturns>0 & BenchmarkReturns<0))./length(find(BenchmarkReturns<0)))
    #
    #
    #        Results.AmountUpWhenUpDaily = clean(mean(Results.DailyReturns(BenchmarkReturns>=0)))
    #        Results.AmountUpWhenDownDaily = clean(mean(Results.DailyReturns(BenchmarkReturns<0)))
    #
    #        out = GetDrawdownStats(Dates,  FundValues)
    #        Results.MaxDrawdown =out.MaxDrawdown
    #        Results.MaxDrawdownDate = out.MaxDrawdownDate
    #        Results.MaxDrawdownDaysToRecover = out.MaxDrawdownDaysToRecover
    #        Results.AllDrawdowns = out.AllDrawdowns
    #
    #        out = GetDrawdownStats(Dates,  BenchmarkIndex)
    #        Results.MaxDrawdownBenchmark =out.MaxDrawdown
    #        Results.MaxDrawdownDateBenchmark = out.MaxDrawdownDate
    #        Results.MaxDrawdownDaysToRecoverBenchmark = out.MaxDrawdownDaysToRecover
    #        Results.AllDrawdownsBenchmark = out.AllDrawdowns
    #
    #        Results.MARRatio = Results.AnnualizedReturnPerTrac./Results.MaxDrawdown
    #
        return Results

    def basicdatainitialize(self):
        #within self.BackTestData
        pass

    def declarecurrentvariables(self):
        pass


    def detectupdatedate(self):
        pass
        #should return a bool val

    def UpdateSpecificStats(self):
        pass

    def updateCapAllocation(self):
        pass

    def StopLossHandler(self):
        pass
    
    def HedgePositions(self):
        pass