# -*- coding: utf-8 -*-
"""
Created on Fri Nov 11 11:03:58 2022

@author: Viren@Incred
DailyTrading Signals Combined Model:
It is combination of 12 Models
1	RSI2+ 50DMA Trend
2	RSI2+ 50SMA
3	Series3
4	20WMA_MACD
5	Body OutSide Band
6	ROCMA
7	Regression CrossOver
8	Vortex
9	Oscillator
10	RSI50
11	Assymetric Weekly
12	Seasoning with Nifty Index Ratio

Model 1: RSI2+ 50DMA Trend
    1. Close Price > 50SMA => 1, Close Price < 50SMA => -1
    2. If Last RSI Position =1 and Last Date RSI > 90 and Current RSI < 90 => Current RSI Position -1, otherwise same position as last
    if Last RSI Position -1, and (Last Date RSI < 10 and Current RSI > 10) => Current RSI Position 1, otherwise same position as Last    
    Final-> Long if both Long, Short if both short, otherwise neutral

    (Summary- if RSI changing from below 10 to above 10 and Price > 50SMA -> Long
     If RSI changing from above 90 to below 90 and Price < 50SMA -> Short
     IF RSI is coming up from below 10 then Positive (+1) RSI Signal, till it reverts from above 90 to below 90 and Price > 50SMA -> +1
     IF RSI is coming down from above 90 then Negative(-1) RSI Signal, till it reverts from below 10 to above 10 and Price < 50SMA -> -1)
    
Model 2: RSI2+ 50SMA
    1. Close Price > 50SMA => 1, Close Price < 50SMA => -1
    2. IF 2RSI> 90, -1, else if 2RSI < 10, 1
    
    Based on both positions:
        if both signal gives same positions then same position (either Long or Short)
        otherwise-> IF Previous Position is not same as Current SMA Position or (Current RSI Position is not neutral and Last Position is different than Current RSI Position) then Neutral
        Otherwise continure same as previous position

    Entry: When both Gives same signal,
    Exit-> when either SMA gives opposite signal or RSISignal gives opposite trade signal

Model 3: Series3
    1. Pattern-> Find out if there is pattern, If High, Low, Close is increasing or decreasing in same direction in past 4 days, then there is directional pattern -> 1
    2. if Close > 100 SMA, then +1, otherewise -1
    
    Final Position is->
    If Previous Position is 0, and Current Pattern is 1, then Same as signal provided by Current 100SMA
    Otherwise break if 1. Trading Days are more than 15, 2. Profit from entry date is more than 10%, 3. Loss is More than 3%, 4. SMA Signal has been changed
    Otherwise continue for the previous position

Model 4: 20WMA_MACD
    if Close > 20WMA and MACD is above SignalLine then Long
    Otherwise if Close < 20WMA and MACD is Below SignalLine then Short 

Model 5: Body OutSide Band
    20EMA, Lower Band (99% of EMA), Upper Band (101% of EMA)    
    1. TradeSignal: If High > Upper band -> 1, If Lower < Lower Band -> -1
    
    If Current TradeSignal is not Neutral and Last TradeSignal is Neutral, then Signal as Current TradeSignal
    if High or Low Crosses the band then Close the Position
    otherwise hits StopLoss - -3%, or  target 10%, then Exit Position
    # From backtests Observation Looks like, should place stoploss of -3% and Target 10%
    
Model 6: ROCMA
    27Days ROC is > 18 Days ROC MA -> +1 Signal Other Wise -1 Signal

Model 7: Regression CrossOver
    10 Days SMA, 
    5Days Linear Regression Slope of Close Prices
    Slope Forecast from last 50 Days slopes
    
    1. if :
    Close > 10 SMA and 5 Days Slope > Slope Estimate from last 50Days data  then Long
    else if Close < 10SMA and 5Days Slope < Slope Estimate from last 50Days data then Short
    
    2. If Last Position is Long:
        if Close < 10SMA or 5Days Slope < 50Days Slope -> neutral
        otherwise continute as last
    3. if Last Position is Short:
        if Close > 10SMA or 5 Days Slope > 50 Days Slope -> Neutral
        Otherwise continue as last
    

Model 8: Vortex
    Calculates the difference between PVI and NVI, based on this difference if +ve then Long otherwise Short
    TR = MovingAverage(GetTrueRange(Data),Days)
    PVM = MovingAverage(numpy.abs(Data.High - Data.Low.shift(1).fillna(0)),Days)
    NVM = MovingAverage(numpy.abs(Data.Low - Data.High.shift(1).fillna(0)),Days)
    PVI = PVM / TR
    NVI = NVM / TR

Model 9: Oscillator
    Calculates the ratio of Difference between (Close and 24 Days SMA) and EMA of 24 Days True Range (ATR)
    1. if this Current Ratio is greater than 1 then Long if this Current Ratio is less than -1 then Short
    2. If Previous position is Long and Current Ratio goes to negative then exit Long position or Previous Position is Short and Current Ratio goes to Positive then close the Short position

Model 10: RSI_50
    if RSI(14) > 50 then Long otherwise Short
    
Model 11: Assymetric Weekly
    if Close < min(20SMA, 10SMA) then Short otherwise LONG

Model 12: Seasoning with Nifty Index Ratio
    Long in between the dates 25 and 4th of every month
    with -3% stop Loss and 10% target Gain
    
Every Security is mapped to a model, the model generates the Long or Short Signal for all securities
The combined Net exposure is hedged with Nifty Futs
"""
import time
starttime = time.time()
import warnings
warnings.filterwarnings("ignore")
try:
    import Bloomberg as bbg
except:
    pass
import pandas
import datetime
import numpy
#import re

#import sqlite3
#import pandas
#from GetData import GetConn, QueryFutTickers
from GetData import *#GetComponentsForIndexForDateRange, GetDataForIndicesFromBloomDB, GetDataForFutTickersFromBloomDB, GetConn, QueryFutTickers
import MyTechnicalLib

import pickle
#from dateutil.relativedelta import relativedelta


from FactoryBackTester import FactoryBackTester
import pdb
import matplotlib.pyplot as plt
import copy
from random import choice

from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter

global MODELSELECTOR
global ColumnsDict
global allFutTickers

today = datetime.date.today()
startday = today -datetime.timedelta(545)# 1.5 Yrs Data

modelfile = 'G:/Shared drives/Quant/Trades/Logs/ModelSelector.xlsx'
modelTable = pandas.read_excel(modelfile, sheet_name = 'MomentumModel', header = 0, index_col= 2)
allFutTickers = list(modelTable.index)

MODELSELECTOR = {}
gg = modelTable.groupby('Model')
for grp in gg.groups.keys():
    MODELSELECTOR[grp] = list(gg.get_group(grp).index)
ColumnsDict = {'LAST_PRICE' : 'Close', 'PX_HIGH' : 'High', 'PX_LOW' : 'Low', 'PX_OPEN': 'Open', 'FUT_CONT_SIZE' : 'LotSize'}



class DailyTradingSignals(FactoryBackTester):
    def __init__(self,data, modelTable):
        FactoryBackTester.__init__(self, data)        
        self.modelTable = modelTable
        
    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2022-06-30')# '2013-01-07'
        self.IndexName = 'NZ1 Index'
        self.BackTestData.LotSize[self.IndexName] = 50
        self.TradedValue = 50000000# 5Cr
        self.TransactionCostRate = 0.0002# Total 2 bps Transaction Charges        
        self.order = Order()
        self.StopLossLimit = -0.05# Stop loss
        self.TargetLimit = 0.15
        self.UpdateDates = list(self.Close.index)#list(self.BackTestData.Close.index)#.values()
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
        self.PviNviRatio = numpy.subtract(self.BackTestData.PVI, self.BackTestData.NVI)
        self.trade_reg = TradeRegister()
            
    def declarecurrentvariables(self):
        self.LastPosition=self.Position.loc[self.LastTime]
        self.CurrentNAV=self.NAV.loc[self.CurrentTime,'NAV']
        self.CurrentPrice=self.Close.loc[self.CurrentTime].dropna()
        
    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True

    def DetectPostionStartDate(self, ticker, positionMat):
        '''positionMat: self.Position/self.PositionWOSL'''
        TradeTakenDate = self.CurrentTime
        tempList = positionMat.loc[:self.CurrentTime, ticker]
        tempList = tempList.sort_index(ascending = False)
        iDate = tempList.index[0]
        for iTime in list(tempList.index[1:90]):
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

    def MD1_RSIwith50SMATrend(self, tickersList):
        '''Model 1: RSI+ 50SMA Trend'''        
        if not hasattr(self, 'StartingPosition'):
            self.StartingPosition = {}
            for ticker in tickersList:
                self.StartingPosition[ticker] = -1
                
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
            self.PositionWOSL.loc[self.CurrentTime, SignalTickers] = self.SMA50Position.loc[self.CurrentTime, SignalTickers].values[0]
        self.PositionWOSL.loc[self.CurrentTime, tickersList.difference(SignalTickers)] = 0

    def MD2_RSI50SMA(self, tickersList):
        '''Model 2: RSI + 50SMA'''
        RSI = self.BackTestData.RSI2.loc[:, tickersList]
        SMA = self.BackTestData.SMA50.loc[self.CurrentTime, tickersList]
        Price = self.Close.loc[self.CurrentTime, tickersList]
        self.SMA50Position.loc[self.CurrentTime, Price[Price > SMA].dropna().index] = 1 #price is above SMA
        self.SMA50Position.loc[self.CurrentTime, Price[Price < SMA].dropna().index] = -1 #price is below SMA        
        
        self.RSI2Position.loc[self.CurrentTime, Price[RSI.loc[self.CurrentTime] < 10].dropna().index] = 1
        self.RSI2Position.loc[self.CurrentTime, Price[RSI.loc[self.CurrentTime] > 90].dropna().index] = -1
        
        SignalTickers = Price[self.RSI2Position.loc[self.CurrentTime, tickersList] == self.SMA50Position.loc[self.CurrentTime, tickersList]].dropna().index
        if len(SignalTickers)>0:
            self.PositionWOSL.loc[self.CurrentTime, SignalTickers] = self.RSI2Position.loc[self.CurrentTime, SignalTickers].values[0]
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

    def MD3_Series3(self, tickersList):
        '''Model 3: Series 3'''
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
        '''Model 4: 20WMA_MACD'''
        Price = self.Close.loc[self.CurrentTime, tickersList]        
        PosSignal = set.intersection(set(Price[self.MACDDiff.loc[self.CurrentTime, tickersList] > 0].dropna().index), set(Price[self.PriceAboveWMA.loc[self.CurrentTime, tickersList] > 0].dropna().index))
        NegSignal = set.intersection(set(Price[self.MACDDiff.loc[self.CurrentTime, tickersList] < 0].dropna().index), set(Price[self.PriceAboveWMA.loc[self.CurrentTime, tickersList] < 0].dropna().index))
        self.PositionWOSL.loc[self.CurrentTime, PosSignal] = 1
        self.PositionWOSL.loc[self.CurrentTime, NegSignal] = -1
        self.PositionWOSL.loc[self.CurrentTime, tickersList.difference(PosSignal, NegSignal)] = 0
    
    def MD5_BodyOutSideBand(self, tickersList):
        '''Body OutSide Band'''
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
        self.PositionWOSL.loc[self.CurrentTime, LastPos.difference(exitLong).union(LastNeg.difference(exitShort))] = self.PositionWOSL.loc[self.LastTime, LastPos.difference(exitLong).union(LastNeg.difference(exitShort))]# Carry same Posiiton if Band is not Broken
        #self.PositionWOSL.loc[self.CurrentTime, LastNeutralCurPos.difference(LastPos, LastNeg)] = self.PositionWOSL.loc[self.LastTime, LastNeutralCurPos.difference(LastPos, LastNeg)]# Carry Same Position for which No new signal is generated
        for iTicker in tickersList:
            if self.PositionWOSL.loc[self.CurrentTime, iTicker] in [-1, 1]:
                  self.StopLoss(iTicker, -0.03)# -3% StopLoss
                  self.Target(iTicker, 0.10)# +10% Target
            
    def MD6_ROCMA(self, tickersList):
        '''ROC MA'''
        Price = self.Close.loc[self.CurrentTime, tickersList]
        PosSignal = Price[self.BackTestData.ROC27D.loc[self.CurrentTime, tickersList] > self.BackTestData.ROCMA18D.loc[self.CurrentTime, tickersList]].dropna().index
        NegSignal = Price[self.BackTestData.ROC27D.loc[self.CurrentTime, tickersList] < self.BackTestData.ROCMA18D.loc[self.CurrentTime, tickersList]].dropna().index
        self.PositionWOSL.loc[self.CurrentTime, PosSignal] = 1
        self.PositionWOSL.loc[self.CurrentTime, NegSignal] = -1
    
    def MD7_RegressionCrossOver(self, tickersList):
        '''Regression CrossOver'''
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
        self.PositionWOSL.loc[self.CurrentTime, Positive] = 1
        self.PositionWOSL.loc[self.CurrentTime, Negative] = -1
        
        PrevPos = Price[self.PositionWOSL.loc[self.LastTime, tickersList] == 1].dropna().index
        PrevNeg = Price[self.PositionWOSL.loc[self.LastTime, tickersList] == -1].dropna().index
        exitPos = PrevPos.intersection(belowSMA.union(belowLR))
        exitNeg = PrevNeg.intersection(aboveSMA.union(aboveLR))
        self.PositionWOSL.loc[self.CurrentTime, exitPos.union(exitNeg)] = 0
        self.PositionWOSL.loc[self.CurrentTime, PrevPos.difference(exitPos)] = self.PositionWOSL.loc[self.LastTime, PrevPos.difference(exitPos)]
        self.PositionWOSL.loc[self.CurrentTime, PrevNeg.difference(exitNeg)] = self.PositionWOSL.loc[self.LastTime, PrevNeg.difference(exitNeg)]
     
    
    def MD8_Vortex(self, tickersList):
        '''Vortex'''
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Ratio = self.PviNviRatio.loc[self.CurrentTime, tickersList]
        self.PositionWOSL.loc[self.CurrentTime, Price[Ratio>0].dropna().index] = 1
        self.PositionWOSL.loc[self.CurrentTime, Price[Ratio<0].dropna().index] = -1
        
    def MD9_Oscillator(self, tickersList):
        '''Oscillator'''
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
        '''RSI50'''
        Price = self.Close.loc[self.CurrentTime, tickersList]
        RSI = self.BackTestData.RSI14.loc[self.CurrentTime, tickersList]
        self.PositionWOSL.loc[self.CurrentTime, Price[RSI>50].dropna().index] = 1
        self.PositionWOSL.loc[self.CurrentTime, Price[RSI<50].dropna().index] = -1

    def MD11_AssymetricWeekly(self, tickersList):
        '''Assymetric Weekly'''
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Long = Price[self.CloseDiffMinSMA.loc[self.CurrentTime, tickersList] > 0].dropna().index
        Short = Price[self.CloseDiffMinSMA.loc[self.CurrentTime, tickersList] <= 0].dropna().index
        self.PositionWOSL.loc[self.CurrentTime, Long] = 1
        self.PositionWOSL.loc[self.CurrentTime, Short] = -1
        
    def MD12_Seasoning(self, tickersList):
        '''Seasoning'''
        Price = self.Close.loc[self.CurrentTime, tickersList]
        Long = set(Price.dropna().index)
        if self.CurrentTime.day not in range(5, 25):
            self.PositionWOSL.loc[self.CurrentTime, Long] = 1
        for iTicker in tickersList:
            self.StopLoss(iTicker, -0.03)# -3% StopLoss
            self.Target(iTicker, 0.10)# +10% Target
            
    def UpdateSpecificStats(self):        
        #allTickers = set(self.Close.columns)     
        #self.MD1_RSIwith50SMATrend(allTickers)
        
        indexNames = self.Close.columns
        
        # Model 1: RSI+ 50SMA Trend
        m1Tickers = set.intersection(set(MODELSELECTOR[1]), set(indexNames))
        self.MD1_RSIwith50SMATrend(m1Tickers) 
        
        #Model 2: RSI + 50SMA
        m2Tickers = set.intersection(set(MODELSELECTOR[2]), set(indexNames))
        self.MD2_RSI50SMA(m2Tickers)
        
        #Model 3:Series3
        m3Tickers = set.intersection(set(MODELSELECTOR[3]), set(indexNames))
        self.MD3_Series3(m3Tickers)
        
        #Model 4:20WMA_MACD
        m4Tickers = set.intersection(set(MODELSELECTOR[4]), set(indexNames))
        self.MD4_20WMA_MACD(m4Tickers)
        
        #Model 5:Body OutSide Band
        m5Tickers = set.intersection(set(MODELSELECTOR[5]), set(indexNames))
        self.MD5_BodyOutSideBand(m5Tickers)
        
        #Model 6: ROC MA
        m6Tickers = set.intersection(set(MODELSELECTOR[6]), set(indexNames))
        self.MD6_ROCMA(m6Tickers)
        
        #Model 7: Regression CrossOver
        m7Tickers = set.intersection(set(MODELSELECTOR[7]), set(indexNames))
        self.MD7_RegressionCrossOver(m7Tickers)
        
        #Model 8: Vortex
        m8Tickers = set.intersection(set(MODELSELECTOR[8]), set(indexNames))
        self.MD8_Vortex(m8Tickers)
        
        # Model 9: Oscillator
        m9Tickers = set.intersection(set(MODELSELECTOR[9]), set(indexNames))
        self.MD9_Oscillator(m9Tickers)
        
        #Model 10: RSI50
        m10Tickers = set.intersection(set(MODELSELECTOR[10]), set(indexNames))
        self.MD10_RSI50(m10Tickers)
        
        #Model 11: Assymetric Weekly
        m11Tickers = set.intersection(set(MODELSELECTOR[11]), set(indexNames))
        self.MD11_AssymetricWeekly(m11Tickers)
        
        #Model 12: Seasoning with Nifty Index Ratio
        m12Tickers = set.intersection(set(MODELSELECTOR[12]), set(indexNames))
        self.MD12_Seasoning(m12Tickers)
            
    def updateCapAllocation(self):
        #self.populateOrderPosition()
        self.CapitalAllocation.loc[self.CurrentTime] = 0       
        positionWOSL = self.PositionWOSL.loc[self.CurrentTime]
        tempDF = positionWOSL[positionWOSL != 0]
        
        self.Position.loc[self.CurrentTime] = self.PositionWOSL.loc[self.CurrentTime]
        for ticker in tempDF.index:
            TradeTakenDate = self.DetectPostionStartDate(ticker, self.PositionWOSL)
            checkExitPosition = self.PositionExitDF.loc[TradeTakenDate:self.CurrentTime, ticker].dropna()
            if len(checkExitPosition) >0:
                self.Position.loc[self.CurrentTime, ticker] = 0
            else:
                self.Position.loc[self.CurrentTime, ticker] = self.PositionWOSL.loc[self.CurrentTime, ticker]

        MaxWT = 3.0/100
        indexName = 'NZ1 Index'#self.BackTestData.indexprice.columns[0]
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
        print(self.CurrentTime.date())


# today = datetime.date.today()
# startday = today -datetime.timedelta(545)# 1.5 Yrs Data

# modelfile = 'G:/Shared drives/Quant/Trades/Logs/ModelSelector.xlsx'
# modelTable = pandas.read_excel(modelfile, sheet_name = 'MomentumModel', header = 0, index_col= 0)
# allFutTickers = list(modelTable['Generic'])

# MODELSELECTOR = {}
# gg = modelTable.groupby('Model')
# for grp in gg.groups.keys():
#     MODELSELECTOR[grp] = list(gg.get_group(3)['Generic'])



# ColumnsDict = {'LAST_PRICE' : 'Close', 'PX_HIGH' : 'High', 'PX_LOW' : 'Low', 'PX_OPEN': 'Open', 'FUT_CONT_SIZE' : 'LotSize'}
def getUpdatedFutData(allFutTickers, startdate):
    tempdataObj = MyBacktestData()
    #pdb.set_trace()
    indata = bbg.BloomRequestData(allFutTickers, datatype = 'FutLiveData',sectype = 'not index',action = 'notinsert', startdate = startdate.strftime('%Y%m%d'), enddate = datetime.date.today().strftime('%Y%m%d'))
    gg = indata.groupby('Ticker')
    for grp in gg.groups.keys():
        dtemp = gg.get_group(grp)
        del dtemp['Ticker']
        dtemp.index = dtemp['date']
        del dtemp['date']
        for colname in dtemp.columns:
            temp = dtemp[colname].dropna()
            temp = pandas.to_numeric(temp)
            temp = pandas.DataFrame(temp)
            temp.columns = [grp]
            temp.index = pandas.to_datetime(temp.index)
            if hasattr(tempdataObj, ColumnsDict[colname]):
                setattr(tempdataObj, ColumnsDict[colname], pandas.concat([getattr(tempdataObj, ColumnsDict[colname]), temp], axis = 1))
            else:
                setattr(tempdataObj, ColumnsDict[colname], temp)
    for atr in ColumnsDict.values():
        setattr(tempdataObj, atr, getattr(tempdataObj, atr).sort_index())
    return tempdataObj

def getUpdatedDFObjects(allFutTickers):
    mydata = MyBacktestData()
    pickleFile = 'G:/Shared drives/Quant/Trades/Logs/PriceData.pkl'
    pf = open(pickleFile, 'rb')
    prevDataObj = pickle.load(pf)
    lastDate = prevDataObj.Close.index[-2]#.date()
    
    curDataObj = getUpdatedFutData(allFutTickers, lastDate.date())
    commonStocks = set.intersection(set(prevDataObj.Close.columns), set(curDataObj.Close.columns))
    newIntroduced = set.difference(set(allFutTickers), commonStocks)
    
    prevClose = prevDataObj.Close.loc[lastDate, commonStocks]
    curClose = curDataObj.Close.loc[lastDate, commonStocks]
    corpActionList = set(curClose[curClose != prevClose].dropna().index)
    
    #fetch full historical data for newIntroduced and Corporate Actions Stocks
    #Append the new recent date data to the old data from pickle file for remaining stocks
    toVertAppend = commonStocks.difference(corpActionList)
    for atr in ColumnsDict.values():
        tempDF = pandas.concat([getattr(prevDataObj, atr).loc[:, toVertAppend], getattr(curDataObj, atr).loc[:, toVertAppend]], axis = 0)
        tempDF = tempDF.sort_index()
        setattr(mydata, atr, tempDF[~tempDF.index.duplicated(keep='last')])
    
    toHorizonAppend = newIntroduced.union(corpActionList)
    if len(toHorizonAppend)>0:
        newDataObj = getUpdatedFutData(toHorizonAppend, startday)
        for atr in ColumnsDict.values():
            setattr(mydata, atr, pandas.concat([getattr(mydata, atr), getattr(newDataObj, atr).loc[:, toHorizonAppend]], axis = 1))
    
    pf = open(pickleFile, 'wb')
    pickle.dump(mydata, pf)
    pf.close()
    return mydata
               
if __name__=='__main__':    
    mydata = getUpdatedDFObjects(allFutTickers)
    for atr in ColumnsDict.values():
        getattr(mydata, atr)['Cash'] = [1.00]*getattr(mydata, atr).shape[0]
        
    mydata.indexprice = mydata.Close['NZ1 Index']
    mydata.indexprice = mydata.indexprice[mydata.indexprice.index.isin(mydata.Close.index)]

    mydata.RSI14 = MyTechnicalLib.GetRSI(mydata.Close, 14)
    mydata.RSI5 =  MyTechnicalLib.GetRSI(mydata.Close, 5)
    mydata.RSI2 =  MyTechnicalLib.GetRSI(mydata.Close, 2)

    mydata.SMA10 = MyTechnicalLib.MovingAverage(mydata.Close,10)
    mydata.SMA20 = MyTechnicalLib.MovingAverage(mydata.Close,20)
    mydata.SMA21 = MyTechnicalLib.MovingAverage(mydata.Close,21)
    mydata.SMA24 = MyTechnicalLib.MovingAverage(mydata.Close,24)
    mydata.SMA50 = MyTechnicalLib.MovingAverage(mydata.Close,50)
    mydata.SMA100 = MyTechnicalLib.MovingAverage(mydata.Close,100)


    mydata.EMA21 = MyTechnicalLib.GetEMA(mydata.Close, 21 , ShiftDays=0)
    mydata.WMA20 = MyTechnicalLib.GetWMA(mydata.Close, 20)
    mydata.MACD, mydata.MACDSignal = MyTechnicalLib.MACD(mydata.Close)

    mydata.ATR24 = MyTechnicalLib.GetATR(mydata, 24)

    mydata.ROC27D = MyTechnicalLib.GetROC(mydata, 27)
    mydata.ROCMA18D = MyTechnicalLib.MovingAverage(mydata.ROC27D, 18)

    mydata.NVI, mydata.PVI = MyTechnicalLib.VortexOscillator(mydata, 14)
    mydata.LS, mydata.LR = MyTechnicalLib.RegressionCrossOverSignal(mydata.Close, LSDays = 5, LRDays = 50)
    
    
    liveDatafile = 'Z:/Pickles/Live_FutsData_Auto_'+ datetime.datetime.today().date().strftime('%d%b-%Y') +'.pkl'
    f = open(liveDatafile, 'wb')
    pickle.dump(mydata, f)
    f.close()
    
    # f = open('Z:/Pickles/Live_FutsData__15Nov-2022.pkl', 'rb')
    # mydata = pickle.load(f)
    # f.close()
    
    basePath = 'Z:/BacktestsResults/DailyTradingSignals_V2/'    
    a = DailyTradingSignals(mydata, modelTable = modelTable)
    a.run()
    a.ResultFrameWithIndex()
    backtestName = 'LiveModel_Auto_'
    filepath = basePath+backtestName+'_'+str(datetime.datetime.today().date())+'.xlsx'
    a.savebacktestresult(filepath)
    backtestName = backtestName.replace(', ', '_').replace(' ', '_')

endtime = time.time()
runtime = endtime - starttime
print('RunTime:',numpy.round(runtime, 1), ' Seconds')