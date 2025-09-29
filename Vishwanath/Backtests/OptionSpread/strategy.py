#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 17-05-2023 13:11
# @Author : Ankur


import sys
import pandas
import numpy
import time

import pandas as pd

sys.path.insert(1, 'G:\Shared drives\BackTests\pycode\MainLibs')
from MainLibs.FactoryBackTester import FactoryBackTester
import pickle
from MainLibs.trade_register import TradeRegister
from MainLibs.stats import Stats, Filter
import datetime
from strategy_data import get_px_frame, create_db_conn
import matplotlib.pyplot as plt

class OptionSpread(FactoryBackTester):

    def __init__(self, data):
        FactoryBackTester.__init__(self, data)
        self.TradedValue = 50000000  # 5Cr.
        self.StrategyType = 'OP'
        self.PairTrades = False

    def basicdatainitialize(self):
        self.CurrentTime = pandas.to_datetime('2016-06-02')# pandas.to_datetime(
        #     list(self.BackTestData.TradingDates.keys())[0])  # pandas.to_datetime('2014-06-26')
        self.UpdateDates = self.ExpiryDates#list(self.BackTestData.TradingDates.keys())  # self.ExpiryDates
        self.UpdateDates = [i for i in self.UpdateDates if i >= self.CurrentTime and i <= self.EndTime]
        self.PositionExitDF = pandas.DataFrame(numpy.nan, index=self.Close.index,
                                               columns=self.Close.columns)  # it tracks for the exit records, StopLoss, Target or anything which may be added
        self.PositionWOSL = pandas.DataFrame(numpy.zeros_like(self.Close), index=self.Close.index,
                                             columns=self.Close.columns)  # It tracks before applying anytype of StopLoss or target
        self.Quantity = pandas.DataFrame(numpy.zeros_like(self.Close), index=self.Close.index,
                                         columns=self.Close.columns)
        self.Exposure = pandas.DataFrame(numpy.zeros_like(self.Close), index=self.Close.index,
                                         columns=self.Close.columns)
        self.trade_reg = TradeRegister()

    def declarecurrentvariables(self):
        self.LastPosition = self.Position.loc[self.LastTime]
        self.CurrentNAV = self.NAV.loc[self.CurrentTime, 'NAV']
        self.CurrentPrice = self.Close.loc[self.CurrentTime].dropna()

    def detectupdatedate(self):
        if self.CurrentTime in self.UpdateDates:
            return True

    def UpdateSpecificStats(self):
        if self.CurrentTime <= self.EndTime:
            try:
                self.Position.loc[self.CurrentTime] = 0
                self.Quantity.loc[self.CurrentTime] = 0
                if self.CurrentTime in self.ExpiryDates:
                    for ticker in self.BackTestData.ticker_on_expiry[self.CurrentTime]:
                        if 'CE' in ticker:
                            short_ticker = ticker
                        elif 'PE' in ticker:
                            long_ticker = ticker
                        else:
                            fut_ticker = ticker
                    # positions update
                    self.Position.loc[self.CurrentTime, short_ticker] = -1
                    self.Position.loc[self.CurrentTime, long_ticker] = 1
                    self.Position.loc[self.CurrentTime, fut_ticker] = 1
                    # quantity update
                    self.Quantity.loc[self.CurrentTime, short_ticker] = -1
                    self.Quantity.loc[self.CurrentTime, long_ticker] = 1
                    self.Quantity.loc[self.CurrentTime, fut_ticker] = 1
            except Exception as e:
                print(e)

    def updateCapAllocation(self):
        self.UpdateOrderBook(strategyID='OptionSpread', options='y')


def main():
    t11 = time.time()
    with open('strategy_data.pkl', 'rb') as fileobj:  # r'Z:\Pickles\BNF_Disp_Strangle_3WkToExpiry17May-2023.pkl'
        mydata = pickle.load(fileobj)

    mydata.indexprice = mydata.Index.Close
    mydata.Close = mydata.Close.join(mydata.indexprice, how='outer')
    #------------------------------------------
    exps = list(mydata.ticker_on_expiry.keys())
    exp_map = {exps[i]:exps[i-1] for i in range(1,len(exps))}
    exp_map.update({datetime.datetime(2016, 5, 26, 0, 0):datetime.datetime(2016, 5, 26, 0, 0)})
    updated_trading_map = {exp_map[k]:mydata.ticker_on_expiry[k] for k in mydata.ticker_on_expiry.keys()}
    mydata.ticker_on_expiry = updated_trading_map
    # ------------------------------------------
    mydata.ticker_on_expiry[pd.to_datetime('2019-07-25')] = {'BANKNIFTY01AUG19CE29100': -1, 'BANKNIFTY01AUG19PE27500': 1, 'BANKNIFTY00XXX00XX0': 1}
    mydata.ticker_on_expiry[pd.to_datetime('2019-08-14')] = {'BANKNIFTY22AUG19CE27600': -1, 'BANKNIFTY22AUG19PE26000': 1,'BANKNIFTY00XXX00XX0': 1}
    mydata.ticker_on_expiry[pd.to_datetime('2020-03-05')] = {'BANKNIFTY12MAR20CE24300': -1, 'BANKNIFTY00XXX00XX0': 1}

    # fix futures token
    for t in mydata.ticker_on_expiry.keys():
        mydata.ticker_on_expiry[t].pop('BANKNIFTY00XXX00XX0')
        mydata.ticker_on_expiry[t]['BANKNIFTY12MAR20XX0'] = 1

    bhavcopy_conn = create_db_conn('BhavCopy')
    missing_strike_data = get_px_frame(bhavcopy_conn, ['BANKNIFTY01AUG19PE27500','BANKNIFTY22AUG19PE26000'], 'Close')
    mydata.Close = pd.concat([mydata.Close, missing_strike_data], axis=1) # injected missing data
    mydata.Close.rename(columns={'BANKNIFTY00XXX00XX0':'BANKNIFTY12MAR20XX0'}, inplace=True)

    model = OptionSpread(mydata)
    model.run()
    model.savebacktestresult('OptionSpreadOutput.xlsx', fullData=True)

    tradeDF = model.trade_reg.get_trade_register()
    stats_obj = Stats(tradeDF)
    statsSymbolDF = stats_obj.create_stats()


    writer = pandas.ExcelWriter('OptionSpread_TradeRegister.xlsx')
    tradeDF.to_excel(writer, 'Trades')
    statsSymbolDF.transpose().to_excel(writer, 'Stats-Symbol')
    model.PlotResult.to_excel(writer, 'NAV')
    writer.save()
    writer.close()
    t12 = time.time()
    print('Time Taken:', round((t12 - t11) / 60, 1), 'Mins', sep=' ')
    backtestName = 'OptionSpread'
    basePath = r'G:\My Drive\workspace\otm_call_put\ver1\charts\\'

    navData = model.PlotResult.dropna()
    navData.plot(title = backtestName, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_NAV.jpg')

    temp = navData.pct_change(252)
    temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
    temp = pandas.DataFrame(temp)
    temp.columns = ['Rolling 1Yr Returns ' + backtestName]
    temp['X-Axis'] = 0
    temp.plot(title = backtestName, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_RR.jpg')

    tmp = navData.resample('a').last()
    tmp = tmp.pct_change()
    tmp.index = tmp.index.year
    tmp.plot(kind = 'bar', title = backtestName, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_Y_Plot.jpg')

    tmp2 = navData.resample('q').last()
    tmp2 = tmp2.pct_change()
    tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
    tmp2.plot(kind = 'bar', title = backtestName, figsize =(18,6))
    plt.savefig(basePath+backtestName+'_Q_Plot.jpg')

if __name__ == '__main__':
    main()
