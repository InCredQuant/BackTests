#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 19-05-2023 11:52
# @Author : Ankur

import sqlite3
import sys

import pandas
import pandas as pd
sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
import MainLibs.GetData as gd
import datetime
from math import floor, ceil
import pickle
import itertools


# monthly expiry dates based market cap data
# rank stocks based on market cap data and select top 15 stocks per expiry
# generate atm strikes of respective stocks on expiry
# calculate 5% otm strike of nifty on expiry

def main():
    start_date = datetime.date(2011,11,25)
    end_date = datetime.date(2023,5,25)
    index_name = 'NIFTY INDEX'
    mydata = gd.MyBacktestData()
    mydata.Index = gd.Index()
    price_conn = gd.GetConn('PriceData')
    bhavcopy_conn = gd.GetConn('BhavCopy')
    mydata.ExpiryDates = gd.QueryExpiryDates(price_conn, expirytype='Monthly')
    mydata.ExpiryTimes = [datetime.datetime.strptime(it, '%Y-%m-%d') for it in
                          mydata.ExpiryDates]  # +datetime.timedelta(hours = 15, minutes=31)
    mydata.ExpiryTimes = [it for it in mydata.ExpiryTimes if (it.date() >= start_date and it.date() <= end_date)]
    mydata.indexcomponents = gd.GetComponentsForIndexForDateRange(price_conn, start_date, datetime.datetime.today(), index_name)
    mydata.bloom_stocks = list(set(itertools.chain(*mydata.indexcomponents.values())))
    mydata.bloom_nse_map = gd.QueryScripMaster(price_conn, mydata.bloom_stocks, 'Bloomberg', 'NSE')
    mydata.nse_stocks = [mydata.bloom_nse_map[stk] for stk in mydata.bloom_stocks]
    mydata.nse_stocks.sort()
    mydata.nse_stocks.append(index_name)
    mydata.index_price = gd.GetDataForIndicesFromBloomDB(price_conn,[index_name],'PX_LAST', start_date)
    mydata.Index.market_cap = gd.GetDataForTickersFromBloomDB(price_conn, mydata.bloom_stocks, 'MCAP', start_date)
    mydata.Index.market_cap.fillna(method='ffill', inplace=True)
    # mydata.Index.market_cap_rank = mydata.Index.market_cap.rank(axis=1, ascending=False)
    index_data = gd.GetDataForIndicesFromBloomDB(price_conn,[index_name],'MCAP',start_date)
    mydata.Index.market_cap = pandas.concat([mydata.Index.market_cap, index_data], axis=1)
    replace_exp = {'23APR14':'24APR14'}
    for idx in range(1, len(mydata.ExpiryTimes)-1):
        start = mydata.ExpiryTimes[idx-1]
        end = mydata.ExpiryTimes[idx]
        exp = end.strftime('%d%b%y').upper()
        if exp == '23APR14':
            exp = replace_exp[exp]

        compDate = [i for i in mydata.indexcomponents.keys() if i <= iStart][-1]
        nseComponents = mydata.indexcomponents[compDate]
        nseComponents = [mydata.DictBloomNSE[i] for i in nseComponents]
        tempMCap = mydata.Index.MarketCap.loc[:iStart].iloc[-1]
        tempMCap = tempMCap.loc[tempMCap.index.isin(nseComponents)]
        tempMCap = tempMCap.sort_values().iloc[-StocksCount:]
        nseComponents = list(set.intersection(set(nseComponents), set(tempMCap.index)))
        nseComponents.append(INDEXNAME)
#---------------------------------------------
    #     compDate = [i for i in mydata.indexcomponents.keys() if i <= start][-1]
    #     nseComponents = mydata.indexcomponents[compDate]
    #     nseComponents = [mydata.bloom_nse_map[i] for i in nseComponents]
    #     tempMCap = mydata.Index.market_cap.loc[:start].iloc[-1]
    #     tempMCap = tempMCap.loc[tempMCap.index.isin(nseComponents)]
    #     tempMCap = tempMCap.sort_values().iloc[-15:]
    #     nseComponents = list(set.intersection(set(nseComponents), set(tempMCap.index)))
    #     nseComponents = [mydata.bloom_nse_map[i] for i in nseComponents]
    #     nseComponents.append(index_name)
    #     diff_values = gd.GetNSEBhavCopyStrikePointsDiff(bhavcopy_conn, nseComponents, exp, getStrikes=True)
    #     print(diff_values)
    # print(mydata.indexcomponents)
#------------------------------------------------

if __name__ == '__main__':
    main()

# after getting the nse ticker, get equivalent futures tickers
# create atm strikes, add nifty futures ticker, at each iteration calculate 5% otm ticker of nifty