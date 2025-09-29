#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 15-05-2023 12:16
# @Author : Ankur

import sqlite3
import sys
import pandas as pd
sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
import MainLibs.GetData as gd
import datetime
from math import floor, ceil
import pickle


def get_offset_expiry(expiry_dates, input_date, offset=0):  # used mostly on the day of expiry
    cmp_date = datetime.datetime.combine(input_date, datetime.datetime.min.time())
    temp = [dt.date() for dt in expiry_dates if dt >= cmp_date]
    temp.sort()
    if temp:
        return temp[offset]


def create_db_conn(value):
    db_path = r'G:\My Drive\resources\data\db\\'
    return sqlite3.connect(db_path + value + '.db')


def create_ticker(symbol, expiry, strike, option_type, inst_type):
    exp = expiry.strftime('%d%b%y').upper()
    if inst_type in ['OPTSTK','OPTIDX']:
        return symbol+exp+option_type+str(strike)
    return symbol+exp+'XX0'


def query_px(conn, timestamp, ticker, field):
    query = f'select {field.upper()} from "NSEFNO" where "TICKER"="{ticker}" and "TIMESTAMP"="{timestamp}";'
    df = pd.read_sql(query, conn)
    return df[field.upper()].values[0]


def round_stk(val, base=100, method='round'):
    if method == 'round':
        return round(val / base) * base
    if method == 'floor':
        return floor(val / base) * base
    if method == 'ceil':
        return ceil(val / base) * base


def get_px_frame(conn, tickers, field):
    def format_tickers(tickers):
        tickers_str = '('+','.join(['"'+t+'"' for t in tickers])+')'
        return tickers_str
    def format_db_data(df, tickers):
        df.set_index('Date', inplace=True, drop=True)
        df.index = pd.to_datetime(df.index)
        ticker_group = df.groupby('Ticker')
        all_dfs = [pd.DataFrame(ticker_group.get_group(t)[field.upper()]).rename(columns={field.upper():t}) for t in ticker_group.groups.keys()]
        final_df = pd.concat(all_dfs, axis=1)
        final_df.sort_index(inplace=True)
        return final_df
    tickers_str = format_tickers(tickers)
    query = f'select TIMESTAMP as Date, TICKER, {field.upper()} from "NSEFNO" where "TICKER" in {tickers_str};'
    df = pd.read_sql(query, conn)
    cons_df = format_db_data(df, tickers)
    return cons_df


def create_data():
    symbol = 'BANKNIFTY'
    price_conn = create_db_conn('PriceData')
    bhavcopy_conn = create_db_conn('BhavCopy')
    weekly_expiry_str = gd.QueryExpiryDates(price_conn, expirytype='Weekly')
    monthly_expiry_str = gd.QueryExpiryDates(price_conn, expirytype='Monthly')
    weekly_expiry = [datetime.datetime.strptime(dt, '%Y-%m-%d') for dt in weekly_expiry_str]
    monthly_expiry = [datetime.datetime.strptime(dt, '%Y-%m-%d') for dt in monthly_expiry_str]
    tickers = []
    ticker_on_expiry = {}
    for wexp in weekly_expiry:
        try:
            mexp = get_offset_expiry(monthly_expiry, wexp)
            fut_ticker = create_ticker(symbol, mexp, None, None, 'FUTIDX')
            fut_px = query_px(bhavcopy_conn, wexp.strftime('%Y-%m-%d'), fut_ticker, 'Close')
            otm_ce = round_stk(fut_px*1.02) # sell
            otm_ce_ticker = create_ticker(symbol, wexp, otm_ce, 'CE', 'OPTIDX')
            otm_pe = round_stk(fut_px*0.95) # buy hedge
            otm_pe_ticker = create_ticker(symbol, wexp, otm_pe, 'PE', 'OPTIDX')
            tickers.extend([otm_ce_ticker, otm_pe_ticker])
            ticker_on_expiry[wexp] = {otm_ce_ticker:-1, otm_pe_ticker:1, 'BANKNIFTY00XXX00XX0':1}
            print(mexp, wexp, fut_ticker)
        except IndexError as e:
            pass
    df = get_px_frame(bhavcopy_conn, tickers, 'Close')
    print(df.head())
    # future data
    start = datetime.date(2016, 5, 27)
    price_conn = create_db_conn('PriceData')
    idx_fut_data = gd.GetDataForFutTickersFromBloomDB(price_conn, ['AF1 Index'], 'PX_LAST', start)[['AF1 INDEX']]
    idx_fut_data.columns = ['BANKNIFTY00XXX00XX0']

    # create data pickle
    mydata = gd.MyBacktestData()
    mydata.Index = gd.Index()
    mydata.Close = df
    mydata.ExpiryDates = weekly_expiry_str
    mydata.tickers = tickers
    mydata.ticker_on_expiry = ticker_on_expiry

    mydata.Index.Close = idx_fut_data#pd.merge(idx_fut_data, df, left_index=True, how='outer')

    with open('strategy_data.pkl','wb') as f:
        pickle.dump(mydata, f)
    print('Process Completed')


def experiment():
    # future data
    start = datetime.date(2016, 5, 27)
    price_conn = create_db_conn('PriceData')
    idx_fut_data = gd.GetDataForFutTickersFromBloomDB(price_conn, ['AF1 Index'], 'PX_LAST', start)[['AF1 INDEX']]
    idx_fut_data.columns = ['BANKNIFTY00XXX00XX0']
    print(idx_fut_data)

if __name__ == '__main__':
    create_data()
    #experiment()
