#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 23-01-2023 09:26
# @Author : Ankur

import pandas as pd
import config as cf
from sqlalchemy import create_engine

# postgres version

class DataBaseConnect:

    def __init__(self, engine_url):
        self.db_conn = create_engine(engine_url)

    def get_ticker_data(self, ticker):
        query = """
        select * from alts.trade.stocks where "Ticker"='%s'
        """%(ticker)
        df = pd.read_sql(query, self.db_conn)
        return df

    def get_data_current(self, symbol, input_date, ce_ticker, pe_ticker):
        query = """
        select * from alts.trade.stocks where "Ticker" in ('%s','%s') and "Date"=date('%s')  
        """%(ce_ticker, pe_ticker, input_date)
        df = pd.read_sql(query, self.db_conn)
        return df

    def get_data(self, symbol, input_date, input_time, ticker): ## internal function used by get_px
        query = """
        select * from alts.trade.stocks where "Date"=date('%s') and "Time"='%s' and "Ticker"='%s' 
        """%(input_date, input_time, ticker)
        df = pd.read_sql(query, self.db_conn)
        return df

    def get_data_tickers(self, input_date, tickers):
        ticker_str = "("+",".join(["'"+i+"'" for i in tickers])+")"
        query = f'select * from alts.trade.stocks where "Ticker" in ' + ticker_str + ' and "Date" = ' +"'"+input_date+"'"
        df = pd.read_sql(query, self.db_conn)
        return df


    def get_px(self, symbol, input_date, input_time, ticker, px='close'):
        df = self.get_data(symbol, input_date, input_time, ticker)
        if not df.empty:
            if px == 'close':
                return df['Close'].values[0]
            elif px == 'high':
                return df['High'].values[0]
            else:
                return None


def get_expiry_dates(symbol, start, end, weekly=True):
    df = pd.read_csv(r'G:\My Drive\workspace\strategies\commons\new_expirys.csv')
    if weekly:
        df = df.loc[(df['SYMBOL']==symbol) & (df['WEEKLY']==1)]
    else: # monthly
        df = df.loc[(df['SYMBOL'] == symbol) & (df['WEEKLY'] == 1)]
    df['DATE'] = pd.to_datetime(df['DATE'], format='%Y-%m-%d')
    df = df.loc[(df['DATE'] >= start) & (df['DATE']<= end)]
    exp_dates = list(df['DATE'])
    # exp_dates = [dt.date() for dt in exp_dates]
    return exp_dates



def main():
    '''
    engine_url = f'postgresql+psycopg2://{cf.user_name}:{cf.pwd}@{cf.host}:{cf.port}/{cf.db_name}'
    db_obj = DataBaseConnect(engine_url)
    # SELECT * FROM NIFTY WHERE "DATE" = "2022-11-29" AND "TIME"="12:49:59" AND "TICKER" = "NIFTY01DEC2218600PE.NFO";
    # df = db_obj.get_data('NIFTY','2022-11-29','12:49:59','NIFTY01DEC2218600PE.NFO')
    tickers = ['NIFTY01DEC2218600PE.NFO','NIFTY01DEC2218600CE.NFO']
    df = db_obj.get_data('NIFTY','2023-10-31','09:29:59','NIFTY-I.NFO')#db_obj.get_data_tickers('2022-11-29', tickers)
    print(df.head())
    '''
    from datetime import datetime
    get_expiry_dates('NIFTY', datetime(2019,2,6), datetime(2023,10,31))


if __name__ == '__main__':
    main()

