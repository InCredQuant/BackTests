#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 12-03-2024 10:59
# @Author : Ankur

import pandas as pd
from sqlalchemy import create_engine

# greek_path = r'G:\Shared drives\BackTests\DB\public.greeks_nsefno.csv'
# df = pd.read_csv(greek_path)
# print(df.head())

engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'
conn = create_engine(engine_url)

def delta(row):
    try:
        ticker = row['Ticker']
        dt = row['TIMESTAMP'].strftime('%Y-%m-%d')
        delta = get_delta_val(ticker, dt)
        print(ticker, dt, delta)
    except:
        pass

def read_file():
    file_name = r'G:\My Drive\workspace\strategies\BATCH7\positional_weekly\nifty_5perc_otm_ce_long_monthly_hedge.xlsx'#r'.\fut_ticker_based_same\fut_2perc_5perc.xlsx'
    df = pd.read_excel(file_name, sheet_name='working')#, index_col=0)
    df['Delta'] = df.apply(delta, axis=1)
    #df.to_excel('delta_6_'+file_name)

def get_delta_val(ticker, dt):
    try:
        query = """
        select "Delta" from data.public.greeks_nsefno where "Ticker"='%s' and "Date"='%s';
        """%(ticker, dt)
        df = pd.read_sql(query, conn)
        if not df.empty:
            delta_val = df.values[0][0]
            return delta_val
    except:
        pass

















































































































































































































































if __name__ == '__main__':
    #main()
    read_file()

