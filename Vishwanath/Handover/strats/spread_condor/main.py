#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 05-03-2024 10:47
# @Author : Ankur

import pandas as pd
from datetime import datetime, date
from sqlalchemy import create_engine

spot_data_file = r'G:\Shared drives\BackTests\Ankur\IDX.xlsx'
expiry_file = r'G:\Shared drives\BackTests\Ankur\exps.csv'

#engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'
engine_url = f'postgresql+psycopg2://{"postgres"}:{"admin"}@{"localhost"}:{"5432"}/{"idx_data"}'
conn = create_engine(engine_url)

def read_expiry(symbol, start, end, weekly=True,):
    exps_df = pd.read_csv(expiry_file)
    exps_df['DATE'] = pd.to_datetime(exps_df['DATE'], format="%d-%m-%Y")
    exps_df = exps_df.loc[(exps_df['DATE'] >= start)&(exps_df['DATE'] <= end)]
    if weekly:
        exps_df = exps_df.loc[(exps_df['SYMBOL']==symbol)&(exps_df['WEEKLY']==1)&(exps_df['INSTRUMENT']=='OPTIDX')]
    else:
        exps_df = exps_df.loc[(exps_df['SYMBOL'] == symbol) & (exps_df['MONTHLY'] == 1) & (exps_df['INSTRUMENT'] == 'OPTIDX')]
    exps = sorted(set(exps_df['DATE']))
    return exps


def get_expiry(input_date, exps):
    filter_exps = [dt for dt in exps if dt >= input_date]
    current_exp = filter_exps[0]
    next_exp = filter_exps[1]
    return current_exp, next_exp


def read_spot(symbol, start, end):
    try:
        data = pd.read_excel(spot_data_file, parse_dates=['Date'])
        data['Date'] = data['Date'].dt.date
        data.set_index('Date', inplace=True)
        data = data.loc[(data.index >= start) & (data.index <= end)][symbol]
        return data
    except Exception as e:
        return None


def xread_fno(tickers, start, end):
    #engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.9"}:{"5433"}/{"Data"}'
    engine_url = f'postgresql+psycopg2://{"postgres"}:{"admin"}@{"localhost"}:{"5432"}/{"idx_data"}'
    conn = create_engine(engine_url)
    ticker_str = "(" + ",".join(["'" + i + "'" for i in tickers]) + ")"
    # query = """
    # select "TIMESTAMP", "CLOSE", "Ticker" from public.bhavcopy where "TIMESTAMP" between date('%s') and date('%s') and "Ticker" in %s
    # """%(start, end, ticker_str)
    # #print(query)
    query = """
        select * from public.bhavcopytwo where "TIMESTAMP" between date('%s') and date('%s') and "Ticker" in %s
        """ % (start, end, ticker_str)
    data = pd.read_sql(query, conn)
    pivot_frame = pd.pivot_table(data, values='CLOSE', index='TIMESTAMP',columns='Ticker')
    return pivot_frame

def read_fno(tickers, start, end):
    ticker_str = "(" + ",".join(["'" + i + "'" for i in tickers]) + ")"
    # query = """
    # select "TIMESTAMP", "CLOSE", "Ticker" from public.bhavcopy where "TIMESTAMP" between date('%s') and date('%s') and "Ticker" in %s
    # """%(start, end, ticker_str)
    # #print(query)
    # query = """
    #     select * from public.bhavcopytwo where "TIMESTAMP" between date('%s') and date('%s') and "Ticker" in %s
    #     """ % (start, end, ticker_str)
    query = """
        select * from data.public.nsefno where "TIMESTAMP" between date('%s') and date('%s') and "Ticker" in %s
        """ % (start, end, ticker_str)
    data = pd.read_sql(query, conn)
    return data


def fmt(expiry):
    return expiry.strftime('%d%b%y').upper()


def werqwemain(): # 2%/3% banknifty // 2%/5% banknifty
    symbol= 'BANKNIFTY'
    start = date(2016,6,1)#date(2019,3,1)
    end = date(2024,2,29)
    spread = 0.05
    expiry_holidays = {datetime(2014,2,27):datetime(2014,2,26), datetime(2018,3,29):datetime(2018,3,28), datetime(2023,3,30):datetime(2023,3,29)}
    spot_data = read_spot(symbol, start, end)
    spot_data = spot_data.to_frame()
    weekly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), True)
    weekly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in weekly_expiry])))
    #monthly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), False)
    #monthly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in monthly_expiry])))
    all_dfs = []
    for idx in range(1,len(weekly_expiry)):
        try:
            current = weekly_expiry[idx-1]
            next = weekly_expiry[idx]
            if next == datetime(2023,6,28):
                next = datetime(2023,6,29)
            spot_px = spot_data.loc[spot_data.index == current.date()][symbol].values[0]
            strike = int(round(spot_px * (1-spread)/100,0)*100)
            ticker = symbol+fmt(next)+'PE'+str(strike)
            data = read_fno([ticker], current.strftime('%Y-%m-%d'), next.strftime('%Y-%m-%d'))
            if not data.empty:
                all_dfs.append(data)
                print(f'Completed {ticker}')
            else:
                print(f'Data not present for ticker {ticker}')
        except Exception as e:
            print(e, current)
    main_df = pd.concat(all_dfs, axis=0)
    main_df.to_excel('banknifty_5perc_otm_pe_long_weekly.xlsx')


def main(): # 3 percent otm pe
    symbol= 'NIFTY'
    start = date(2019,3,1)
    end = date(2024,4,2)
    spread = 0.03
    expiry_holidays = {datetime(2014,2,27):datetime(2014,2,26), datetime(2018,3,29):datetime(2018,3,28), datetime(2023,3,30):datetime(2023,3,29)}
    spot_data = read_spot(symbol, start, end)
    spot_data = spot_data.to_frame()
    weekly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), True)
    weekly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in weekly_expiry])))
    #monthly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), False)
    #monthly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in monthly_expiry])))
    all_dfs = []
    for idx in range(1,len(weekly_expiry)):
        try:
            current = weekly_expiry[idx-1]
            next = weekly_expiry[idx]
            if next == datetime(2023,6,28):
                next = datetime(2023,6,29)
            spot_px = spot_data.loc[spot_data.index == current.date()][symbol].values[0]
            strike = int(round(spot_px * (1+spread)/100,0)*100)
            ticker = symbol+fmt(next)+'CE'+str(strike)
            data = read_fno([ticker], current.strftime('%Y-%m-%d'), next.strftime('%Y-%m-%d'))
            if not data.empty:
                all_dfs.append(data)
                print(f'Completed {ticker}')
            else:
                print(f'Data not present for ticker {ticker}')
        except Exception as e:
            print(e, current)
    main_df = pd.concat(all_dfs, axis=0)
    main_df.to_excel('nifty_3perc_otm_ce_long_weekly.xlsx')


def xxfamain(): # Monthly ITM 5perc call long version # commenting for testing different combination # Nifty atm ce sell
    symbol= 'NIFTY'
    start = date(2010,1,1)
    end = date(2024,2,29)
    spread = 0
    expiry_holidays = {datetime(2014,2,27):datetime(2014,2,26), datetime(2018,3,29):datetime(2018,3,28), datetime(2023,3,30):datetime(2023,3,29)}
    spot_data = read_spot(symbol, start, end)
    spot_data = spot_data.to_frame()
    weekly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), True)
    monthly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), False)
    monthly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in monthly_expiry])))
    all_dfs = []
    for idx in range(1,len(monthly_expiry)):
        try:
            current = monthly_expiry[idx-1]
            next = monthly_expiry[idx]
            if next == datetime(2023,6,28):
                next = datetime(2023,6,29)
            spot_px = spot_data.loc[spot_data.index == current.date()][symbol].values[0]
            strike = int(round(spot_px * (1-spread)/100,0)*100)
            ticker = symbol+fmt(next)+'CE'+str(strike)
            data = read_fno([ticker], current.strftime('%Y-%m-%d'), next.strftime('%Y-%m-%d'))
            if not data.empty:
                all_dfs.append(data)
                print(f'Completed {ticker}')
            else:
                print(f'Data not present for ticker {ticker}')
        except Exception as e:
            print(e, current)
    main_df = pd.concat(all_dfs, axis=0)
    main_df.to_excel('nifty_atm_ce_short_monthly.xlsx') #'nifty_5perc_itm_ce_long_monthly_2010.xlsx'


def ffmain(): #Monthly OTM 5perc call long version # Monthly OTM 5perc put long version (hedge fill) # commenting for testing different combination
    symbol= 'NIFTY'
    start = date(2010,1,1)
    end = date(2024,4,2)
    spread = 0.05
    expiry_holidays = {datetime(2014,2,27):datetime(2014,2,26), datetime(2018,3,29):datetime(2018,3,28), datetime(2023,3,30):datetime(2023,3,29)}
    spot_data = read_spot(symbol, start, end)
    spot_data = spot_data.to_frame()
    weekly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), True)
    monthly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), False)
    monthly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in monthly_expiry])))
    all_dfs = []
    for idx in range(1,len(monthly_expiry)):
        try:
            current = monthly_expiry[idx-1]
            next = monthly_expiry[idx]
            if next == datetime(2023,6,28):
                next = datetime(2023,6,29)
            spot_px = spot_data.loc[spot_data.index == current.date()][symbol].values[0]
            strike = int(round(spot_px * (1+spread)/100,0)*100)
            ticker = symbol+fmt(next)+'CE'+str(strike)
            data = read_fno([ticker], current.strftime('%Y-%m-%d'), next.strftime('%Y-%m-%d'))
            if not data.empty:
                all_dfs.append(data)
                print(f'Completed {ticker}')
            else:
                print(f'Data not present for ticker {ticker}')
        except Exception as e:
            print(e, current)
    main_df = pd.concat(all_dfs, axis=0)
    main_df.to_excel('nifty_5perc_otm_ce_long_monthly_hedge.xlsx') # _2 version is the same as previous, its just the data start date is 2010-1-1


def xcvmain(): # 4% otm ce short/6% otm pe long (hedge) and futures long spread (BANKNIFTY) // atm ce short/ 5% otm pe long (hedge) and futures long spread (BANKNIFTY)
    symbol= 'BANKNIFTY'
    start = date(2010,1,1)
    end = date(2024,2,29)
    spread = 0.00 # 0.00 for atm
    expiry_holidays = {datetime(2014,2,27):datetime(2014,2,26), datetime(2018,3,29):datetime(2018,3,28), datetime(2023,3,30):datetime(2023,3,29)}
    spot_data = read_spot(symbol, start, end)
    spot_data = spot_data.to_frame()
    weekly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), True)
    monthly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), False)
    monthly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in monthly_expiry])))
    all_dfs = []
    for idx in range(1,len(monthly_expiry)):
        try:
            current = monthly_expiry[idx-1]
            next = monthly_expiry[idx]
            if next == datetime(2023,6,28):
                next = datetime(2023,6,29)
            spot_px = spot_data.loc[spot_data.index == current.date()][symbol].values[0]
            strike = int(round(spot_px * (1-spread)/100,0)*100)
            ticker = symbol+fmt(next)+'CE'+str(strike)
            # if ticker == 'BANKNIFTY28SEP23PE41300':
            #     ticker = 'BANKNIFTY28SEP23PE41000'
            # if ticker == 'BANKNIFTY30NOV23PE39700':
            #     ticker = 'BANKNIFTY30NOV23PE39500'
            data = read_fno([ticker], current.strftime('%Y-%m-%d'), next.strftime('%Y-%m-%d'))
            if not data.empty:
                all_dfs.append(data)
                print(f'Completed {ticker}')
            else:
                print(f'Data not present for ticker {ticker}')
        except Exception as e:
            print(e, current)
    main_df = pd.concat(all_dfs, axis=0)
    main_df.to_excel('banknifty_atm_ce_short_monthly.xlsx')

#BANKNIFTY28SEP23PE41300 #BANKNIFTY30NOV23PE39700

def fut_data():
    symbol = 'NIFTY'
    start = date(2010,1,1)#date(2016,5,1)
    end = date(2024,2,29)
    spread = 0.05
    expiry_holidays = {datetime(2014,2,27):datetime(2014,2,26), datetime(2018,3,29):datetime(2018,3,28), datetime(2023,3,30):datetime(2023,3,29)}
    spot_data = read_spot(symbol, start, end)
    spot_data = spot_data.to_frame()
    weekly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), True)
    monthly_expiry = read_expiry(symbol, datetime.combine(start, datetime.min.time()), datetime.combine(end, datetime.min.time()), False)
    monthly_expiry = sorted(list(set([expiry_holidays[i] if i in expiry_holidays.keys() else i for i in monthly_expiry])))
    all_dfs = []
    for idx in range(1,len(monthly_expiry)):
        try:
            current = monthly_expiry[idx-1]
            next = monthly_expiry[idx]
            if next == datetime(2023,6,28):
                next = datetime(2023,6,29)
            spot_px = spot_data.loc[spot_data.index == current.date()][symbol].values[0]
            #strike = int(round(spot_px * (1-spread)/100,0)*100)
            ticker = symbol+fmt(next)+'XX0'
            data = read_fno([ticker], current.strftime('%Y-%m-%d'), next.strftime('%Y-%m-%d'))
            if not data.empty:
                all_dfs.append(data)
                print(f'Completed {ticker}')
            else:
                print(f'Data not present for ticker {ticker}')
        except Exception as e:
            print(e, current)
    main_df = pd.concat(all_dfs, axis=0)
    main_df.to_excel('nifty_fut.xlsx')

def monthly():
    df = pd.read_excel('nifty_5perc_itm_ce_long_monthly.xlsx')
    df = df[['TIMESTAMP','Return']]
    df.rename(columns={'TIMESTAMP':'Date','Return':'Monthly_5perc_ITM_Long'}, inplace=True)
    df.set_index('Date', inplace=True)
    monthly = df.resample('M').sum()
    monthly.to_excel('sample_monthy.xlsx')
    print(monthly)


    #print(monthly_expiry)


# def main():
#
#     symbol= 'NIFTY'
#     start = date(2019,3,1)
#     end = date(2024,2,29)
#     spread = 0.05
#
#     spot_data = read_spot(symbol, start, end)
#     spot_data = spot_data.to_frame()
#     weekly_expiry = read_expiry(symbol)
#     monthly_expiry = read_expiry(symbol, False)
#     expiry_struct = []
#     for dt in list(spot_data.index):
#         current_week, next_week = get_expiry(dt, weekly_expiry)
#         current_month, next_month = get_expiry(dt, monthly_expiry)
#         expiry_struct.append([dt, fmt(current_week), fmt(next_week), fmt(current_month), fmt(next_month)])
#     expiry_frame = pd.DataFrame(expiry_struct, columns=['Date','CurrentWeek','NextWeek','CurrentMonth','NextMonth'])
#     expiry_frame.set_index('Date', inplace=True)
#     spot_data = spot_data.join(expiry_frame, how='left')
#     spot_data['Strike'] = spot_data[symbol].apply(lambda x: round((x*(1+spread))/100)*100)
#     spot_data['TickerC'] = symbol+spot_data['CurrentMonth']+'CE'+spot_data['Strike'].astype(str)
#     spot_data['TickerN'] = symbol + spot_data['NextMonth']+'CE'+spot_data['Strike'].astype(str)
#     # print(spot_data)
#     unique_tickers = list(set(list(spot_data['TickerC'])+list(spot_data['TickerN'])))
#     fno_data = read_fno(unique_tickers, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
#     fno_data.to_excel('nifty_itm_5perc_data.xlsx')
#
#     def get_px(row):
#         print(fno_data.loc[fno_data.index == row.name][row.TickerC])
#     spot_data['TickerC_Px'] = spot_data.apply(get_px, axis=1)



if __name__ == '__main__':
    main()
    #monthly()
    #fut_data()