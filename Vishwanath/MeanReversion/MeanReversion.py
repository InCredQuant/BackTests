from re import L
import pandas as pd
import numpy as np
import sys
sys.path.insert(0,r'G:\Shared drives\BackTests\pycode\DBUpdation')
import pg_redirect
import warnings
warnings.filterwarnings("ignore")
import os
import psycopg2 as pg
from datetime import datetime, timedelta

index = 'NIFTY'
startdate = '2024-01-01'
enddate = '2025-05-23'
entrytime = '09:20:59'
exittime = '15:00:59'
stoploss_flag = 'Y'
stoploss = 0.30
weekly_filter = 'W1'

def fetch_options(index,startdate,enddate,entrytime,exittime):
    opt_query = f'''
                SELECT * FROM weekly_gdfl_min_opt
                where "Date" >= '{startdate}' and "Date" <= '{enddate}'
                AND ("Time" >= '{entrytime}' AND "Time" <= '{exittime}')
                AND "Name" = '{index.upper()}' and "Ticker" not like '%-I%'
                AND "Label" = '{weekly_filter}';
                '''
    conn = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    optdf = pd.read_sql(opt_query,conn)
    conn.close()
    return optdf

def fetch_spot_data(index,startdate,enddate):
    spotquery = f'''
                SELECT * FROM spotdata
                where "Date" >= '{startdate}' and "Date" <= '{enddate}'
                AND "Symbol" = '{index.upper()}';
                '''
    conn = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    spotdf = pd.read_sql(spotquery,conn)
    conn.close()
    return spotdf

def check_signal(df):
    df = df.sort_values(by=['Date','Time']).reset_index(drop=True)
    df['PreviousClose'] = df['Close'].shift(1)
    df['PreviousClose'] = np.where(df['PreviousClose'].isnull(),df['Close'],df['PreviousClose'])
    dFsignal = df[df['Time'] < entrytime].sort_values(by='Date').reset_index(drop=True)
    dFsignal['CumTimeCount'] = dFsignal.groupby('Date')['Time'].cumcount()
    dFsignal['GapSignal'] = np.where((dFsignal['CumTimeCount'] == 0) & (dFsignal['PreviousClose'] < dFsignal['Close']), 1,
    np.where((dFsignal['CumTimeCount'] == 0) & (dFsignal['PreviousClose'] > dFsignal['Close']), -1, 0))
    def get_signal(group):
        idx_min = group.index.min()
        idx_max = group.index.max()
        close_min = group.loc[idx_min, 'Close']
        close_max = group.loc[idx_max, 'Close']
        gap_signal = group.loc[group['CumTimeCount'] == 0, 'GapSignal'].iloc[0]  # Get GapSignal for the group
        
        if (close_max > close_min) and (gap_signal == 1):
            return 1
        elif (close_min > close_max) and (gap_signal == -1):
            return -1
        else:
            return 0
    signal_map = dFsignal.groupby('Date').apply(get_signal).to_dict()
    dFsignal['Signal'] = dFsignal['Date'].map(signal_map)
    dFsignal.to_csv("dFsignal.csv",index=False)
    return dFsignal[dFsignal['Signal'] != 0][['Date','Signal']].drop_duplicates()

def merge_and_signal_generator(optdf,dfSignal):
    mergedDF = pd.merge(optdf,dfSignal,on='Date',how='inner',suffixes=['','_Signal'])
    mergedDF['Side'] = np.where(mergedDF['Signal'] == 1,'CE','PE')
    mergedDF = mergedDF[mergedDF['Call_Or_Put'] == mergedDF['Side']]
    return mergedDF.drop_duplicates()

def strike_selector(df,spotdf,entrytime):
    spotdf = spotdf[spotdf['Time'] == entrytime]
    spotdf['Date'] = pd.to_datetime(spotdf['Date']).dt.date
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df = pd.merge(df,spotdf[['Date','Time','Close']],on=['Date','Time'],how='left',suffixes=['','_EQ'])
    df['Difference'] = df['StrikePrice'].astype(float) - df['Close_EQ']
    df['Difference'] = df['Difference'].abs()
    df = df.dropna(subset = ['Close_EQ'])
    result_df = df.loc[df.groupby(['Date','Call_Or_Put'])['Difference'].idxmin()]
    return result_df[['Ticker','Date','Difference']]

def strategySignal_generator(df,strikeselectordf,entrytime,exittime):
    data = pd.merge(df,strikeselectordf[['Ticker','Date','Difference']],on=['Ticker','Date'],how='inner')
    data = data[(data['Time'] >= entrytime) & (data['Time'] <= exittime)]
    data['CumTimeCount'] = data.groupby(['Date', 'Ticker']).cumcount()
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == 0), 'Sell', '')
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == data.groupby(['Date', 'Ticker'])['CumTimeCount'].transform('max')), 'Buy', data['BuySellFlag'])
    data['EntryPrice'] = np.where(data['BuySellFlag'] == 'Sell', data['Close'], np.nan)
    data['EntryPrice'] = data.groupby(['Date', 'Ticker'])['EntryPrice'].transform('last')
    data['Stoploss'] = np.where((data['High'] >= (data['EntryPrice'] * (1 + stoploss))) & (data["CumTimeCount"] != 0), 'Triggered', '')
    data['StoplossPrice'] = np.where(data['Stoploss'] == 'Triggered', (data['EntryPrice'] * (1 + stoploss)), '')
    data['stoploss_rn'] = data.groupby(['Date', 'Ticker', 'Stoploss'])['Time'].rank().astype(int)
    data.loc[(data['stoploss_rn'] != 1) | (data['Stoploss'] != "Triggered"), 'stoploss_rn'] = np.nan
    data['BuySellFlag'] = np.where(data["stoploss_rn"] == 1, 'Buy', data['BuySellFlag'])
    signals_df = data[(data["BuySellFlag"] == "Buy") | (data["BuySellFlag"] == "Sell")]
    signals_df['buysell_rn'] = signals_df.groupby(['Date', 'Ticker', 'BuySellFlag'])['Time'].rank().astype(int)
    signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
    signals_df.loc[signals_df["Stoploss"] == "Triggered", "sl_flag"] = "hard"
    signals_df["Position"] = 1
    return signals_df

def tradebook_new(signals_df):
    commission = 1
    brokerage = 0
    signals_df = signals_df[['Ticker', 'Date', 'Time', 'Close', 'Call_Or_Put', 'BuySellFlag', 'Stoploss', 'StoplossPrice','sl_flag', "Position",'StrikePrice']]
    entry_df = signals_df[signals_df["BuySellFlag"] == "Sell"].rename(columns = {"Time": "EntryTime", "Close": "EntryPrice"}).drop(["BuySellFlag","Stoploss", "StoplossPrice", "sl_flag"], axis = 1)
    exit_df = signals_df[signals_df["BuySellFlag"] == "Buy"].rename(columns = {"Time": "ExitTime", "Close": "ExitPrice"}).drop(["BuySellFlag", "Call_Or_Put","StrikePrice"], axis = 1)
    exit_df.loc[exit_df["Stoploss"] == "Triggered", "ExitPrice"] = pd.to_numeric(exit_df["StoplossPrice"], errors='coerce')
    exit_df = exit_df.drop(["StoplossPrice"], axis = 1)
    tradebook = pd.merge(entry_df, exit_df, on = ["Ticker", "Date", "Position"], how = "left")
    tradebook["PositionType"] = 'Short'
    tradebook['GrossPnL'] = tradebook['EntryPrice'] - tradebook['ExitPrice']
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['StrikePrice']) * 100,2)
    tradebook['Final_PnL%'] = tradebook['PnL%'] - (((brokerage*2) / (tradebook['StrikePrice'] * 15))*100)
    tradebook = tradebook.sort_values(["Date", "EntryTime"])
    return tradebook

def backtest():
    optdf = fetch_options(index,startdate,enddate,entrytime,exittime)
    spotdf = fetch_spot_data(index,startdate,enddate)
    dfSignal = check_signal(spotdf.copy())
    mergedDF = merge_and_signal_generator(optdf.copy(),dfSignal.copy())
    # mergedDF.to_csv("mergedDF.csv",index=False)
    atmDF = strike_selector(mergedDF.copy(),spotdf.copy(),entrytime)
    # atmDF.to_csv("atmDF.csv",index=False)
    strategySignalDF = strategySignal_generator(mergedDF.copy(),atmDF.copy(),entrytime,exittime)
    # strategySignalDF.to_csv("signaldf.csv",index=False)
    tradebook = tradebook_new(strategySignalDF.copy())
    tradebook.to_csv(fr"Tradebook_{startdate}_{enddate}.csv",index=False)

if __name__ == "__main__":
    backtest()

