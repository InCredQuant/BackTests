import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta, time as dt_time
import time
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")

index = 'NIFTY'
startdate = '2010-01-01' if index != 'MIDCPNIFTY' else '2023-01-01'
enddate = '2025-05-25'
index_name = {'NIFTY': 'NF', 'BANKNIFTY': 'BN', 'MIDCPNIFTY': 'MN'}
filename = index_name[index]
filepath = fr"C:\Vishwanath\PythonCodes\Strategy\ChaitanyaStrategy\Double 7"
tradebook_path = os.path.join(filepath,f"Tradebook\{index.capitalize()}\{datetime.today().date()}")
os.makedirs(tradebook_path, exist_ok=True)
nav_path = os.path.join(filepath,f"NAV\{index.capitalize()}\{datetime.today().date()}")
os.makedirs(nav_path, exist_ok=True)
timeframe = 'B'
ma_period = 200
# window_for_checking_highs_lows = 5
window_for_checking_highs_lows = [3,4,5,6,7]
# window_for_checking_highs_lows = [8]

def expirydata_fetcher(filepath):
    expquery = f''' 
        SELECT * FROM nseexpiry; '''
    
    conn = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    expdf = pd.read_sql(expquery,conn)
    full_path = os.path.join(filepath, f"{filename}_Expdf.pkl")
    expdf.to_pickle(full_path, protocol=4)

def format_spotdata(spotdf):
    spotdf['time'] = pd.to_datetime(spotdf['date']).dt.time
    spotdf['date'] = pd.to_datetime(spotdf['date']).dt.date
    spotdf['symbol'] = index
    spotdf.columns = map(str.capitalize, spotdf.columns)
    spotdf = spotdf[['Symbol','Date','Time','Open','High','Low','Close','Volume']]
    return spotdf
    
def convert_to_daily_data(df,timeframe):
    df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + " " + df['Time'])
    df.set_index('Datetime',inplace=True)
    df_converted = df.resample(f'{timeframe}T').agg({
        'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
    }) if timeframe.upper() != 'B' else df.resample(f'{timeframe.upper()}').agg({
        'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
    })
    df_converted['Date'] = df_converted.index.date
    df_converted['Time'] = df_converted.index.time
    df_converted.reset_index(drop=True,inplace=True)
    df_converted = df_converted[['Date','Time','Open','High','Low','Close','Volume']]
    df_converted = df_converted.dropna(subset=['Close'])
    return df_converted

def calculate_sma(df,period):
    df[f'{period}_SMA'] = df['Close'].rolling(window=period).mean()
    return df

def extract_trades(df, period=50,rolling_window=7):
    print(f"Running extract_trades with parameters: period = {period}, rolling_window = {rolling_window}")
    df = df.reset_index(drop=True)
    df['7_Day_Rolling_Low'] = df['Close'].rolling(window=rolling_window, min_periods=1).min()
    df['7_Day_Rolling_High'] = df['Close'].rolling(window=rolling_window, min_periods=1).max()
    df[f'{period}_SMA'] = df['Close'].rolling(window=period).mean()
    trades = []
    in_trade = False
    entry_price = None
    entry_date = None
    
    for i in range(len(df)):
        if not in_trade and df.loc[i, 'Close'] > df.loc[i, f'{period}_SMA'] and df.loc[i, 'Close'] == df.loc[i, '7_Day_Rolling_Low']:
            entry_price = df.loc[i, 'Close']
            entry_date = df.loc[i,'Date']
            in_trade = True
    
        elif in_trade and (df.loc[i, 'Close'] < df.loc[i, f'{period}_SMA'] or df.loc[i, 'Close'] == df.loc[i, '7_Day_Rolling_High']):
            exit_price = df.loc[i, 'Close']
            exit_date = df.loc[i,'Date']
            trades.append({
                'Entry Date': entry_date,
                'Entry Price': entry_price,
                'Exit Date': exit_date,
                'Exit Price': exit_price
            })
            in_trade = False
    return pd.DataFrame(trades)

def get_future_prices(f1_,trades):
    f1_ = f1_.rename(columns={'PX_LAST':'Close'})
    entrydf_dates,exitdf_dates = trades['Entry Date'],trades['Exit Date']
    entrydf = f1_[f1_['Date'].isin(entrydf_dates)].sort_values('Date').reset_index(drop=True)
    exitdf = f1_[f1_['Date'].isin(exitdf_dates)].sort_values('Date').reset_index(drop=True)
    tradedf = pd.merge(entrydf[['Date','Close']],exitdf[['Date','Close']],left_on=entrydf.index,right_on=exitdf.index,how='inner',suffixes=['_Entry','_Exit'])
    tradedf['PnL'] = tradedf['Close_Exit'] - tradedf['Close_Entry']
    tradedf['PnL_withcosts'] = (tradedf['Close_Exit'] * 0.9997) - (tradedf['Close_Entry'] * 1.0003)
    tradedf['PnL%'] = round(((tradedf['PnL_withcosts'] / tradedf['Close_Entry']) * 100),3)
    return tradedf

def get_daily_pnl(df,futdf):
    daily_pnl = pd.DataFrame()
    for i in range(len(df)):
        futdf = futdf.rename(columns={'PX_LAST':'Close'})
        futdf_ = futdf[(futdf['Date'] >= df.loc[i,'Date_Entry']) & (futdf['Date'] <= df.loc[i,'Date_Exit'])][['Date','Close']]
        futdf_['PrevClose'] = futdf_['Close'].shift(1)
        futdf_['PnL'] = np.where(futdf_['PrevClose'].isnull(),0,futdf_['Close'] - futdf_['PrevClose'])
        # display(futdf_)
        daily_pnl = pd.concat([daily_pnl,futdf_],ignore_index=True)
    return daily_pnl

def get_nav(daily_pnl):
    daily_pnl['PnL%'] = round((daily_pnl['PnL'] / daily_pnl['Close']) * 100,3)
    daily_pnl['NAV'] = round((np.cumprod(1 + daily_pnl['PnL%'] / 100)) * 100,3)
    return daily_pnl[['Date','NAV']]

def get_unique_dates(spotdf,navdf):
    uniquedates = spotdf[['Date']]
    uniquedates['Date'] = pd.to_datetime(uniquedates['Date'])
    navdf['Date'] = pd.to_datetime(navdf['Date'])
    uniquedates = pd.merge(uniquedates,navdf[['Date','NAV']],on='Date',how='left')
    uniquedates = uniquedates[uniquedates['Date'] >= uniquedates[uniquedates['NAV']==100]['Date'].min()]
    uniquedates['NAV'] = uniquedates['NAV'].bfill()
    return uniquedates


spotdf = pd.read_csv(rf"G:\Shared drives\BackTests\Spot Data 1min\NIFTY 50_day.csv")
spotdf_ = format_spotdata(spotdf)
f1_ = pd.read_excel(rf"G:\Shared drives\BackTests\Vishwanath\Nifty_Futures_EOD.xlsx")
# f1_ = convert_to_daily_data(f1,timeframe)
f1_['Date'] = pd.to_datetime(f1_['Date']).dt.date
spotdf_['Date'] = pd.to_datetime(spotdf_['Date']).dt.date
f1_ = f1_[f1_['Date'] >= pd.to_datetime(startdate).date()].sort_values('Date').reset_index(drop=True)
spotdf_ = spotdf_[(spotdf_['Date'] >= pd.to_datetime(startdate).date()) & (spotdf_['Date'] <= pd.to_datetime(enddate).date())].sort_values('Date').reset_index(drop=True)
spotdf_ = calculate_sma(spotdf_.copy(),ma_period)

for window in window_for_checking_highs_lows:
    trades = extract_trades(spotdf_,ma_period,window)
    tradebook = get_future_prices(f1_.copy(),trades.copy())
    print(tradebook['PnL_withcosts'].sum())
    tradebook.to_csv(os.path.join(tradebook_path,f"Tradebook_Period_{ma_period}_Window_{window}.csv"),index=False)
    daily_pnl = get_daily_pnl(tradebook.copy(),f1_.copy())
    navdf = get_nav(daily_pnl.copy())
    finalnav = get_unique_dates(spotdf_,navdf)
    finalnav.to_excel(os.path.join(nav_path,f"NAV_Period_{ma_period}_Window_{window}.xlsx"))