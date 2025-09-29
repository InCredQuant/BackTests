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
startdate = '2023-01-01'
enddate = '2025-04-25'
entrytime = '15:25:59'
exittime = '09:16:59'
tradetype = 'Short'
strikes_itm = 3
strike_multiplier = {'NIFTY':50,'BANKNIFTY':100,'MIDCPNIFTY':25}
multiplier = strike_multiplier[index]

def get_weeklyexpiry_dates(index,startdate):
    expiryquery = f'''
                    SELECT * FROM nseexpiry WHERE "WEEKLY" = 1 and "SYMBOL" = '{index.upper()}'
                    and "DATE" >= '{startdate}';
                    '''
    conn = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    expdf = pd.read_sql(expiryquery,conn).sort_values(by='DATE')
    conn.close()
    return expdf

def fetch_options(index,startdate,enddate,entrytime,exittime):
    opt_query = f'''
                SELECT * FROM gdfl_min
                where "Date" >= '{startdate}' and "Date" <= '{enddate}'
                AND ("Time" = '{entrytime}' OR "Time" = '{exittime}')
                AND "Name" = '{index.upper()}' and "Ticker" not like '%-I%';
                '''
    print(opt_query)
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

def convert_eq_to_daily_data(equitydf):
    equitydf['Datetime'] = pd.to_datetime(equitydf['Date'].astype(str) + ' ' + equitydf['Time'].astype(str))
    equitydf = equitydf.sort_values('Datetime').reset_index(drop=True)
    daily_groups = equitydf.groupby(equitydf['Date'])
    daily_data = pd.DataFrame({
        'Date': daily_groups['Date'].first(),'Open': daily_groups['Open'].first(),'High': daily_groups['High'].max(),
        'Low': daily_groups['Low'].min(),'Close': daily_groups['Close'].last(),'Volume': daily_groups['Volume'].sum()
    })
    daily_data['Date'] = pd.to_datetime(daily_data['Date'])
    return daily_data.reset_index(drop=True)

def merge_spot(df,eqdf):
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['Time'] = pd.to_datetime(df['Time']).dt.date
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    eqdf['Time'] = pd.to_datetime(eqdf['Time']).dt.date
    optdf = pd.merge(df,eqdf[['Date','Time','Close']],on=['Date','Time'],how='left',suffixes=['','_EQ'])
    return optdf

def ticker_change(df,flag='N'):
    if index == 'BANKNIFTY':
        df['Ticker'] = df['Ticker'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
    elif index == 'NIFTY':
        df['Ticker'] = df['Ticker'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23')
    return df

def expiry_changes(expirydf):
    if index == 'BANKNIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('30MAR23','29MAR23').str.replace('29JUN23', '28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-03-30').date()) | (expirydf['DATE'] == pd.to_datetime('2023-06-29').date()) | (expirydf['DATE'] == pd.to_datetime('2024-03-28').date()) | (expirydf['DATE'] == pd.to_datetime('2024-04-25').date()) | (expirydf['DATE'] == pd.to_datetime('2024-06-27').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    elif index == 'NIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('29JUN23', '28JUN23').str.replace('29MAR23','30MAR23')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-03-30').date()) | (expirydf['DATE'] == pd.to_datetime('2023-06-29').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    return expirydf.drop_duplicates()

def define_candlecolour(df):
    df_range = df[(df['Time'] == '09:15:59') | (df['Time']==entrytime)].sort_values(by=['Date','Time']).reset_index(drop=True)
    df_range['DayOpen'] = df_range.groupby('Date')['Open'].shift(1)
    df_range = df_range.dropna(subset='DayOpen')
    if tradetype == 'Long':
        df_range['Side'] = np.where(df_range['Close'] > df_range['DayOpen'],'PE','CE')
    elif tradetype == 'Short':
        df_range['Side'] = np.where(df_range['Close'] > df_range['DayOpen'],'CE','PE')
    return df_range

def filter_weekly_contracts(df):
    df['ExpiryDate'] = pd.to_datetime(df['ExpiryDate'])
    mask = df['ExpiryDate'] == df.groupby('Date')['ExpiryDate'].transform('min')
    weeklydf = df[mask].sort_values(by='Date').reset_index(drop=True)
    return weeklydf

def filter_next_weekly_on_expiryday(optdf,expdf):
    optdf_ = optdf[optdf['Date'].isin(expdf['DATE'])]
    optdf_['ExpiryDate'] = pd.to_datetime(optdf_['ExpiryDate'])
    expdf = expdf.sort_values(by='DATE')
    expdf['ShiftedDate'] = expdf['DATE'].shift(-1)
    expdf['ShiftedDate'] = pd.to_datetime(expdf['ShiftedDate'])
    df = pd.merge(optdf_,expdf[['DATE','ShiftedDate']],left_on=['Date','ExpiryDate'],right_on=['DATE','ShiftedDate'],how='inner')
    return df

def strikeselector(df):
    df['Difference'] = df['StrikePrice'].astype(float) - df['Close_EQ']
    df['Difference'] = df['Difference'].abs()
    df = df.dropna(subset = ['Close_EQ'])
    result_df = df.loc[df.groupby(['Date','Time', 'Call_Or_Put'])['Difference'].idxmin()]
    return result_df

def get_entry(optdf,signalsdf):
    entrydf = optdf[optdf['Time'] == entrytime].reset_index(drop=True)
    mergeddf = pd.merge(entrydf,signalsdf[['Date','Time','Close','Side']],on=['Date','Time'],how='inner',suffixes=['','_EQ'])
    atmdf = strikeselector(mergeddf.copy())
    entrydf = atmdf[atmdf['Call_Or_Put'] == atmdf['Side']].reset_index(drop=True)
    return entrydf

def get_itm_strikes(df):
    df['ITM_Strike'] = np.where(df['Side'] == 'CE',df['StrikePrice'].astype(float) - (strikes_itm * multiplier),df['StrikePrice'].astype(float) + (strikes_itm * multiplier))
    itm_strikes = df[['Ticker','Date','Time','StrikePrice','Side','ITM_Strike','Close_EQ']].copy()
    itm_strikes['Ticker'] = itm_strikes.apply(lambda row: row['Ticker'].replace(str(int(row['StrikePrice'])), str(int(row['ITM_Strike']))), axis=1)
    itm_strikes['StrikePrice'] = itm_strikes['ITM_Strike']
    itm_strikes = itm_strikes[['Ticker','Date','Time','StrikePrice','Side','Close_EQ']]
    return itm_strikes
    
def get_exit(df,rawdf):
    df = df.sort_values(by='Date').reset_index(drop=True)
    df['StrikePrice'] = df['StrikePrice'].astype(float)
    df['ExitDate'] = df.groupby('Date')['Date'].transform(lambda x: df['Date'].unique()[np.where(df['Date'].unique() == x.iloc[0])[0][0] + 1] if x.iloc[0] != df['Date'].unique()[-1] else None)
    df['ExitTime'] = exittime
    exitdf = df[['Ticker','ExitDate','ExitTime']]
    exitdfprices = pd.merge(rawdf,exitdf,left_on=['Ticker','Date','Time'],right_on=['Ticker','ExitDate','ExitTime'],how='inner').sort_values(by=['Date','Time'])
    return exitdfprices

def tradebook(entrydf,exitdf,period='Weekly'):
    commission = 1 if period == 'Weekly' else 0.5
    entrydf = entrydf[['Ticker','Date','Time','Close','Close_EQ','TradeType']].rename(columns={'Date':'EntryDate','Time':'EntryTime','Close':'EntryPrice'}).reset_index(drop=True)
    entrydf['Exit'] = entrydf.groupby('EntryDate')['EntryDate'].transform(lambda x: entrydf['EntryDate'].unique()[np.where(entrydf['EntryDate'].unique() == x.iloc[0])[0][0] + 1] if x.iloc[0] != entrydf['EntryDate'].unique()[-1] else None)
    # entrydf['Exit'] = entrydf.sort_values(by='EntryDate')['EntryDate'].shift(-1)
    exitdf = exitdf[['Ticker','Date','Time','Close']].rename(columns={'Date':'ExitDate','Time':'ExitTime','Close':'ExitPrice'}).reset_index(drop=True)
    tradebook = pd.merge(entrydf,exitdf,left_on=['Ticker','Exit'],right_on=['Ticker','ExitDate'],how='inner')
    tradebook['GrossPnL'] = np.where(tradebook['TradeType'] == 'Short',tradebook['EntryPrice'] - tradebook['ExitPrice'],(tradebook['ExitPrice'] - tradebook['EntryPrice']) *2)
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['Close_EQ']) * 100,2)
    return tradebook

expdf = get_weeklyexpiry_dates(index,startdate)
expdf = expiry_changes(expdf)
optdf = fetch_options(index,startdate,enddate,entrytime,exittime)
eqdf = fetch_spot_data(index,startdate,enddate)
weeklydf = filter_weekly_contracts(optdf.copy())
weeklydf_ = weeklydf[~weeklydf['Date'].isin(expdf['DATE'])]
nextweekdf = filter_next_weekly_on_expiryday(optdf.copy(),expdf.copy())
df = pd.concat([weeklydf_,nextweekdf],ignore_index=True).sort_values(by='Date')
signalsdf = define_candlecolour(eqdf.copy())
entrydf = get_entry(df.copy(),signalsdf.copy())
entrydf_strikes = get_itm_strikes(entrydf.copy())
entrydf_itm_strikes_price = pd.merge(entrydf_strikes,df[['Ticker','Date','Time','Close']],on=['Ticker','Date','Time'],how='left')
entrydf_price = entrydf[['Ticker','Date','Time','Side','StrikePrice','Close','Close_EQ']]
entrydf_itm_strikes_price = entrydf_itm_strikes_price[['Ticker','Date','Time','Side','StrikePrice','Close','Close_EQ']]
entrydf_price['TradeType'] = tradetype
entrydf_itm_strikes_price['TradeType'] = 'Long' if tradetype == 'Short' else 'Short'
df_entry = pd.concat([entrydf_price,entrydf_itm_strikes_price],ignore_index=True).sort_values(by=['Date','Time'])
df_exit = get_exit(df_entry.copy(),optdf.copy())
tradesheet = tradebook(df_entry.copy(),df_exit.copy())

os.makedirs(fr"C:\Vishwanath\PythonCodes\Strategy\BTST\BuyingOnly\{index.capitalize()}\Tradebook",exist_ok=True)
tradesheet.to_csv(fr"C:\Vishwanath\PythonCodes\Strategy\BTST\BuyingOnly\{index.capitalize()}\Tradebook\\{index.capitalize()}_ZebraSpread_{entrytime.replace(':','')[:4]}-{exittime.replace(':','')[:4]}_{datetime.today().date()}.csv",index=False)
print("Backtest done")