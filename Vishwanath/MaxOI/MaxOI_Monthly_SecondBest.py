import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import time
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
import re

startdate = '2024-11-01'
enddate = '2025-02-25'
index = 'BANKNIFTY'
instrument = 'OPTIDX'
timeframe = 'MONTHLY'
stoploss = 50
data_timeframe = 15
price_filter = 50
consider_price_filter = 'Y'

def connection():
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    return connection

def read_spotdata():
    spot_query = f'''SELECT * FROM spotdata WHERE "Symbol" = '{index.upper()}' AND "Date" >= '{startdate}' AND "Date" <= '{enddate}' order by "Date", "Time";'''
    print(f"Fetching {index.capitalize()} spot data..")
    conn = connection()
    spotdf = pd.read_sql(spot_query,conn)
    conn.close()
    return spotdf

def fetch_expiry():
    expiry_query = f'''SELECT * FROM nseexpiry WHERE "SYMBOL" = '{index}' AND "INSTRUMENT" = '{instrument}' AND "{timeframe}" = 1 ;'''
    print(f"Fetching {index.capitalize()} {timeframe.capitalize()} Expiry..")
    conn = connection()
    expdf = pd.read_sql(expiry_query,conn)
    conn.close()
    return expdf

def convert_data(df,data_timeframe,week):
    print(f'Converting data for {week}..')
    df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    df.set_index('Datetime',inplace=True)
    df_min = df.groupby(['StrikePrice','Call_Or_Put']).resample(f'{data_timeframe}T').agg({
    'Ticker': 'first','Open': 'first','High': 'max','Low': 'min','Close': 'last','Volume': 'sum','Open Interest': 'last','Label':'first'})
    df_min.reset_index(inplace=True)
    df_min.dropna(inplace=True)
    df_min['Date'] = df_min['Datetime'].dt.date
    df_min['Time'] = df_min['Datetime'].dt.time
    df_min = df_min[['Ticker','Date','Time','Open','High','Low','Close','Volume','Open Interest','StrikePrice','Call_Or_Put','Label']]
    return df_min

def fetch_opt_data(startdate,enddate):
    weeks = ['M1','M2']
    final_df = pd.DataFrame()
    for week in weeks:
        sql_query = f'''
            SELECT *
            FROM monthly_gdfl_min_opt
            WHERE "Label" = '{week}'
            AND "Name" = '{index.upper()}'
            and "Date" >= '{startdate}' and "Date" <= '{enddate}';  
        '''
        print("Fetching options data ..")
        conn = connection()
        df = pd.read_sql(sql_query,conn)
        # df.to_pickle(rf"C:\Vishwanath\PythonCodes\Strategy\MaxOI\RawFiles\\df_monthlymax_{week}_{startdate}-{enddate}.pkl",protocol=4)
        df_15min = convert_data(df.copy(),data_timeframe,week)
        final_df = pd.concat([final_df,df_15min],ignore_index=True)
    conn.close()
    return final_df

def get_max_oi_side(df,expirydates):
    time_till_maxoi = '15:00:00'
    pcr_df = df[df['Time'] == datetime.strptime(time_till_maxoi,'%H:%M:%S').time()]
    pcr_df['Flag'] = np.where(pcr_df['Date'].isin(expirydates['DATE']),1,0)
    pcr_df = pcr_df[((pcr_df['Flag']==0) & (pcr_df['Label']=='M1')) | ((pcr_df['Flag']==1) & (pcr_df['Label']=='M2'))]
    pcr_df['OI_Sum'] = pcr_df.groupby(['Date','Call_Or_Put'])['Open Interest'].transform('sum')
    return pcr_df.drop_duplicates(subset=['Date','Call_Or_Put']).sort_values(by=['Date','Time'])

def compute_pcr(group):
    pe_sum = group.loc[group['Call_Or_Put'] == 'PE', 'OI_Sum'].sum()
    ce_sum = group.loc[group['Call_Or_Put'] == 'CE', 'OI_Sum'].sum()
    return pe_sum / ce_sum

def get_entry(df,maxoi_signals):
    time_to_check_maxoi_strike = '15:15:00'
    optdf = pd.merge(df,maxoi_signals,on=['Date'],how='inner')
    optdf['LabelToSelect'] = np.where(optdf['Flag']==1,'M2','M1')
    optdf = optdf[(optdf['Label']==optdf['LabelToSelect']) & (optdf['Time'] == datetime.strptime(time_to_check_maxoi_strike,'%H:%M:%S').time()) & (optdf['Call_Or_Put']==optdf['Optiontochoose'])].sort_values(by=['Date','Time']).reset_index(drop=True)
    max_open_interest = optdf.groupby('Date')['Open Interest'].transform('max')
    result = optdf[optdf['Open Interest'] == max_open_interest]
    return result

def find_first_digit_index(s):
    match = re.search(r'\d', s)
    return match.start() if match else np.nan

def check_strike_to_enter(df,optdf):
    df['First_Digit_Index'] = df['Ticker'].apply(find_first_digit_index)
    df['ContractExpiry'] = pd.to_datetime(df.apply(lambda row: row['Ticker'][row['First_Digit_Index']:16], axis=1),format='%d%b%y',dayfirst=True)
    df['Date'] = pd.to_datetime(df['Date'],format='mixed',dayfirst=True)
    df['DTE'] = (df['ContractExpiry'] - df['Date']).dt.days
    df['ContractToSelect'] = np.where((df['DTE'] == 1) | (df['Flag'] == 1),'M2','M1')
    optdf['Date'] = pd.to_datetime(optdf['Date'],format='mixed',dayfirst=True)
    entrydf = pd.merge(df[['Date','Time','ContractExpiry','StrikePrice','Call_Or_Put','ContractToSelect']],optdf,left_on=['Date','Time','StrikePrice','Call_Or_Put','ContractToSelect'],right_on=['Date','Time','StrikePrice','Call_Or_Put','Label'],how='inner')
    entrydf['First_Digit_Index'] = entrydf['Ticker'].apply(find_first_digit_index)
    entrydf['ContractExpiry'] = pd.to_datetime(entrydf.apply(lambda row: row['Ticker'][row['First_Digit_Index']:16], axis=1),format='%d%b%y',dayfirst=True)
    return entrydf

def check_strikes_for_price(df):
    df_with_entrysignals = df[['Date','Time','ContractExpiry','StrikePrice','Call_Or_Put','Ticker','Open']].sort_values(by='Date').rename(columns={'Open':'EntryPrice'}).reset_index(drop=True)
    df_with_entrysignals['ExitDate'] = df_with_entrysignals['Date'].shift(-1)
    df_with_entrysignals.loc[0,'UniqueFlag'] = 1
    for i in range(1,len(df_with_entrysignals)):
        if df_with_entrysignals.loc[i,'Ticker'] == df_with_entrysignals.loc[i-1,'Ticker']:
            continue
        else:
            df_with_entrysignals.loc[i,'UniqueFlag'] = 1
    df_with_entrysignals = df_with_entrysignals.dropna(subset='UniqueFlag')
    df_with_entrysignals['ExitDate'] = df_with_entrysignals['Date'].shift(-1)
    df_with_entrysignals = df_with_entrysignals[(df_with_entrysignals['EntryPrice'] >= price_filter)] if consider_price_filter == 'Y' else df_with_entrysignals
    return df_with_entrysignals.drop(columns='UniqueFlag')

def check_stoploss(signals_df,df,stoploss):
    df['Date'] = pd.to_datetime(df['Date'])
    data = pd.merge(df,signals_df[['Ticker','Date','Time','ContractExpiry','EntryPrice','ExitDate']],on=['Ticker'],how='inner',suffixes=['','_tocheck'])
    data = data[(data['Date'] >= data['Date_tocheck']) & (data['Date'] <= data['ExitDate'])].sort_values(by=['Date','Time']).reset_index(drop=True)
    data['EntryTime_adjusted'] = np.where(data['Date'] == data['Date_tocheck'], data['Time'] >= data['Time_tocheck'], False)
    data['ExitTime_adjusted'] = np.where(data['Date'] == data['ExitDate'], data['Time'] <= data['Time_tocheck'], False)
    data = data[((data['EntryTime_adjusted']==True) | (data['ExitTime_adjusted']==True)) | ((data['Date'] > data['Date_tocheck']) & (data['Date'] < data['ExitDate']))].drop(columns=['EntryTime_adjusted','ExitTime_adjusted']).sort_values(by=['Date','Time','ExitDate']).reset_index(drop=True)
    data.loc[0,'Count'] = 1
    for i in range(1,len(data)):
        if data.loc[i,'Ticker'] == data.loc[i-1,'Ticker']:
            data.loc[i,'Count'] = data.loc[i-1,'Count']
        else:
            data.loc[i,'Count'] = data.loc[i-1,'Count'] + 1
    data['CumTimeCount'] = data.groupby(['Count']).cumcount()
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == 0), 'Sell', '')
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == data.groupby(['Count'])['CumTimeCount'].transform('max')), 'Buy', data['BuySellFlag'])
    data['Stoploss'] = np.where((data['High'] >= data['EntryPrice'] * (1+stoploss/100)) & (data['CumTimeCount'] !=0),'Triggered','')
    data['StoplossPrice'] = np.where((data['High'] >= (data['EntryPrice'] * (1 + stoploss/100))) & (data["CumTimeCount"] != 0), (data['EntryPrice'] * (1 + stoploss/100)), '')
    data['stoploss_rn'] = data.groupby(['Count', 'Stoploss'])['Time'].rank().astype(int)
    data.loc[(data['stoploss_rn'] != 1) | (data['Stoploss'] != "Triggered"), 'stoploss_rn'] = np.nan
    data['BuySellFlag'] = np.where(data["stoploss_rn"] == 1, 'Buy', data['BuySellFlag'])
    signals_df = data[(data["BuySellFlag"] == "Buy") | (data["BuySellFlag"] == "Sell")]
    signals_df['buysell_rn'] = signals_df.groupby(['Count', 'BuySellFlag'])['Time'].rank().astype(int)
    signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
    return signals_df

def convert_eq(df,timeframe):
    df.columns = df.columns.str.capitalize()
    df['Date'] = pd.to_datetime(df['Date'])
    df['Time'] = df['Date'].dt.time
    df['Date'] = df['Date'].dt.date
    df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + " " + df['Time'].astype(str))
    df.set_index('Datetime',inplace=True)
    df_5min = df.resample(f'{timeframe}T').agg({
        'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
    })
    df_5min['Date'] = df_5min.index.date
    df_5min['Time'] = df_5min.index.time
    df_5min.reset_index(drop=True,inplace=True)
    df_5min = df_5min[['Date','Time','Open','High','Low','Close','Volume']]
    df_5min = df_5min.dropna(subset=['Close'])
    return df_5min

def tradebook_generator(signals_df,eqdf):
    eqdf['Date'] = pd.to_datetime(eqdf['Date'])
    commission = 1
    brokerage = 6
    signals_df = signals_df[['Ticker', 'Date', 'Time', 'Open', 'Call_Or_Put', 'BuySellFlag', 'Stoploss', 'StoplossPrice','Count','EntryPrice']]
    entry_df = signals_df[signals_df["BuySellFlag"] == "Sell"].rename(columns = {"Date":"EntryDate","Time": "EntryTime"}).drop(["BuySellFlag","Stoploss", "StoplossPrice",'Open'], axis = 1)
    exit_df = signals_df[signals_df["BuySellFlag"] == "Buy"].rename(columns = {"Date":"ExitDate","Time": "ExitTime", "Open": "ExitPrice"}).drop(["BuySellFlag", "Call_Or_Put",'EntryPrice'], axis = 1)
    exit_df.loc[exit_df["Stoploss"] == "Triggered", "ExitPrice"] = pd.to_numeric(exit_df["StoplossPrice"], errors='coerce')
    exit_df = exit_df.drop(["StoplossPrice"], axis = 1)
    tradebook = pd.merge(entry_df, exit_df, on = ["Ticker", "Count"], how = "left")
    tradebook["PositionType"] = 'Short'
    tradebook = pd.merge(tradebook,eqdf[['Date','Time','Open']],left_on=['EntryDate','EntryTime'],right_on=['Date','Time'],how='left').rename(columns={'Open':'Close_EQ'}).drop(columns=['Date','Time'])
    tradebook['GrossPnL'] = tradebook['EntryPrice'] - tradebook['ExitPrice']
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['Close_EQ']) * 100,2)
    tradebook['Final_PnL%'] = tradebook['PnL%'] - (((brokerage*2) / (tradebook['Close_EQ'] * 15))*100)
    tradebook = tradebook.sort_values(["EntryDate", "EntryTime"])
    return tradebook


df_15 = fetch_opt_data(startdate,enddate)
spotdata = read_spotdata()
expdf = fetch_expiry()
monthlyexpdf = expdf[expdf['MONTHLY']==1]
pcr_df = get_max_oi_side(df_15.copy(),expdf.copy())
pcr_df['PCR'] = pcr_df.groupby('Date').apply(lambda group: compute_pcr(group)).reindex(pcr_df['Date']).values
pcr_df['Optiontochoose'] = pcr_df.groupby('Date')['PCR'].transform(lambda x: 'CE' if x.mean() < 1 else 'PE')
pcr_df_signals = pcr_df[['Date','Flag','Optiontochoose']].drop_duplicates(subset=['Date'])
pcr_df_signals['Flag'] = np.where(pcr_df_signals['Date'].isin(expdf['DATE']),1,0)
maxoi_strikes = get_entry(df_15.copy(),pcr_df_signals.copy())
entrydf = check_strike_to_enter(maxoi_strikes.copy(),df_15.copy())
entrysignals_df = check_strikes_for_price(entrydf.copy())
signals_df = check_stoploss(entrysignals_df.copy(),df_15.copy(),stoploss)
equity_5min = convert_eq(spotdata.copy(),5)
tradebook = tradebook_generator(signals_df.copy(),equity_5min.copy())
print(tradebook['PnL_after_commission'].sum())
