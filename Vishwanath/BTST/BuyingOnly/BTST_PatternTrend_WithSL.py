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
startdate = '2019-02-14'
enddate = '2025-05-22'
entrytime = '15:25:59'
exittime = '09:17:59'
tradetype = 'Long'
stoploss_flag = 'N'
stoploss = 30

def define_entry_exit_dates(df):
    uniquedatesdf = pd.DataFrame(sorted(df['Date'].unique()),columns=['Date'])
    uniquedatesdf['EntryDate'] = uniquedatesdf['Date']
    uniquedatesdf['ExitDate'] = uniquedatesdf['Date'].shift(-1)
    return uniquedatesdf

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
                AND ("Time" = '{entrytime}' OR "Time" <= '{exittime}')
                AND "Name" = '{index.upper()}' and "Ticker" not like '%-I%';
                '''
    # print(opt_query)
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
        df_range['Side'] = np.where(df_range['Close'] > df_range['DayOpen'],'CE','PE')
    elif tradetype == 'Short':
        df_range['Side'] = np.where(df_range['Close'] > df_range['DayOpen'],'PE','CE')
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

# def get_exit(df,rawdf,entry_exit_dates):
    if stoploss_flag == 'N':
        df_merged = pd.merge(df,entry_exit_dates[['Date','ExitDate']],on='Date',how='inner')
        df_merged['ExitTime'] = exittime
        exitdf = df_merged[['Ticker','ExitDate','ExitTime']]
        exitdfprices = pd.merge(rawdf,exitdf,left_on=['Ticker','Date','Time'],right_on=['Ticker','ExitDate','ExitTime'],how='inner').sort_values(by=['Date','Time'])
    else:
        exittype = 'Buy' if tradetype == 'Short' else 'Sell'
        df_merged = pd.merge(df,entry_exit_dates[['Date','ExitDate']],on='Date',how='inner')
        exitdf = df_merged[['Ticker','ExitDate','Close']]
        # print(exitdf)
        exitdfprices = pd.merge(rawdf,exitdf,left_on=['Ticker','Date'],right_on=['Ticker','ExitDate'],how='inner',suffixes=['','_Entry']).sort_values(by=['Date'])
        exitdfprices = exitdfprices[exitdfprices['Time'] <= exittime].sort_values(by='Date').reset_index(drop=True)
        exitdfprices['CumCount'] = exitdfprices.groupby(['Date','Ticker'])['Time'].cumcount()
        # exitdfprices['BuySellFlag'] = np.where((exitdfprices['CumCount'] == 0), f'{tradetype}', '')
        exitdfprices['BuySellFlag'] = np.where((exitdfprices['CumCount'] == exitdfprices.groupby(['Date', 'Ticker'])['CumCount'].transform('max')), f'{exittype}', '')
        
        exitdfprices['Stoploss'] = np.where((tradetype == 'Long') & (exitdfprices['Open'] <= exitdfprices['Close_Entry'] * (1 - float(stoploss/100))) & (exitdfprices['CumCount'] == 0), 'Triggered' , 
        np.where((tradetype == 'Short') & (exitdfprices['Open'] >= exitdfprices['Close_Entry'] * (1 + float(stoploss/100))) & (exitdfprices['CumCount'] == 0), 'Triggered' , ''))
        
        exitdfprices['Stoploss'] = np.where((tradetype == 'Long') & (exitdfprices['Low'] <= exitdfprices['Close_Entry'] * (1 - float(stoploss/100))) & (exitdfprices['CumCount'] != 0), 'Triggered' , 
        np.where((tradetype == 'Short') & (exitdfprices['High'] >= exitdfprices['Close_Entry'] * (1 + float(stoploss/100))) & (exitdfprices['CumCount'] != 0), 'Triggered' , ''))
        
        exitdfprices['StoplossPrice'] = np.where((tradetype == 'Long') & (exitdfprices['Stoploss'] == 'Triggered'), (exitdfprices['Close_Entry'] * (1 - float(stoploss/100))), 
        np.where((tradetype == 'Short') & (exitdfprices['Stoploss'] == 'Triggered'), (exitdfprices['Close_Entry'] * (1 + float(stoploss/100))), ''))
        exitdfprices['stoploss_rn'] = exitdfprices.groupby(['Date', 'Ticker', 'Stoploss'])['Time'].rank().astype(int)
        exitdfprices.loc[(exitdfprices['stoploss_rn'] != 1) | (exitdfprices['Stoploss'] != "Triggered"), 'stoploss_rn'] = np.nan
        exitdfprices['BuySellFlag'] = np.where((tradetype == 'Long') & (exitdfprices["stoploss_rn"] == 1), 'Sell', 
        np.where((tradetype == 'Short') & (exitdfprices["stoploss_rn"] == 1), 'Buy', exitdfprices['BuySellFlag']))
        signals_df = exitdfprices[(exitdfprices["BuySellFlag"] == "Buy") | (exitdfprices["BuySellFlag"] == "Sell")]
        signals_df['buysell_rn'] = signals_df.groupby(['Date', 'Ticker', 'BuySellFlag'])['Time'].rank().astype(int)
        signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
        signals_df.loc[signals_df["Stoploss"] == "Triggered", "sl_flag"] = "hard"
        print(signals_df)
        exitdfprices.to_csv("exitdfprices.csv",index=False)
        xxxx
        
    return exitdfprices


def get_exit(df, rawdf, entry_exit_dates, stoploss_flag='N', exittime=None, 
             tradetype='Long', stoploss=0):
    """
    Calculate exit prices with proper entry/exit count matching
    """
    print(f"Debug - Input entry count: {len(df)}")
    print(f"Debug - Entry dates/tickers: {df[['Date', 'Ticker']].drop_duplicates().shape[0]}")
    
    # Simple exit without stop loss
    if stoploss_flag == 'N':
        df_merged = pd.merge(df, entry_exit_dates[['Date','ExitDate']], on='Date', how='inner')
        print(f"Debug - After merge with exit dates: {len(df_merged)}")
        
        df_merged['ExitTime'] = exittime
        exitdf = df_merged[['Ticker','ExitDate','ExitTime']]
        
        exitdfprices = pd.merge(
            rawdf, exitdf,
            left_on=['Ticker','Date','Time'],
            right_on=['Ticker','ExitDate','ExitTime'],
            how='inner'
        ).sort_values(by=['Date','Time'])
        
        print(f"Debug - Final exit count (no stoploss): {len(exitdfprices)}")
        return exitdfprices
    
    # Exit with stop loss logic
    exittype = 'Buy' if tradetype == 'Short' else 'Sell'
    
    # Merge and prepare data
    df_merged = pd.merge(df, entry_exit_dates[['Date','ExitDate']], on='Date', how='inner')
    print(f"Debug - After merge with exit dates: {len(df_merged)}")
    
    if len(df_merged) == 0:
        print("ERROR: No matches found when merging entries with exit dates!")
        return pd.DataFrame()
    
    exitdf = df_merged[['Ticker','ExitDate','Close']]
    
    exitdfprices = pd.merge(
        rawdf, exitdf,
        left_on=['Ticker','Date'],
        right_on=['Ticker','ExitDate'],
        how='inner',
        suffixes=['','_Entry']
    ).sort_values(by=['Date'])
    
    print(f"Debug - After merge with raw data: {len(exitdfprices)}")
    
    if len(exitdfprices) == 0:
        print("ERROR: No matches found when merging with raw price data!")
        print("Check if rawdf has data for the exit dates and tickers")
        return pd.DataFrame()
    
    # Check if exittime filter removes all data
    before_time_filter = len(exitdfprices)
    exitdfprices = exitdfprices[exitdfprices['Time'] <= exittime]
    after_time_filter = len(exitdfprices)
    
    print(f"Debug - Before time filter: {before_time_filter}, After: {after_time_filter}")
    
    if len(exitdfprices) == 0:
        print(f"ERROR: Time filter ({exittime}) removed all data!")
        return pd.DataFrame()
    
    # Filter by exit time and add cumulative count
    exitdfprices = (
        exitdfprices.sort_values(by=['Date', 'Time'])
        .reset_index(drop=True)
    )
    exitdfprices['CumCount'] = exitdfprices.groupby(['Date','Ticker'])['Time'].cumcount()
    
    # CRITICAL FIX: Ensure every Date/Ticker combination gets at least one exit signal
    # Set regular exit signal (last candle of the day for each Date/Ticker)
    max_cumcount = exitdfprices.groupby(['Date', 'Ticker'])['CumCount'].transform('max')
    exitdfprices['BuySellFlag'] = np.where(
        exitdfprices['CumCount'] == max_cumcount, 
        exittype, 
        ''
    )
    
    # Calculate stop loss threshold
    stoploss_decimal = float(stoploss / 100)
    if tradetype == 'Long':
        stoploss_threshold = exitdfprices['Close_Entry'] * (1 - stoploss_decimal)
    else:  # Short
        stoploss_threshold = exitdfprices['Close_Entry'] * (1 + stoploss_decimal)
    
    # Initialize Stoploss column
    exitdfprices['Stoploss'] = ''
    
    # STOP LOSS LOGIC:
    # 1. First candle (CumCount = 0): Check against Open price
    first_candle_mask = (exitdfprices['CumCount'] == 0)
    if tradetype == 'Long':
        first_candle_sl = first_candle_mask & (exitdfprices['Open'] <= stoploss_threshold)
    else:  # Short
        first_candle_sl = first_candle_mask & (exitdfprices['Open'] >= stoploss_threshold)
    
    exitdfprices.loc[first_candle_sl, 'Stoploss'] = 'Triggered'
    
    # 2. Subsequent candles (CumCount != 0): Check against Low/High prices
    subsequent_candles_mask = (exitdfprices['CumCount'] != 0)
    if tradetype == 'Long':
        subsequent_candles_sl = subsequent_candles_mask & (exitdfprices['Low'] <= stoploss_threshold)
    else:  # Short
        subsequent_candles_sl = subsequent_candles_mask & (exitdfprices['High'] >= stoploss_threshold)
    
    exitdfprices.loc[subsequent_candles_sl, 'Stoploss'] = 'Triggered'
    
    # Set stop loss price based on actual execution price
    exitdfprices['StoplossPrice'] = ''
    
    # For first candle stop loss triggers - use Open price
    first_candle_triggered = (exitdfprices['CumCount'] == 0) & (exitdfprices['Stoploss'] == 'Triggered')
    exitdfprices.loc[first_candle_triggered, 'StoplossPrice'] = exitdfprices.loc[first_candle_triggered, 'Open']
    
    # For subsequent candle stop loss triggers - use Close price
    subsequent_triggered = (exitdfprices['CumCount'] != 0) & (exitdfprices['Stoploss'] == 'Triggered')
    exitdfprices.loc[subsequent_triggered, 'StoplossPrice'] = exitdfprices.loc[subsequent_triggered, 'Close']
    
    # Rank stop loss triggers to get the first occurrence per day/ticker
    exitdfprices['stoploss_rn'] = np.nan
    stoploss_mask = exitdfprices['Stoploss'] == 'Triggered'
    if stoploss_mask.any():
        exitdfprices.loc[stoploss_mask, 'stoploss_rn'] = (
            exitdfprices.loc[stoploss_mask]
            .groupby(['Date', 'Ticker'])['Time']
            .rank(method='first')
            .astype(int)
        )
    
    # Override BuySellFlag for stop loss triggers that occur first
    stop_loss_exit_type = 'Sell' if tradetype == 'Long' else 'Buy'
    first_stoploss_mask = exitdfprices['stoploss_rn'] == 1
    
    # Only override BuySellFlag if stop loss occurs before regular exit time
    exitdfprices.loc[first_stoploss_mask, 'BuySellFlag'] = stop_loss_exit_type
    
    # CRITICAL: Ensure we have exactly one exit per Date/Ticker combination
    # Get the earliest exit signal (either stop loss or regular exit)
    exit_signals = exitdfprices[exitdfprices['BuySellFlag'].isin(['Buy', 'Sell'])].copy()
    
    print(f"Debug - Total exit signals before ranking: {len(exit_signals)}")
    print("Debug - Exit signals per Date/Ticker:")
    print(exit_signals.groupby(['Date', 'Ticker']).size().value_counts())
    
    # Rank to get the FIRST exit signal per Date/Ticker combination  
    exit_signals['exit_rank'] = exit_signals.groupby(['Date', 'Ticker'])['Time'].rank(method='first')
    final_exits = exit_signals[exit_signals['exit_rank'] == 1].copy()
    
    # Mark hard stop loss exits
    final_exits.loc[final_exits["Stoploss"] == "Triggered", "sl_flag"] = "hard"
    
    # Sort for consistent output
    final_exits = final_exits.sort_values(["Date", "Time"]).reset_index(drop=True)
    
    print(f"Debug - Final exit count: {len(final_exits)}")
    print(f"Debug - Unique Date/Ticker combinations in exits: {final_exits[['Date', 'Ticker']].drop_duplicates().shape[0]}")
    
    # Verify we have one exit per entry
    entry_combinations = set(zip(df['Date'], df['Ticker']))
    exit_combinations = set(zip(final_exits['Date'], final_exits['Ticker']))
    
    missing_exits = entry_combinations - exit_combinations
    extra_exits = exit_combinations - entry_combinations
    
    if missing_exits:
        print(f"WARNING - Missing exits for: {missing_exits}")
    if extra_exits:
        print(f"WARNING - Extra exits for: {extra_exits}")
    
    return final_exits

def tradebook(entrydf,exitdf,period='Weekly'):
    commission = 1 if period == 'Weekly' else 0.5
    entrydf = entrydf[['Ticker','Date','Time','Close','Close_EQ']].rename(columns={'Date':'EntryDate','Time':'EntryTime','Close':'EntryPrice'}).reset_index(drop=True)
    entrydf['Exit'] = entrydf.sort_values(by='EntryDate')['EntryDate'].shift(-1)
    exitdf = exitdf[['Ticker','Date','Time','Close']].rename(columns={'Date':'ExitDate','Time':'ExitTime','Close':'ExitPrice'}).reset_index(drop=True)
    tradebook = pd.merge(entrydf,exitdf,left_on=['Ticker','Exit'],right_on=['Ticker','ExitDate'],how='inner')
    tradebook['TradeType'] = tradetype
    tradebook['GrossPnL'] = np.where(tradebook['TradeType'] == 'Short',tradebook['EntryPrice'] - tradebook['ExitPrice'],tradebook['ExitPrice'] - tradebook['EntryPrice'])
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['Close_EQ']) * 100,2)
    return tradebook

expdf = get_weeklyexpiry_dates(index,startdate)
expdf_ = expiry_changes(expdf.copy())
optdf = fetch_options(index,startdate,enddate,entrytime,exittime)
eqdf = fetch_spot_data(index,startdate,enddate)
entry_exit_dates = define_entry_exit_dates(eqdf.copy()).dropna(subset=['EntryDate','ExitDate'])
weeklydf = filter_weekly_contracts(optdf.copy())
weeklydf_ = weeklydf[~weeklydf['Date'].isin(expdf_['DATE'])]
nextweekdf = filter_next_weekly_on_expiryday(optdf.copy(),expdf_.copy())
df = pd.concat([weeklydf_,nextweekdf],ignore_index=True).sort_values(by='Date')
signalsdf = define_candlecolour(eqdf.copy())
entrydf = get_entry(df.copy(),signalsdf.copy())
exitdf = get_exit(entrydf.copy(),optdf.copy(),entry_exit_dates.copy())
exitdf = get_exit(entrydf.copy(),optdf.copy(),entry_exit_dates.copy(),stoploss_flag,exittime,tradetype,stoploss)
tradesheet = tradebook(entrydf.copy(),exitdf.copy())
# print(tradesheet)

os.makedirs(fr"C:\Vishwanath\PythonCodes\Strategy\BTST\BuyingOnly\{index.capitalize()}\Tradebook",exist_ok=True)
tradesheet.to_csv(fr"C:\Vishwanath\PythonCodes\Strategy\BTST\BuyingOnly\{index.capitalize()}\Tradebook\\{index.capitalize()}_{datetime.today().date()}_{tradetype}_{stoploss_flag}_{stoploss}.csv",index=False)