import pandas as pd
import numpy as np
from datetime import datetime
from openpyxl import load_workbook

market_open = datetime.strptime('09:30:00', '%H:%M:%S').time()
market_close_time = datetime.strptime('15:30:00', '%H:%M:%S').time()
distanceMultiplier = 1.5
dmaWindow = 30
dmaMinPeriods = 1

def load_and_preprocess_data(filepath):
    df = pd.read_feather(filepath)
    df['time'] = df['datetime'].dt.time
    df['date'] = df['datetime'].dt.date
    min_expiry_per_datetime = df.groupby('datetime')['expiry'].transform('min')
    currentWeekdf = df[df['expiry'] == min_expiry_per_datetime].copy()
    currentWeekdf = currentWeekdf.sort_values(['datetime', 'strike_price'])
    currentWeekdf['strike_spot_diff'] = abs(currentWeekdf['strike_price'] - currentWeekdf['spot'])
    atm_strikes = (currentWeekdf.groupby('datetime')['strike_spot_diff']
                   .idxmin()
                   .map(lambda idx: currentWeekdf.loc[idx, 'strike_price']))
    currentWeekdf['atm_strike'] = currentWeekdf['datetime'].map(atm_strikes)
    
    return df, currentWeekdf

def calculate_strangle_strikes(currentWeekdf):
    atm_only_df = currentWeekdf[currentWeekdf['strike_price'] == currentWeekdf['atm_strike']]
    straddle_price = atm_only_df[atm_only_df['time'] == market_open].copy()
    straddle_price['straddle'] = straddle_price.groupby('date')['close'].transform('sum')
    straddle_price['distance'] = straddle_price['straddle'] * distanceMultiplier
    straddle_price['strangle_strike'] = np.where(
        straddle_price['option_type'] == 'c',
        straddle_price['strike_price'] + straddle_price['distance'],
        straddle_price['strike_price'] - straddle_price['distance']
    )
    
    return straddle_price[['date', 'expiry', 'option_type', 'strangle_strike', 'time']]

def find_strangle_contracts(df, strangle_strikes_info):
    strangle_strikes = pd.merge(
        df,
        strangle_strikes_info,
        on=['date', 'expiry', 'option_type'],
        how='left',
        suffixes=['', '_entry']
    )
    strangle_strikes = strangle_strikes.dropna(subset=['strangle_strike']).reset_index(drop=True)
    strangle_strikes['diff'] = abs(strangle_strikes['strangle_strike'] - strangle_strikes['strike_price'])
    strangle_strikes = strangle_strikes[strangle_strikes['time'] == strangle_strikes['time_entry']]
    idxmin = strangle_strikes.groupby(['date', 'option_type'])['diff'].idxmin()
    filtered_strikes = strangle_strikes.loc[idxmin]
    strangleDf = pd.merge(
        df,
        filtered_strikes[['date', 'expiry', 'option_type', 'strike_price', 'time']],
        on=['date', 'expiry', 'option_type', 'strike_price'],
        how='inner',
        suffixes=['', '_entry']
    )
    strangleDf = strangleDf[strangleDf['time'] >= strangleDf['time_entry']].reset_index(drop=True)
    strangleDf['combined_premium'] = strangleDf.groupby(['date', 'time'])['close'].transform('sum')
    strangle_df_ = strangleDf[['date', 'time', 'expiry', 'combined_premium']].drop_duplicates().reset_index(drop=True)
    strangle_df_['30_DMA'] = strangle_df_['combined_premium'].rolling(window=dmaWindow, min_periods=dmaMinPeriods).mean()
    strangle_df_ = strangle_df_.sort_values(['date', 'time']).reset_index(drop=True)
    
    return strangle_df_

def strangle_strategy(df):
    df = df.assign(
        signal=0,
        position=0,
        entry_price=0.0,
        exit_price=0.0,
        pnl=0.0,
        prev_premium=df.groupby('date')['combined_premium'].shift(1),
        prev_dma=df.groupby('date')['30_DMA'].shift(1),
        is_market_close=df['time'] >= market_close_time
    )
    df['sell_condition'] = (
        (df['prev_premium'] >= df['prev_dma']) & 
        (df['combined_premium'] < df['30_DMA']) & 
        df['prev_premium'].notna() &
        ~df['is_market_close']
    )
    
    df['buy_condition'] = (
        (df['prev_premium'] <= df['prev_dma']) & 
        (df['combined_premium'] > df['30_DMA']) & 
        df['prev_premium'].notna() &
        ~df['is_market_close']
    )
    position_state = 0
    entry_price_state = 0.0
    
    for i, row in df.iterrows():
        current_price = row['combined_premium']
        if row['is_market_close'] and position_state == 1:
            df.at[i, 'signal'] = -1
            df.at[i, 'exit_price'] = current_price
            df.at[i, 'pnl'] = entry_price_state - current_price
            position_state = 0
            entry_price_state = 0.0
        
        elif row['sell_condition'] and position_state == 0:
            df.at[i, 'signal'] = 1
            df.at[i, 'entry_price'] = current_price
            position_state = 1
            entry_price_state = current_price
        
        elif row['buy_condition'] and position_state == 1:
            df.at[i, 'signal'] = -1
            df.at[i, 'exit_price'] = current_price
            df.at[i, 'pnl'] = entry_price_state - current_price
            position_state = 0
            entry_price_state = 0.0
        
        df.at[i, 'position'] = position_state
    df['cumulative_pnl'] = df['pnl'].cumsum()
    df['trade_count'] = (df['signal'] == -1).cumsum()
    cols_to_drop = ['prev_premium', 'prev_dma', 'is_market_close', 'sell_condition', 'buy_condition']
    return df.drop(columns=cols_to_drop)

def extract_trade_details(resultDf):
    entries = resultDf[resultDf['signal'] == 1][['date', 'time', 'entry_price']].copy()
    entries['datetime'] = pd.to_datetime(entries['date'].astype(str) + ' ' + entries['time'].astype(str))
    exits = resultDf[resultDf['signal'] == -1][['date', 'time', 'exit_price', 'pnl']].copy()
    exits['datetime'] = pd.to_datetime(exits['date'].astype(str) + ' ' + exits['time'].astype(str))
    trades = pd.DataFrame({
        'entry_datetime': entries['datetime'].values,
        'entry_price': entries['entry_price'].values,
        'exit_datetime': exits['datetime'].values,
        'exit_price': exits['exit_price'].values,
        'pnl': exits['pnl'].values
    })    
    return trades[['entry_datetime', 'entry_price', 'exit_price', 'pnl']]

def calculate_daily_pnl(resultDf):
    daily_pnl = resultDf.groupby('date')['pnl'].sum().reset_index()
    daily_pnl.columns = ['date', 'total_pnl']
    return daily_pnl

def write_to_excel(trades_df, daily_pnl_df, excel_path='results.xlsx'):
    try:
        book = load_workbook(excel_path)
        writer = pd.ExcelWriter(excel_path, engine='openpyxl') 
        writer.book = book
    except FileNotFoundError:
        writer = pd.ExcelWriter(excel_path, engine='openpyxl')
    trades_df.to_excel(writer, sheet_name='answer_4a', index=False)
    daily_pnl_df.to_excel(writer, sheet_name='answer_4b', index=False)
    writer.close()

def main():
    df, currentWeekdf = load_and_preprocess_data("combined_data_BN.feather")
    strangleStrikesdf = calculate_strangle_strikes(currentWeekdf)
    strangleDf = find_strangle_contracts(df, strangleStrikesdf)
    resultDf = strangle_strategy(strangleDf.copy())
    print(resultDf)
    # resultDf.to_csv("resultdf.csv", index=False)
    # trades_df = extract_trade_details(resultDf)
    # daily_pnl_df = calculate_daily_pnl(resultDf)
    # write_to_excel(trades_df, daily_pnl_df)

if __name__ == "__main__":
    main()