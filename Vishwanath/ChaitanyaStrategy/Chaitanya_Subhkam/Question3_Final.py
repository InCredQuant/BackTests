import pandas as pd
import numpy as np
from datetime import datetime
min_period = 1 ## assuming minimum periods used for calculating mma
rsi_value = 14 ## standard value considered for calculating rsi

def rsi(series, window=14):
    delta = series.diff()
    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)
    avg_gains = gains.rolling(window=window, min_periods=window).mean()
    avg_losses = losses.rolling(window=window, min_periods=window).mean()
    rs = avg_gains / avg_losses
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_mma_rsi_for_OTM(df):
    callDf = df[df['option_type'] == 'c'].copy()
    callDf['time'] = callDf['datetime'].dt.time
    min_expiry_per_datetime = callDf.groupby('datetime')['expiry'].transform('min')
    currentWeekDf = callDf[callDf['expiry'] == min_expiry_per_datetime].copy()
    currentWeekDf = currentWeekDf.sort_values(['datetime', 'strike_price'])
    currentWeekDf['strike_spot_diff'] = abs(currentWeekDf['strike_price'] - currentWeekDf['spot'])
    atm_strikes = (currentWeekDf.groupby('datetime')['strike_spot_diff']
                   .idxmin()
                   .map(lambda idx: currentWeekDf.loc[idx, 'strike_price']))
    currentWeekDf['atm_strike'] = currentWeekDf['datetime'].map(atm_strikes)
    otmCalls = currentWeekDf[currentWeekDf['strike_price'] > currentWeekDf['atm_strike']].copy()
    otmCalls['rank'] = otmCalls.groupby('datetime')['strike_price'].rank(method='first')
    nearestOtm = otmCalls[otmCalls['rank'] == 1].copy()
    target_time = datetime.strptime('09:16:00', '%H:%M:%S').time()
    nearestOtm = nearestOtm[nearestOtm['time'] == target_time].copy()
    merge_keys = nearestOtm[['date', 'strike_price', 'expiry']].drop_duplicates()
    nearestOtmDf = pd.merge(merge_keys, callDf, on=['date', 'strike_price', 'expiry'], how='inner')
    nearestOtmDf = nearestOtmDf.sort_values(['date', 'datetime'])
    nearestOtmDf['30_min_MA'] = (nearestOtmDf.groupby('date')['close']
                                 .transform(lambda x: x.rolling(window=30, min_periods=min_period).mean()))
    nearestOtmDf['RSI_14_ROLLING'] = (nearestOtmDf.groupby('date')['close']
                                      .transform(lambda x: rsi(x, window=14)))
    
    return nearestOtmDf

def process_data(df):
    df['time'] = df['datetime'].dt.time
    spotdf = df[['datetime', 'time', 'spot']].drop_duplicates().reset_index(drop=True)
    spotdf = spotdf.sort_values('datetime').reset_index(drop=True)
    spotdf['30_min_MA'] = spotdf['spot'].rolling(window=30, min_periods=min_period).mean()
    spotdf['RSI_14_ROLLING'] = rsi(spotdf['spot'], window=rsi_value)
    otmDf = calculate_mma_rsi_for_OTM(df.copy())
    finalDf = spotdf[['datetime', 'spot', '30_min_MA', 'RSI_14_ROLLING']].rename(
        columns={'30_min_MA': 'bnifty_30_mma', 'RSI_14_ROLLING': 'bnifty_rsi'}
    )
    otm_cols = otmDf[['datetime', '30_min_MA', 'RSI_14_ROLLING']].rename(
        columns={'30_min_MA': 'otm_ce_mma', 'RSI_14_ROLLING': 'otm_ce_rsi'}
    )    
    finalDf = pd.merge(finalDf, otm_cols, on='datetime', how='inner')
    
    return finalDf

df = pd.read_feather("combined_data_BN.feather")
finalDf = process_data(df)
# finalDf.to_excel("finalDf_Q3.xlsx")
print(finalDf)