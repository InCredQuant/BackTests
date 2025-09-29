import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore')

df = pd.read_feather("combined_data_BN.feather")
df['time'] = df['datetime'].dt.time
min_expiry_per_datetime = df.groupby('datetime')['expiry'].transform('min')
currentWeekDf = df[df['expiry'] == min_expiry_per_datetime].copy()
currentWeekDf = currentWeekDf.sort_values(['datetime', 'strike_price'])
currentWeekDf['strike_spot_diff'] = abs(currentWeekDf['strike_price'] - currentWeekDf['spot'])
atm_strikes = (currentWeekDf.groupby('datetime')['strike_spot_diff']
                .idxmin()
                .map(lambda idx: currentWeekDf.loc[idx, 'strike_price']))
currentWeekDf['atm_strike'] = currentWeekDf['datetime'].map(atm_strikes)
atm_only_df = currentWeekDf[currentWeekDf['strike_price'] == currentWeekDf['atm_strike']]
straddleDf = atm_only_df[atm_only_df['time']==datetime.strptime('09:30:00','%H:%M:%S').time()]

def getStraddleDf(straddledf,df):
    mergedDf = pd.merge(df,straddledf[['date','strike_price','option_type','expiry','time']],on=['date','strike_price','option_type','expiry'],how='inner',suffixes=['','_entry'])
    mergedDf = mergedDf[(mergedDf['time'] >= mergedDf['time_entry']) & (mergedDf['time'] <= datetime.strptime('15:15:00','%H:%M:%S').time())]
    mergedDf['ticker'] = mergedDf['symbol'] + mergedDf['option_type']
    mergedDf['cumcount'] = mergedDf.groupby(['date','ticker']).cumcount()
    mergedDf['BuySellFlag'] = np.where((mergedDf['cumcount'] == 0), 'Sell', '')
    mergedDf['BuySellFlag'] = np.where((mergedDf['cumcount'] == mergedDf.groupby(['date', 'ticker'])['cumcount'].transform('max')), 'Buy', mergedDf['BuySellFlag'])
    mergedDf['EntryPrice'] = np.where(mergedDf['BuySellFlag'] == 'Sell', mergedDf['close'], np.nan)
    mergedDf['EntryPrice'] = mergedDf.groupby(['date', 'ticker'])['EntryPrice'].transform('last')
    signals_df = mergedDf[(mergedDf["BuySellFlag"] == "Buy") | (mergedDf["BuySellFlag"] == "Sell")]
    return signals_df
    # display(signals_df)

def tradebook(signals_df):
    signals_df = signals_df[['ticker','date','time', 'close', 'option_type', 'BuySellFlag','spot']]
    entry_df = signals_df[signals_df["BuySellFlag"] == "Sell"].rename(columns = {"time": "entrytime", "close": "EntryPrice"}).drop(["BuySellFlag"], axis = 1)
    exit_df = signals_df[signals_df["BuySellFlag"] == "Buy"].rename(columns = {"time": "exittime", "close": "ExitPrice"}).drop(["BuySellFlag", "option_type",'spot'], axis = 1)
    tradebook = pd.merge(entry_df, exit_df, on = ["ticker", "date"], how = "left")
    tradebook["PositionType"] = 'Short'
    tradebook['GrossPnL'] = tradebook['EntryPrice'] - tradebook['ExitPrice']
    tradebook['PnL%'] = round((tradebook['GrossPnL'] / tradebook['spot']) * 100,2)
    tradebook = tradebook.sort_values(["date", "entrytime"])
    return tradebook

def getStraddleMTM(straddle_entry_df, df):
    mergedDf = pd.merge(
        df,
        straddle_entry_df[['date', 'strike_price', 'option_type', 'expiry', 'time', 'close']],
        on=['date', 'strike_price', 'option_type', 'expiry'],
        how='inner',
        suffixes=('', '_entry')
    )
    mergedDf = mergedDf[
        (mergedDf['time'] >= mergedDf['time_entry']) & 
        (mergedDf['time'] <= datetime.strptime('15:15:00', '%H:%M:%S').time())
    ]
    mergedDf['MTM_PnL'] = mergedDf['close_entry'] - mergedDf['close']
    mtm_df = mergedDf.groupby(['date', 'time', 'spot']).agg({
        'MTM_PnL': 'sum',
        'close_entry': 'first'
    }).reset_index()
    return mtm_df

signalsDf = getStraddleDf(straddleDf.copy(),df.copy())
tradebook = tradebook(signalsDf.copy())
mtm_df = getStraddleMTM(straddleDf.copy(), df.copy())
daily_mtm_stats = mtm_df.groupby('date').agg({
    'MTM_PnL': ['min', 'max'],
}).reset_index()
daily_mtm_stats.columns = ['date', 'Worst_MTM_PnL', 'Best_MTM_PnL']
daily_pnl = tradebook.groupby('date').agg({'GrossPnL':'sum'}).reset_index()
daily_pnl = pd.merge(daily_pnl,daily_mtm_stats,on='date',how='left')
# daily_pnl.to_csv("Q2_Final.csv",index=False)

