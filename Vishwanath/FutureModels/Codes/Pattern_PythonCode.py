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
sys.path.insert(0, r"C:\Vishwanath\PythonCodes\Strategy\FutureModels\commons")
from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from stats import Stats, Filter
from datetime import datetime, timedelta
import pandas as pd
sys.path.insert(0,r"C:\Vishwanath\PythonCodes\Strategy\BacktestUpdater")
from date_config import *
from pathlib import Path

input_df = pd.read_excel(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\InputFile.xlsx")
folder = Path(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Pattern\output")
outputfolder = Path(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Pattern\output\FinalOutput")
indices = ['NIFTY','BANKNIFTY','MIDCPNIFTY']
# indices = ['MIDCPNIFTY']
fut_or_spot = 'FUT'                               ## FUT/SPOT
smoothing_factor = 30
lookback = 4                                      ## for calculating CR
multiplier = 0.62

def define_atr(df):
    df = df.dropna(subset=[f'{fut_or_spot}_OPEN',f'{fut_or_spot}_CLOSE']).reset_index(drop=True)
    df['TR'] = np.subtract(np.maximum(df[f'{fut_or_spot}_HIGH'],df[f'{fut_or_spot}_CLOSE'].shift(1)),np.minimum(df[f'{fut_or_spot}_LOW'],df[f'{fut_or_spot}_CLOSE'].shift(1)))
    initial_atr = df['TR'].iloc[1:31].mean()
    df['ATR'] = np.nan
    df.loc[30, 'ATR'] = initial_atr
    df = df.reset_index(drop=True)
    for i in range(31, len(df)):
        df.loc[i, 'ATR'] = (df.loc[i, 'TR'] * (2/31)) + (df.loc[i-1, 'ATR'] * (1-(2/31)))
    df['DR'] = np.subtract(df[f'{fut_or_spot}_HIGH'],df[f'{fut_or_spot}_LOW'])
    df['CR'] = df[f'{fut_or_spot}_CLOSE'].rolling(window=lookback).max() - df[f'{fut_or_spot}_CLOSE'].rolling(window=lookback).min()
    df['TW'] = np.where(df['CR'].shift(1)<df['ATR'].shift(1),1,0)
    df['B'] = np.add(df[f'{fut_or_spot}_OPEN'],np.multiply(df['DR'].shift(1),multiplier))
    df['S'] = np.subtract(df[f'{fut_or_spot}_OPEN'],np.multiply(df['DR'].shift(1),multiplier))
    # print(df.tail(20))
    return df

def pattern(input_df,indices):
    with pd.ExcelWriter(r"Pattern_PythonCode_Op.xlsx", engine='openpyxl') as writer:
        for indexName in indices:
            index_mapping = {'NIFTY': 'NF_', 'BANKNIFTY': 'BNF_', 'MIDCPNIFTY': 'MNF_'}
            prefix = index_mapping.get(indexName)
            selected_columns = ['Dates'] + [col for col in input_df.columns if col.startswith(prefix) and fut_or_spot in col.replace(prefix,'')]
            df = input_df[selected_columns]
            df.columns = df.columns.str.replace(prefix, '')
            # Drop rows where all values (except Dates) are same as previous row
            cols_to_compare = [col for col in df.columns if col != 'Dates']
            df = df.loc[(df[cols_to_compare] != df[cols_to_compare].shift(1)).any(axis=1)].reset_index(drop=True)
            df = df[df['Dates'] >= '2007-01-02'].reset_index(drop=True)
            df_ = define_atr(df.copy())
            df_.to_excel(writer, sheet_name=indexName, index=False)
            workbook = writer.book
            worksheet = writer.sheets[indexName]
            worksheet.freeze_panes = 'B2'
    print("Pattern Done!")

def entry_order(symbol, pos, qty, px, dt):
    order_obj = Order()
    order_obj.segment = Segment.FT
    order_obj.symbol = symbol
    order_obj.entry_price = px
    order_obj.quantity = qty
    order_obj.entry_date = dt
    order_obj.position = pos
    return order_obj

def exit_order(order_obj, px, dt, trade_reg):
    order_obj.exit_date = dt
    order_obj.exit_price = px
    trade_reg.append_trade(order_obj)

def main():
    start_dates ={
        'NIFTY':datetime(2010,1,1),
        'BANKNIFTY':datetime(2010,1,1),
        'MIDCPNIFTY':datetime(2023,6,1)
    }
    # symbol = 'MIDCPNIFTY'
    # start_date = datetime(2023,6,1) # 2010,1,1 for NF and BN, 2023,6,1 for Midcp
    # end_date = datetime(2025,4,24) ## only change this to the most recent date for updating the backtest.
    end_date = futurelastdate

    for symbol, start_date in start_dates.items():
        print(f"Processing {symbol} from {start_date} to {end_date}")
        df = pd.read_excel('Pattern_PythonCode_Op.xlsx', sheet_name=symbol, index_col=0)
        df.dropna(inplace=True)
        df.index = pd.to_datetime(df.index, format="%Y-%m-%d")
        df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
        trade_reg = TradeRegister()
        entry_flag = False
        pos = 0
        order_obj = None
        backtest_name = symbol+'_PATTERN'
        qty_map = {'NIFTY':1, 'BANKNIFTY':1,'MIDCPNIFTY':1}#{'NIFTY':50, 'BANKNIFTY':25}
        all_data = []
        current_px = 0
        for idx in range(1, len(df)):
            current_px = df.iloc[idx][f'{fut_or_spot}_CLOSE']
            current_dt = df.iloc[idx].name
            if entry_flag:
                if (pos == 1) & (df.iloc[idx][f'{fut_or_spot}_LOW'] <= df.iloc[idx]['S']): # long exit
                    exit_order(order_obj, df.iloc[idx]['S'], df.iloc[idx].name, trade_reg)
                    entry_flag = False
                    pos = 0
                    order_obj = None
                    current_px = df.iloc[idx]['S']
                    print('Long exit on {}'.format(df.iloc[idx].name))
                if (pos == -1) & (df.iloc[idx][f'{fut_or_spot}_HIGH'] >= df.iloc[idx]['B']): # short exit
                    exit_order(order_obj, df.iloc[idx]['B'], df.iloc[idx].name, trade_reg)
                    entry_flag = False
                    pos = 0
                    order_obj = None
                    current_px = df.iloc[idx]['B']
                    print('Short exit on {}'.format(df.iloc[idx].name))

            if not entry_flag:
                if df.iloc[idx]['TW'] == 1: # trading window
                    if (pos == 0) & (df.iloc[idx][f'{fut_or_spot}_HIGH'] > df.iloc[idx]['B']): # buy triggered
                        order_obj = entry_order(symbol,Position.LONG,qty_map[symbol],df.iloc[idx]['B'],df.iloc[idx].name)
                        entry_flag = True
                        pos = 1
                        current_px = df.iloc[idx]['B']
                        print('Long on {}'.format(df.iloc[idx].name))
                    if (pos == 0) & (df.iloc[idx][f'{fut_or_spot}_LOW'] < df.iloc[idx]['S']):
                        order_obj = entry_order(symbol, Position.SHORT, qty_map[symbol], df.iloc[idx]['S'], df.iloc[idx].name)
                        entry_flag = True
                        pos = -1
                        current_px = df.iloc[idx]['S']
                        print('Short on {}'.format(df.iloc[idx].name))

            all_data.append((current_dt,current_px,pos))

        writer = pd.ExcelWriter('C:/Vishwanath/PythonCodes/Strategy/FutureModels/Pattern/output/' + symbol + '_ALL_STATS.xlsx')

        trades = trade_reg.get_trade_register()
        trades['RETURN'] = trades['PNL']/(trades['ENTRY_PRICE']*trades['QUANTITY'])
        trades.to_excel(writer, sheet_name='TRADES')
        daily_df = pd.DataFrame(all_data, columns=['Date','Px','Pos'])
        daily_df.to_excel(writer, sheet_name='DAILY')
        stats_obj = Stats(trades)
        stats_df = stats_obj.create_stats()
        stats_df.to_excel(writer, sheet_name='STATS')
        writer.close()

def calculate_returns(df):
    df['Date'] = pd.to_datetime(df['Date'])
    prev_pos = df['Pos'].shift(1).fillna(0)
    prev_px = df['Px'].shift(1)
    current_px = df['Px']
    conditions = [prev_pos == 1, prev_pos == -1]
    choices = [(current_px / prev_px) - 1, (prev_px / current_px) - 1]
    df['Returns'] = np.select(conditions, choices, default=0)
    df['NAV'] = 100 * (1 + df['Returns']).cumprod()
    df['Running_Max_NAV'] = df['NAV'].cummax()
    df['Drawdown'] = (df['NAV'] / df['Running_Max_NAV']) - 1
    max_drawdown = df['Drawdown'].min()
    result_df = df[['Date', 'Px', 'Pos', 'Returns', 'NAV', 'Drawdown']]
    result_df['Max_Drawdown'] = max_drawdown
    return result_df

if __name__ == '__main__':
    pattern(input_df,indices)
    main()
    for file in folder.glob('*.xlsx'):
        df = pd.read_excel(file, sheet_name='DAILY')
        result_df = calculate_returns(df)
        output_file = outputfolder / f"{file.stem}_OUTPUT.xlsx"
        result_df.to_excel(output_file)