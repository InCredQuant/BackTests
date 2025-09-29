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
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

input_df = pd.read_excel(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\InputFile.xlsx")
indices = ['NIFTY','BANKNIFTY','MIDCPNIFTY']
index_multipliers = {'NIFTY': (0.5, 1), 'BANKNIFTY': (0.75, 2), 'MIDCPNIFTY': (0.5, 1)}

def define_gap_atr(df, index):
    part_a_multiplier, part_b_multiplier = index_multipliers.get(index, (0.5, 1))
    df = df.dropna(subset=['SPOT_OPEN','SPOT_CLOSE','FUT_OPEN','FUT_CLOSE'])
    df['GAP'] = np.where(df.index == 0, 0,
                        np.where(df['SPOT_OPEN'] > df['SPOT_CLOSE'].shift(1), 1, -1))
    df['TR'] = np.where(df.index == 0, df['SPOT_HIGH'] - df['SPOT_LOW'],
                       np.maximum.reduce([df['SPOT_HIGH'] - df['SPOT_LOW'],
                                        abs(df['SPOT_HIGH'] - df['SPOT_CLOSE'].shift(1)),
                                        abs(df['SPOT_LOW'] - df['SPOT_CLOSE'].shift(1))]))
    df['2ATR'] = df['TR'].rolling(window=2).mean()
    df['5ATR'] = df['TR'].rolling(window=5).mean()
    strategy_cols = ['AVG_ATR', 'PART_A', 'PART_B', 'BUY_LEVEL', 'SELL_LEVEL', 
                    'BUY_TRG', 'SELL_TRG', 'SIGNAL', 'PX', 'RETURN', 'NAV', 
                    'DRAWDOWN', 'MAXDRAWDOWN']
    for col in strategy_cols:
        df[col] = np.nan
    valid_mask = df['5ATR'].notna()
    if valid_mask.any():
        valid_df = df[valid_mask].copy()
        atr_cols = valid_df[['TR', '2ATR', '5ATR']]
        valid_df['AVG_ATR'] = atr_cols.mean(axis=1, skipna=True)
        valid_df['PART_A'] = np.multiply(valid_df['AVG_ATR'], part_a_multiplier)
        valid_df['PART_B'] = np.multiply(valid_df['AVG_ATR'], part_b_multiplier)
        valid_df['BUY_LEVEL'] = np.where(valid_df['GAP'] == 1, 
                                        valid_df['FUT_OPEN']+valid_df['PART_A'].shift(1), 
                                        valid_df['FUT_OPEN']+valid_df['PART_B'].shift(1))
        valid_df['SELL_LEVEL'] = np.where(valid_df['GAP'] == -1, 
                                         valid_df['FUT_OPEN']-valid_df['PART_A'].shift(1), 
                                         valid_df['FUT_OPEN']-valid_df['PART_B'].shift(1))
        valid_df['BUY_TRG'] = np.where(valid_df['FUT_HIGH'] > valid_df['BUY_LEVEL'], 1, 0)
        valid_df['SELL_TRG'] = np.where(valid_df['FUT_LOW'] < valid_df['SELL_LEVEL'], -1, 0)
        valid_df['SIGNAL'] = np.select([
            (valid_df['BUY_TRG'] == 1) & (valid_df['SELL_TRG'] == 0),
            (valid_df['BUY_TRG'] == 0) & (valid_df['SELL_TRG'] == -1),
            (valid_df['BUY_TRG'] == 1) & (valid_df['SELL_TRG'] == -1)
        ], [1, -1, 1], default=np.nan)
        valid_df['SIGNAL'] = valid_df['SIGNAL'].fillna(method='ffill').fillna(0)
        valid_df['PX'] = np.select([
            (valid_df['SIGNAL'] == 1) & (valid_df['SIGNAL'] != valid_df['SIGNAL'].shift(1)),
            (valid_df['SIGNAL'] == -1) & (valid_df['SIGNAL'] != valid_df['SIGNAL'].shift(1))
        ], [valid_df['BUY_LEVEL'], valid_df['SELL_LEVEL']], default=valid_df['FUT_CLOSE'])
        valid_df['PX'] = valid_df['PX'].fillna(method='ffill').fillna(valid_df['FUT_CLOSE'])
        valid_df['RETURN'] = np.where(valid_df['SIGNAL'].shift(1)==1, valid_df['PX']/valid_df['PX'].shift(1)-1,
                                    np.where(valid_df['SIGNAL'].shift(1)==-1, valid_df['PX'].shift(1)/valid_df['PX']-1, 0))
        valid_df['RETURN'] = valid_df['RETURN'].fillna(0)
        valid_df['NAV'] = (valid_df['RETURN']+1).cumprod()
        valid_df['DRAWDOWN'] = valid_df['NAV'] / valid_df['NAV'].expanding().max() - 1
        valid_df['MAXDRAWDOWN'] = valid_df['DRAWDOWN'].expanding().min()
        for col in strategy_cols:
            df.loc[valid_mask, col] = valid_df[col]
    
    return df

def gap_vol_breakout(input_df, indices):
    with pd.ExcelWriter(r"GapVolBreakout_PythonCode_Op.xlsx", engine='openpyxl') as writer:
        for index in indices:
            index_mapping = {'NIFTY': 'NF_', 'BANKNIFTY': 'BNF_', 'MIDCPNIFTY': 'MNF_'}
            prefix = index_mapping.get(index)
            selected_columns = ['Dates'] + [col for col in input_df.columns if col.startswith(prefix)]
            df = input_df[selected_columns]
            df.columns = df.columns.str.replace(prefix, '')
            # Drop rows where all values (except Dates) are same as previous row
            cols_to_compare = [col for col in df.columns if col != 'Dates']
            df = df.loc[(df[cols_to_compare] != df[cols_to_compare].shift(1)).any(axis=1)].reset_index(drop=True)
            df_ = define_gap_atr(df.copy(), index)
            df_.to_excel(writer, sheet_name=index, index=False)
            workbook = writer.book
            worksheet = writer.sheets[index]
            worksheet.freeze_panes = 'B2'
            
    print("Gap Vol Breakout Done!")

gap_vol_breakout(input_df, indices)