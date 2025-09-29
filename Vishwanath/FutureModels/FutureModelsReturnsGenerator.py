import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import time
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")

def ExcelWriter(df,strategy):
    output_path = rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\AllModels\\"
    filename = f"FutureModels_{datetime.today().date().strftime('%d%m%Y')}.xlsx"
    output_path = os.path.join(output_path,filename)
    mode = 'a' if os.path.exists(output_path) else 'w'
    if_sheet_exists = 'new' if os.path.exists(output_path) else None
    with pd.ExcelWriter(output_path, engine='openpyxl', mode=mode, if_sheet_exists=if_sheet_exists) as writer:
        df.to_excel(writer, sheet_name=strategy, index=False)

def GapVolBreakout():
    strategy = 'GapVolBreakout'
    filepath = rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\GapVolBreakout\VOL_BREAKOUT_SYSTEM_GAP.xlsx"
    df = pd.read_excel(filepath,sheet_name='INDIVIDUAL_MONTHLY_RETURNS')
    df = df.loc[:,~df.columns.str.contains('^Unnamed')]
    columns_to_convert = ['NIFTY', 'BANKNIFTY','MIDCPNIFTY']
    df[columns_to_convert] = df[columns_to_convert].apply(lambda x: x * 100)
    for sheet in columns_to_convert:
        deltadf = pd.read_excel(filepath,sheet_name=sheet)
        deltadf_ = deltadf[['Dates','SIGNAL']]
        df = pd.merge(df,deltadf_,on='Dates',how='left').rename(columns={'SIGNAL':f'{sheet}Delta'})

    ExcelWriter(df,strategy)

def Pattern():
    PatternDF = pd.DataFrame()
    strategy = 'Pattern'
    indices = ['BANKNIFTY','NIFTY','MIDCPNIFTY']
    symbol_mapper = {'BANKNIFTY':'BNF','NIFTY':'NF','MIDCPNIFTY':'MNF'}
    for index in indices:
        filepath = rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Pattern\output\FinalOutput\\{index.upper()}_ALL_STATS_OUTPUT.xlsx"
        df = pd.read_excel(filepath)
        df = df.loc[:,~df.columns.str.contains('^Unnamed')]
        df_ = df[['Date','Returns','Pos']]
        symbolName = symbol_mapper.get(index)
        df_ = df_.rename(columns={'Pos':f'{symbolName}Delta','Returns':f'{symbolName}Returns'})
        columns_to_convert = [f'{symbolName}Returns']
        df_[columns_to_convert] = df_[columns_to_convert].apply(lambda x: x * 100)
        if PatternDF.empty:
            PatternDF = pd.concat([PatternDF, df_], axis=1)
        else:
            PatternDF = pd.merge(PatternDF, df_, on='Date', how='outer')
    ExcelWriter(PatternDF,strategy)

def Breakout():
    BreakoutDF = pd.DataFrame()
    strategy = 'Breakout'
    indices = ['BANKNIFTY','NIFTY','MIDCPNIFTY']
    symbol_mapper = {'BANKNIFTY':'BNF','NIFTY':'NF','MIDCPNIFTY':'MNF'}
    for index in indices:
        filepath = rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Breakout\output\FinalOutput\\{index}_BKT_REVISED_DAILY_OUTPUT.xlsx"
        df = pd.read_excel(filepath)
        df = df.loc[:,~df.columns.str.contains('^Unnamed')]
        df_ = df[['Date','Returns','Pos']]
        symbolName = symbol_mapper.get(index)
        df_ = df_.rename(columns={'Pos':f'{symbolName}Delta','Returns':f'{symbolName}Returns'})
        columns_to_convert = [f'{symbolName}Returns']
        df_[columns_to_convert] = df_[columns_to_convert].apply(lambda x: x * 100)
        if BreakoutDF.empty:
            BreakoutDF = pd.concat([BreakoutDF, df_], axis=1)
        else:
            BreakoutDF = pd.merge(BreakoutDF, df_, on='Date', how='outer')
    ExcelWriter(BreakoutDF,strategy)

GapVolBreakout()
Pattern()
Breakout()
print(f"File generated successfully for all models at FutureModels_{datetime.today().date().strftime('%d%m%Y')}.xlsx")