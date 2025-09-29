import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import sys
sys.path.insert(0, 'G:\\Shared drives\\BackTests\\pycode\\DBUpdation\\')
import pg_redirect
from date_config import *

# Input Parameters
# last_updated_date = '2025-03-24'
# date_obj = datetime.strptime(last_updated_date, '%Y-%m-%d')
# lastupdated_asof = date_obj.strftime('%d%m%Y')

# enddate = '2025-04-25'
# date_obj = datetime.strptime(enddate, '%Y-%m-%d')
# updated_asof = date_obj.strftime('%d%m%Y')
strategy_starttime = '11:00:00'
strategy_endtime = '13:00:00'
endtime = '15:25:00'
interval = timedelta(minutes=5)
timeframe = '5'
stoploss = 0.25
index = 'Nifty'
period = 20
timeframe = 5

tradesheet_filename = rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\Tradesheet\\Updated_{lastupdated_asof}\\"
navpath = rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\NAV\\Updated_{lastupdated_asof}\\"

def connection_details():
    conn = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    return conn

def get_last_updated_date():
    # last_updated_nav_sheet = pd.read_excel(os.path.join(navpath,f'{index}_Directional_1100_NAV.xlsx'))
    last_updated_nav_sheet = pd.read_excel(os.path.join(navpath,f'{index}_1100_NAV.xlsx'))
    lastdate = pd.to_datetime(last_updated_nav_sheet['Date']).dt.date.max()
    return lastdate

def fetch_latest_data(lastdate,strategy_starttime):
    connection = connection_details()
    sqlquery = f'''
        SELECT * FROM weekly_gdfl_min_opt
        WHERE "Date" > '{lastdate}' 
        AND "Label" = 'W1'
        AND "Name" = '{index.upper()}'
        AND "Time" >= '{strategy_starttime}'
        AND "Ticker" NOT LIKE '%-I%'; '''
    rawdata = pd.read_sql(sqlquery,connection)
    connection.close()
    return rawdata

def fetch_latest_spotdata(lastdate):
    connection = connection_details()
    sql_query = f'''
        SELECT * FROM spotdata
        WHERE "Symbol" = '{index.upper()}' 
        AND "Date" > '{lastdate}'; '''
    eqdata = pd.read_sql(sql_query,connection)
    connection.close()
    return eqdata

def convert_eq(df,timeframe):
    df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + " " + df['Time'])
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

def calculate_sma(df,period):
    df['20_SMA'] = df['Close'].rolling(window=period).mean()
    return df

def seconds_converter(data):
    st = time.time()
    data['Time'] = pd.to_datetime(data['Time'])
    data['Time'] = data['Time'].dt.strftime('%H:%M:00')
    print("Time taken: ",time.time()-st)
    return data

def strikeselector(optdf,eqdf,strategy_starttime):
    df = pd.merge(optdf,eqdf[['Date','Time','Close']],on=['Date','Time'],suffixes=['','_EQ'])
    df = df[df['Time'] == strategy_starttime]
    df['Difference'] = df['StrikePrice'] - df['Close_EQ']
    df['Difference'] = df['Difference'].abs()
    df = df.dropna(subset = ['Close_EQ'])
    result_df = df.loc[df.groupby(['Date', 'Call_Or_Put'])['Difference'].idxmin()]
    return result_df

def directional_sma_generator(atmdf,smadf,rawdf):
    signal_df = pd.merge(atmdf,smadf[['Date','Time','20_SMA']],on=['Date','Time'],how='left')
    signal_df['Signal'] = np.where(signal_df['Close_EQ'] > signal_df['20_SMA'],'PE','CE')
    signal_df = signal_df[signal_df['Call_Or_Put'] == signal_df['Signal']]
    to_workdf = pd.merge(rawdf,signal_df[['Date','Ticker','Signal','Close_EQ']],on=['Date','Ticker'],how='inner')
    return to_workdf

def directional_signal_generator(df,stoploss):
    df['CumTimeCount'] = df.groupby(['Date', 'Ticker']).cumcount()
    df['BuySellFlag'] = np.where((df['CumTimeCount'] == 0), 'Sell', '')
    df['BuySellFlag'] = np.where((df['CumTimeCount'] == df.groupby(['Date', 'Ticker'])['CumTimeCount'].transform('max')), 'Buy', df['BuySellFlag'])
    df['EntryPrice'] = np.where(df['BuySellFlag'] == 'Sell', df['Close'], np.nan)
    df['EntryPrice'] = df.groupby(['Date', 'Ticker'])['EntryPrice'].transform('last')
    df['Stoploss'] = np.where((df['High'] >= (df['EntryPrice'] * (1 + stoploss))) & (df["CumTimeCount"] != 0), 'Triggered', '')
    df['StoplossPrice'] = np.where(df['Stoploss'] == 'Triggered', (df['EntryPrice'] * (1 + stoploss)), '')
    df['stoploss_rn'] = df.groupby(['Date', 'Ticker', 'Stoploss'])['Time'].rank().astype(int)
    df.loc[(df['stoploss_rn'] != 1) | (df['Stoploss'] != "Triggered"), 'stoploss_rn'] = np.nan
    df['BuySellFlag'] = np.where(df["stoploss_rn"] == 1, 'Buy', df['BuySellFlag'])
    signals_df = df[(df["BuySellFlag"] == "Buy") | (df["BuySellFlag"] == "Sell")]
    signals_df['buysell_rn'] = signals_df.groupby(['Date', 'Ticker', 'BuySellFlag'])['Time'].rank().astype(int)
    signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
    signals_df.loc[signals_df["Stoploss"] == "Triggered", "sl_flag"] = "hard"
    signals_df["Position"] = 1
    return signals_df

def tradebook_new(signals_df):
    commission = 1
    brokerage = 6
    signals_df = signals_df[['Ticker', 'Date', 'Time', 'Close', 'Call_Or_Put', 'BuySellFlag', 'Stoploss', 'StoplossPrice','sl_flag', "Position","Close_EQ"]]
    entry_df = signals_df[signals_df["BuySellFlag"] == "Sell"].rename(columns = {"Time": "EntryTime", "Close": "EntryPrice"}).drop(["BuySellFlag","Stoploss", "StoplossPrice", "sl_flag"], axis = 1)
    exit_df = signals_df[signals_df["BuySellFlag"] == "Buy"].rename(columns = {"Time": "ExitTime", "Close": "ExitPrice"}).drop(["BuySellFlag", "Call_Or_Put","Close_EQ"], axis = 1)
    exit_df.loc[exit_df["Stoploss"] == "Triggered", "ExitPrice"] = pd.to_numeric(exit_df["StoplossPrice"], errors='coerce')
    exit_df = exit_df.drop(["StoplossPrice"], axis = 1)
    tradebook = pd.merge(entry_df, exit_df, on = ["Ticker", "Date", "Position"], how = "left")
    tradebook["PositionType"] = 'Short'
    tradebook['GrossPnL'] = tradebook['EntryPrice'] - tradebook['ExitPrice']
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['Close_EQ']) * 100,2)
    tradebook['Final_PnL%'] = tradebook['PnL%'] - (((brokerage*2) / (tradebook['Close_EQ'] * 15))*100)
    tradebook = tradebook.sort_values(["Date", "EntryTime"])
    return tradebook

if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\Tradesheet\\Updated_{updated_asof}\\"):
    os.makedirs(rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\Tradesheet\\Updated_{updated_asof}\\")

outputpath = rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\Tradesheet\\Updated_{updated_asof}\\"

lastdate = get_last_updated_date()
data = fetch_latest_data(lastdate,strategy_starttime)
spotdata = fetch_latest_spotdata(lastdate)
equity_5min = convert_eq(spotdata.copy(),timeframe)
sma_df = calculate_sma(equity_5min.copy(),period)
data = seconds_converter(data)
spotdata = seconds_converter(spotdata)

strategy_starttime = '11:00:00'
strategy_endtime = '13:00:00'
start_time = datetime.strptime('11:00:00','%H:%M:%S')
end_time = datetime.strptime('13:00:00','%H:%M:%S')
while start_time <= end_time:
    time1 = {'11:00:00':'1100','11:05:00':'1105','11:10:00':'1110','11:15:00':'1115','11:20:00':'1120','11:25:00':'1125','11:30:00':'1130','11:35:00':'1135','11:40:00':'1140','11:45:00':'1145','11:50:00':'1150','11:55:00':'1155','12:00:00':'1200','12:05:00':'1205','12:10:00':'1210','12:15:00':'1215','12:20:00':'1220','12:25:00':'1225','12:30:00':'1230','12:35:00':'1235','12:40:00':'1240','12:45:00':'1245','12:50:00':'1250','12:55:00':'1255','13:00:00':'1300'}[start_time.strftime('%H:%M:%S')]
    print("Running backtest for strategy start time:", start_time.strftime('%H:%M:%S'))
    atmdf = strikeselector(data.copy(),spotdata.copy(),start_time.strftime('%H:%M:%S'))
    atmdf['Time'] = pd.to_datetime(atmdf['Time']).dt.time

    to_workdf = directional_sma_generator(atmdf.copy(),sma_df.copy(),data.copy())
    to_workdf = to_workdf[(to_workdf['Time'] >= start_time.strftime('%H:%M:%S')) & (to_workdf['Time'] <= endtime)].sort_values(by=['Date','Time']).reset_index(drop=True)
    finalsignals = directional_signal_generator(to_workdf.copy(),stoploss)
    tradebook = tradebook_new(finalsignals)
    tradebook.to_csv(os.path.join(outputpath,f'{index}Tradebook_{time1}.csv'),index=False)
    start_time += interval

print("BACKTEST UPDATED!!")
print("GENERATING NAVs for DIRECTIONAL STRATEGY")

def generate_equitycurve(start_time,index):
    column_name = f"NAV_{start_time.strftime('%H%M')}"
    lastnavfilepath = rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\NAV\Updated_{lastupdated_asof}\\{index}_{start_time.strftime('%H%M')}_NAV.xlsx"
    lastnavfile = pd.read_excel(lastnavfilepath)
    equitycurve = pd.read_csv(fr"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\Tradesheet\Updated_{updated_asof}\{index}Tradebook_{start_time.strftime('%H%M')}.csv")
    equitycurve['DaySum'] = equitycurve.groupby('Date')['PnL_after_commission'].transform('sum')
    equitycurve_unique = equitycurve.drop_duplicates(subset=['Date']).reset_index(drop=True)
    equitycurve_unique['DayPnL%'] = round((equitycurve_unique['DaySum'] / equitycurve_unique['Close_EQ']) * 100,3)
    equitycurve_unique['DayPnL%'] = round((equitycurve_unique['DayPnL%'])  - ((12 / (equitycurve_unique['Close_EQ']*15)) * 100),3)
    equitycurve_unique.loc[0,column_name] = round(lastnavfile[column_name].iloc[-1] * (1+equitycurve_unique.loc[0,'DayPnL%']/100),2)
    for i in range(1,len(equitycurve_unique)):
        equitycurve_unique.loc[i,column_name] = round(equitycurve_unique.loc[i-1,column_name] * (1+equitycurve_unique.loc[i,'DayPnL%']/100),3)

    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\Directional\\{index}\NAV\Updated_{updated_asof}\\{index}_{start_time.strftime('%H%M')}_NAV.xlsx"):
        equitycurve_unique[['Date',column_name]].to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\Directional\\{index}\NAV\Updated_{updated_asof}\\{index}_{start_time.strftime('%H%M')}_NAV.xlsx")
    return equitycurve_unique[['Date',column_name]]

def get_daily_nav(index):
    
    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\NAV\Updated_{updated_asof}\\"):
        os.mkdir(rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\NAV\Updated_{updated_asof}\\")
    
    strategy_starttime = '11:00:00'
    strategy_endtime = '13:00:00'
    start_time = datetime.strptime(strategy_starttime,'%H:%M:%S')
    end_time = datetime.strptime(strategy_endtime,'%H:%M:%S')

    delta = timedelta(minutes=5)
    finaldf = pd.DataFrame()
    while start_time <= end_time:
        equitycurve = generate_equitycurve(start_time,index)
        equitycurve = equitycurve.loc[:, ~equitycurve.columns.str.contains('^Unnamed')]
        suffix = f"NAV_{start_time.strftime('%H%M')}"
        if start_time.strftime('%H%M') == '1100':
            finaldf = pd.concat([finaldf,equitycurve],ignore_index=True)
            finaldf[suffix] = finaldf[suffix].fillna(method='ffill')
        else:
            finaldf = pd.merge(finaldf,equitycurve[['Date',suffix]],on=['Date'],how='left',suffixes=['',f'_{suffix}'])
            finaldf[suffix] = finaldf[suffix].fillna(method='ffill')
        start_time += delta
    
    finaldf.to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\Directional\{index}\\Combined\{index}Directional_CombinedNAV_{updated_asof}.xlsx")
    
get_daily_nav(index)

