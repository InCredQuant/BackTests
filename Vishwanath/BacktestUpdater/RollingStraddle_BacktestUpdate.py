import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
import sys
sys.path.insert(0,r'G:\Shared drives\BackTests\pycode\DBUpdation')
import pg_redirect
from date_config import *

# Input Parameters
# last_updated_date = '2025-03-24' ## Replace with last updated date
# date_obj = datetime.strptime(last_updated_date, '%Y-%m-%d')
# lastupdated_asof = date_obj.strftime('%d%m%Y')

# enddate = '2025-04-25' ## Replace with date you are updating to
# date_obj = datetime.strptime(enddate, '%Y-%m-%d')
# updated_asof = date_obj.strftime('%d%m%Y')
strategy_starttime = '09:30:59'
strategy_endtime = '13:00:59'
endtime = '15:25:59'
interval = timedelta(minutes=15)
timeframe = '5'
reentry_count = 5
reentry_condition = 'Y' # Y or N
no_reentry_after = '15:00:59'
index = 'Nifty' #BankNifty/Nifty/FinNifty
bips = [40,50,60]

def get_last_updated_date(bip=40):
    navpath = rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Updated_{lastupdated_asof}\\{bip}bips"
    print(f"Fetching last available date for {bip} bips..")
    last_updated_nav_sheet = pd.read_excel(os.path.join(navpath,f'{index}_0930_NAV.xlsx'))
    lastdate = pd.to_datetime(last_updated_nav_sheet['Date']).dt.date.max()
    return lastdate

def fetch_latest_data(lastdate,strategy_starttime):
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    sqlquery = f'''
        SELECT * FROM weekly_gdfl_min_opt
        WHERE "Date" > '{lastdate}' 
        AND "Label" = 'W1' 
        AND "Name" = '{index.upper()}'
        AND "Time" >= '{strategy_starttime}'
        AND "Ticker" NOT LIKE '%-I%'; '''
    rawdata = pd.read_sql(sqlquery,connection)
    connection.close()
    rawdata['ExpiryDate'] = pd.to_datetime(rawdata['ExpiryDate'])
    rawdata = rawdata[rawdata['Date'] == rawdata['ExpiryDate']]
    return rawdata

def fetch_latest_spotdata(lastdate):
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    sql_query = f'''
        SELECT * FROM spotdata
        WHERE "Symbol" = '{index.upper()}' 
        AND "Date" > '{lastdate}'; '''
    eqdata = pd.read_sql(sql_query,connection)
    connection.close()
    return eqdata

## Strike selection based on ATM - for reentry
def strikeselector_reentry(df):
    df['Difference'] = df['StrikePrice'] - df['Close_EQ']
    df['Difference'] = df['Difference'].abs()
    df = df.dropna(subset = ['Close_EQ'])
    result_df = df.loc[df.groupby(['Date', 'Call_Or_Put'])['Difference'].idxmin()]
    return result_df

## Strike selection - for starttime - first entry
def strikeselector(df,strategy_starttime):
    df = df[df['Time'] == strategy_starttime]
    df['Difference'] = df['StrikePrice'] - df['Close_EQ']
    df['Difference'] = df['Difference'].abs()
    df = df.dropna(subset = ['Close_EQ'])
    result_df = df.loc[df.groupby(['Date', 'Call_Or_Put'])['Difference'].idxmin()]
    return result_df

def rollingstraddle_signal_generator(df,strikeselectordf,entrytime,exittime,bips):
    data = pd.merge(df,strikeselectordf[['Ticker','Date','Difference']],on=['Ticker','Date'],how='inner')
    data = data[(pd.to_datetime(data['Time']).dt.time >= entrytime) & (data['Time'] <= exittime)].sort_values(by=['Date','Time'])
    data['CumTimeCount'] = data.groupby(['Date', 'Ticker']).cumcount()
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == 0), 'Sell', '')
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == data.groupby(['Date', 'Ticker'])['CumTimeCount'].transform('max')), 'Buy', data['BuySellFlag'])
    data['EntryPrice'] = np.where(data['BuySellFlag'] == 'Sell', data['Close'], np.nan)
    data['EntryPrice'] = data.groupby(['Date', 'Ticker'])['EntryPrice'].transform('last')
    data['PrevClose_EQ'] = np.where(data['BuySellFlag'] == '',data.groupby(['Date','Ticker'])['Close_EQ'].transform('first'),np.nan)
    data['Stoploss'] = np.where(abs(((data['Close_EQ'] / data['PrevClose_EQ']) - 1) * 100) >= bips/100,'Triggered','')
    data['StoplossPrice'] = np.where(data['Stoploss'] == 'Triggered', data['Close'], '')
    data['stoploss_rn'] = data.groupby(['Date', 'Ticker', 'Stoploss'])['Time'].rank().astype(int)
    data.loc[(data['stoploss_rn'] != 1) | (data['Stoploss'] != "Triggered"), 'stoploss_rn'] = np.nan
    data['BuySellFlag'] = np.where(data["stoploss_rn"] == 1, 'Buy', data['BuySellFlag'])
    signals_df = data[(data["BuySellFlag"] == "Buy") | (data["BuySellFlag"] == "Sell")]
    signals_df['buysell_rn'] = signals_df.groupby(['Date', 'Ticker', 'BuySellFlag'])['Time'].rank().astype(int)
    signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
    signals_df["Position"] = 1
    return signals_df

def tradebook_generator(signals_df):
    commission = 1
    brokerage = 6
    signals_df = signals_df[['Ticker', 'Date', 'Time', 'Close', 'Call_Or_Put', 'BuySellFlag', 'Stoploss', 'StoplossPrice', "Position","Close_EQ"]]
    entry_df = signals_df[signals_df["BuySellFlag"] == "Sell"].rename(columns = {"Time": "EntryTime", "Close": "EntryPrice"}).drop(["BuySellFlag","Stoploss", "StoplossPrice"], axis = 1)
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

def rollingstraddle_signal_generator_reentry(data,exittime,bips):    
    data = data.reset_index().sort_values("Time")
    df = data[data["Time"] == data["Reentry_Time"]]
    strikeselectordf = strikeselector_reentry(df)
    merged_data = pd.merge(data, strikeselectordf[['Ticker','Date']], on = ["Ticker", "Date"], how = "inner")
    merged_data = merged_data[(merged_data['Time'] <= exittime)].sort_values(by=['Date','Time'])
    merged_data['CumTimeCount'] = merged_data.groupby(['Date', 'Ticker']).cumcount()
    merged_data['BuySellFlag'] = np.where((merged_data['CumTimeCount'] == 0), 'Sell', '')
    merged_data['BuySellFlag'] = np.where((merged_data['CumTimeCount'] == merged_data.groupby(['Date', 'Ticker'])['CumTimeCount'].transform('max')), 'Buy', merged_data['BuySellFlag'])
    merged_data['EntryPrice'] = np.where(merged_data['BuySellFlag'] == 'Sell', merged_data['Close'], np.nan)
    merged_data['EntryPrice'] = merged_data.groupby(['Date', 'Ticker'])['EntryPrice'].transform('last')
    merged_data['PrevClose_EQ'] = np.where(merged_data['BuySellFlag'] == '',merged_data.groupby(['Date','Ticker'])['Close_EQ'].transform('first'),np.nan)
    merged_data['Stoploss'] = np.where(abs(((merged_data['Close_EQ'] / merged_data['PrevClose_EQ']) - 1) * 100) >= bips/100,'Triggered','')
    merged_data['StoplossPrice'] = np.where(merged_data['Stoploss'] == 'Triggered', merged_data['Close'], '')

    merged_data['stoploss_rn'] = merged_data.groupby(['Date', 'Ticker', 'Stoploss'])['Time'].rank().astype(int)
    merged_data.loc[(merged_data['stoploss_rn'] != 1) | (merged_data['Stoploss'] != "Triggered"), 'stoploss_rn'] = np.nan
    merged_data['BuySellFlag'] = np.where(merged_data["stoploss_rn"] == 1, 'Buy', merged_data['BuySellFlag'])
    
    signals_df = merged_data[(merged_data["BuySellFlag"] == "Buy") | (merged_data["BuySellFlag"] == "Sell")]
    signals_df['buysell_rn'] = signals_df.groupby(['Date', 'Ticker', 'BuySellFlag'])['Time'].rank().astype(int)
    signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
    return signals_df

def reentry(signalsdf,testdf,reentry_count,entrytime,exittime,bips):
    
    dummy_date = datetime(2019, 2, 11)  # You can choose any date here
    exittime = datetime.strptime(exittime,'%H:%M:%S').time()
    exit_datetime = datetime.combine(dummy_date, exittime)
    exit_datetime = (exit_datetime - timedelta(hours=0)).time()
    for i in range(0,reentry_count):
        # print("Re-entry ", i+1)
        if i == 0:
            signalsdf["CumTimeCount"] = signalsdf.groupby("Date")["Time"].cumcount()
            reentry_time = signalsdf[signalsdf["CumTimeCount"] == 3][["Date", "Time"]].reset_index(drop = True)
        else:
            reentry_signals_df["CumTimeCount"] = reentry_signals_df.groupby("Date")["Time"].cumcount()
            reentry_time = reentry_signals_df[reentry_signals_df["CumTimeCount"] == 3][["Date", "Time"]].reset_index(drop = True)
        reentry_time = reentry_time[(pd.to_datetime(reentry_time["Time"]).dt.time != exittime) & (pd.to_datetime(reentry_time["Time"]).dt.time <= exit_datetime)].rename(columns = {"Time": "Reentry_Time"})
        reentry_merged_data = pd.merge(reentry_time, testdf, on = "Date", how = "left")
        reentry_merged_data = reentry_merged_data[reentry_merged_data["Time"] >= reentry_merged_data["Reentry_Time"]].reset_index(drop=True)
        if (i == 0) & (len(reentry_merged_data) == 0):
            signals_df_main = signalsdf.copy()
            break
        
        if len(reentry_merged_data) != 0:
            reentry_signals_df = rollingstraddle_signal_generator_reentry(reentry_merged_data, endtime,bips)
            reentry_signals_df["Position"] = i+2
            if i == 0:
                signals_df_main = pd.concat([signalsdf, reentry_signals_df], ignore_index = True)
            else:
                signals_df_main = pd.concat([signals_df_main, reentry_signals_df], ignore_index = True)
        else:
            print("No Re-entries possible")
            break
        
    return signals_df_main

def backtest(strategy_starttime,strategy_endtime,endtime,bips):
    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\Tradesheet\\Updated_{updated_asof}\\"):
        os.makedirs(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\Tradesheet\\Updated_{updated_asof}\\")
    
    folderpath = rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\\{index}\Tradesheet\\Updated_{updated_asof}\\"

    lastdate = get_last_updated_date() # can enter any of the above bip range numbers as this is just to get the last updated date (assuming everything is updated together)
    data = fetch_latest_data(lastdate,strategy_starttime)
    spotdata = fetch_latest_spotdata(lastdate)
    # data = pd.read_pickle(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index.capitalize()}\{index.capitalize()}optdf.pkl")
    # spotdata = pd.read_pickle(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index.capitalize()}\{index.capitalize()}eqdf.pkl")
    df = pd.merge(data,spotdata[['Date','Time','Close']],on=['Date','Time'],how='left',suffixes=['','_EQ'])
    iteration_starttime = datetime.strptime(strategy_starttime, '%H:%M:%S')
    iteration_endtime = datetime.strptime(strategy_endtime, '%H:%M:%S')
    delta = timedelta(minutes=15)
    while iteration_starttime <= iteration_endtime: ## to run the backtest from strategy defined starttime to endtime
        print("Running backtest for strategy start time:", iteration_starttime.strftime('%H:%M:%S'))
        time1 = iteration_starttime.strftime('%H%M')
        iteration_starttime_time = iteration_starttime.time()
        ssdf = strikeselector(df.copy(),iteration_starttime.strftime("%H:%M:%S"))
        for bip in bips:
            if not os.path.exists(os.path.join(folderpath,f"{bip}bips")):
                os.makedirs(os.path.join(folderpath,f"{bip}bips"))
            
            print(f"Iterating for {bip} bips..")
            signalsdf = rollingstraddle_signal_generator(df.copy(),ssdf.copy(),iteration_starttime_time,endtime,bip)
            signalsdf = reentry(signalsdf, df.copy(), reentry_count, iteration_starttime_time, endtime,bip)
            tradebook = tradebook_generator(signalsdf.copy()).drop_duplicates().reset_index(drop=True)
            tradebook.to_csv(os.path.join(folderpath,f"{bip}bips",f"{index}_Rolling{bip}bips_{time1}.csv"),index=False)
        iteration_starttime += delta
    
print(f"Running {index.capitalize()} Rolling Straddle Backtest..")
backtest(strategy_starttime,strategy_endtime,endtime,bips)

print("BACKTEST UPDATED!!")
print("GENERATING NAVs for ROLLING STRADDLE STRATEGY")

def generate_equitycurve(index,bip,start_time):
    column_name = f"NAV_{start_time.strftime('%H%M')}"
    last_nav_file_path = rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\\Updated_{lastupdated_asof}\{bip}bips\{index}_{start_time.strftime('%H%M')}_NAV.xlsx"
    lastnavfile = pd.read_excel(last_nav_file_path)
    filepath = fr"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\\Outputs\\{index}\\Tradesheet\\Updated_{updated_asof}\{bip}bips\{index}_Rolling{bip}bips_{start_time.strftime('%H%M')}.csv"
    equitycurve = pd.read_csv(filepath)
    equitycurve['DaySum'] = equitycurve.groupby(['Date','Position'])['PnL_after_commission'].transform('sum')
    equitycurve['PositionPnL%'] = round((equitycurve['DaySum'] / equitycurve['Close_EQ']) * 100,2)
    equitycurve = equitycurve.drop_duplicates(subset=['Date','Position'])
    equitycurve['DayPnL%'] = equitycurve.groupby('Date')['PositionPnL%'].transform('sum')
    equitycurve_unique = equitycurve.drop_duplicates(subset=['Date']).reset_index(drop=True)
    # print(lastnavfile['NAV'].iloc[-1],equitycurve_unique.loc[0,'DayPnL%']/100)
    equitycurve_unique.loc[0,column_name] = round(lastnavfile[column_name].iloc[-1] * (1+equitycurve_unique.loc[0,'DayPnL%']/100),2)
    for i in range(1,len(equitycurve_unique)):
        equitycurve_unique.loc[i,column_name] = round(equitycurve_unique.loc[i-1,column_name] * (1+equitycurve_unique.loc[i,'DayPnL%']/100),2)
    
    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Updated_{updated_asof}\\{bip}bips\\"):
        os.mkdir(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Updated_{updated_asof}\\{bip}bips\\")
    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Updated_{updated_asof}\\{bip}bips\\{index}_{start_time.strftime('%H%M')}_NAV.xlsx"):
        equitycurve_unique[['Date',column_name]].to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Updated_{updated_asof}\\{bip}bips\\{index}_{start_time.strftime('%H%M')}_NAV.xlsx")
    
    return equitycurve_unique
    
def get_daily_nav(index):
    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Updated_{updated_asof}\\"):
        os.mkdir(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Updated_{updated_asof}\\")
    
    strategy_starttime = '09:30:00'
    strategy_endtime = '13:00:00'
    delta = timedelta(minutes=15)
    lastdate = get_last_updated_date()
    eqdf = fetch_latest_spotdata(lastdate)
    # eqdf = pd.read_pickle(fr"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\{index}eqdf.pkl")
    dailyeqdf = eqdf[['Date']].drop_duplicates()
    bips = [40,50,60]
    for bip in bips:
        start_time = datetime.strptime(strategy_starttime,'%H:%M:%S')
        end_time = datetime.strptime(strategy_endtime,'%H:%M:%S')
        print(f"Running for {bip} bip")
        final_NAVs = pd.DataFrame()
        while start_time <= end_time:
            equitycurve = generate_equitycurve(index,bip,start_time)
            equitycurve = equitycurve.loc[:, ~equitycurve.columns.str.contains('^Unnamed')]
            equitycurve['Date'] = pd.to_datetime(equitycurve['Date']).dt.date
            suffix = f"NAV_{start_time.strftime('%H%M')}"
            finaldf = pd.merge(dailyeqdf,equitycurve[['Date',suffix]],on=['Date'],how='left')
            finaldf = finaldf.sort_values(by='Date')
            finaldf[suffix] = finaldf[suffix].fillna(method='ffill')
            if start_time.strftime('%H%M') == '0930':
                final_NAVs = pd.concat([final_NAVs,finaldf],axis=1)
            else:
                final_NAVs = pd.concat([final_NAVs,finaldf[[suffix]]],axis=1)
            
            start_time += delta
        
        # display(final_NAVs)
        final_NAVs = final_NAVs.reset_index(drop=True)
        if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Combined\Updated_{updated_asof}"):
            os.mkdir(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Combined\Updated_{updated_asof}")
        final_NAVs.to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\RollingStraddle\Outputs\{index}\NAVs\Combined\Updated_{updated_asof}\{index}RollingStraddle_{bip}bips_CombinedNAV_{updated_asof}.xlsx")

get_daily_nav(index.capitalize())