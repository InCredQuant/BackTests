import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")

# Input Parameters
last_updated_date = '2025-01-28'
date_obj = datetime.strptime(last_updated_date, '%Y-%m-%d')
lastupdated_asof = date_obj.strftime('%d%m%Y')

enddate = '2025-02-20'
date_obj = datetime.strptime(enddate, '%Y-%m-%d')
updated_asof = date_obj.strftime('%d%m%Y')
strategy_starttime = '09:45:59'
strategy_endtime = '15:05:59'
endtime = '15:05:59'
interval = timedelta(minutes=5)
stoploss = float(0.25)
stoploss_flag = 'Y'
trailingstoploss = float(0.30)
sl_method = 'hard'                    # hard / trailing
index = 'Nifty'

tradesheetpath = rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\Tradesheet\\Updated_{lastupdated_asof}\\"
navpath = rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\NAV\\Updated_{lastupdated_asof}\\"

def connection_details():
    conn = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    return conn

def get_last_updated_date():
    last_updated_nav_sheet = pd.read_excel(os.path.join(navpath,f'{index}_0945_NAV.xlsx'))
    last_updated_nav_sheet['Date'] = pd.to_datetime(last_updated_nav_sheet['Date'])
    lastdate = last_updated_nav_sheet['Date'].dt.date.max()
    return lastdate

def fetch_latest_data(lastdate):
    connection = connection_details()
    sqlquery = f'''
        SELECT * FROM weekly_gdfl_min_opt
        WHERE "Date" > '{lastdate}' 
        AND "Label" = 'W1' 
        AND "Name" = '{index.upper()}'
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

def strikeselector(df,strategy_starttime):
    df = df[df['Time'] == strategy_starttime]
    df['Difference'] = df['StrikePrice'] - df['Close_EQ']
    df['Difference'] = df['Difference'].abs()
    df = df.dropna(subset = ['Close_EQ'])
    result_df = df.loc[df.groupby(['Date', 'Call_Or_Put'])['Difference'].idxmin()]
    return result_df

def shortstraddle_signal_generator(df,strikeselectordf,stoploss,entrytime,exittime):
    data = pd.merge(df,strikeselectordf[['Ticker','Date','Difference']],on=['Ticker','Date'],how='inner')
    data = data[(data['Time'] >= entrytime) & (data['Time'] <= exittime)]
    data['CumTimeCount'] = data.groupby(['Date', 'Ticker']).cumcount()
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == 0), 'Sell', '')
    data['BuySellFlag'] = np.where((data['CumTimeCount'] == data.groupby(['Date', 'Ticker'])['CumTimeCount'].transform('max')), 'Buy', data['BuySellFlag'])
    data['EntryPrice'] = np.where(data['BuySellFlag'] == 'Sell', data['Close'], np.nan)
    data['EntryPrice'] = data.groupby(['Date', 'Ticker'])['EntryPrice'].transform('last')
    data['Stoploss'] = np.where((data['High'] >= (data['EntryPrice'] * (1 + stoploss))) & (data["CumTimeCount"] != 0), 'Triggered', '')
    data['StoplossPrice'] = np.where(data['Stoploss'] == 'Triggered', (data['EntryPrice'] * (1 + stoploss)), '')
    data['stoploss_rn'] = data.groupby(['Date', 'Ticker', 'Stoploss'])['Time'].rank().astype(int)
    data.loc[(data['stoploss_rn'] != 1) | (data['Stoploss'] != "Triggered"), 'stoploss_rn'] = np.nan
    data['BuySellFlag'] = np.where(data["stoploss_rn"] == 1, 'Buy', data['BuySellFlag'])
    signals_df = data[(data["BuySellFlag"] == "Buy") | (data["BuySellFlag"] == "Sell")]
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

def preprocess():
    lastdate = get_last_updated_date()
    data = fetch_latest_data(lastdate)
    spotdata = fetch_latest_spotdata(lastdate)
    df = pd.merge(data,spotdata[['Date','Time','Close']],on=['Date','Time'],how='left',suffixes=['','_EQ'])
    return df

def backtest(df,strategy_starttime):
    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\Tradesheet\Updated_{updated_asof}\\"):
        os.mkdir(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\Tradesheet\Updated_{updated_asof}\\")
    
    print("Strategy start time: ",strategy_starttime)
    time_obj = datetime.strptime(strategy_starttime, '%H:%M:%S')
    time1 = time_obj.strftime('%H%M')
    strikeselectordf = strikeselector(df.copy(), strategy_starttime)
    signaldf = shortstraddle_signal_generator(df.copy(), strikeselectordf.copy(), stoploss, strategy_starttime, strategy_endtime)
    tradebook = tradebook_new(signaldf.copy())
    tradebook.to_csv(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\Tradesheet\Updated_{updated_asof}\\{index}_ShortStraddle{time1}_Tradebook.csv",index=False)

print("Starting backtest..")
df = preprocess()
start_time = datetime.strptime('09:45:59', '%H:%M:%S')
end_time = datetime.strptime('13:00:59', '%H:%M:%S')
step = timedelta(minutes=5)
current_time = start_time
while current_time <= end_time:
    backtest(df.copy(),current_time.strftime('%H:%M:%S'))
    current_time += step

print("BACKTEST UPDATED!!")
print("GENERATING NAVs for STRADDLE STRATEGY")

def generate_equitycurve(time1,index):
    column_name = f'NAV_{time1}'
    lastnavfilepath = rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\NAV\Updated_{lastupdated_asof}\{index}_{time1}_NAV.xlsx"
    lastnavfile = pd.read_excel(lastnavfilepath)
    filepath = fr"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\Tradesheet\Updated_{updated_asof}\{index}_ShortStraddle{time1}_Tradebook.csv"
    equitycurve = pd.read_csv(filepath)
    equitycurve['DaySum'] = equitycurve.groupby(['Date'])['PnL_after_commission'].transform('sum')
    equitycurve_unique = equitycurve.drop_duplicates(subset=['Date']).reset_index(drop=True)
    equitycurve_unique['DayPnL%'] = round((equitycurve_unique['DaySum'] / equitycurve_unique['Close_EQ']) * 100,2)
    equitycurve_unique.loc[0,column_name] = round(lastnavfile[column_name].iloc[-1] * (1+equitycurve_unique.loc[0,'DayPnL%']/100),2)
    for i in range(1,len(equitycurve_unique)):
        equitycurve_unique.loc[i,column_name] = round(equitycurve_unique.loc[i-1,column_name] * (1+equitycurve_unique.loc[i,'DayPnL%']/100),2)

    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\NAV\Updated_{updated_asof}\\{index}_{time1}_NAV.xlsx"):
        equitycurve_unique[['Date',column_name]].to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\NAV\Updated_{updated_asof}\\{index}_{time1}_NAV.xlsx")
    return equitycurve_unique[['Date',column_name]]

def get_daily_nav(index):
    
    if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\NAV\Updated_{updated_asof}\\"):
        os.mkdir(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\NAV\Updated_{updated_asof}\\")
    finaldf = pd.DataFrame()
    lastdate = get_last_updated_date()
    spotdata = fetch_latest_spotdata(lastdate)
    dailyeqdf = spotdata[['Date']].drop_duplicates()
    dailyeqdf = dailyeqdf.sort_values(by=['Date']).reset_index(drop=True)
    start_time = datetime.strptime('09:45:59', '%H:%M:%S')
    end_time = datetime.strptime('13:00:59', '%H:%M:%S')
    step = timedelta(minutes=5)
    current_time = start_time
    while current_time <= end_time:
        # time_obj = datetime.strptime(current_time, '%H:%M:%S')
        time1 = current_time.strftime('%H%M')
        column_name = f'NAV_{time1}'
        equitycurveunique = generate_equitycurve(time1,index)
        equitycurveunique['Date'] = pd.to_datetime(equitycurveunique['Date']).dt.date
        if time1 == '0945':
            finaldf = pd.merge(dailyeqdf,equitycurveunique[['Date',column_name]],on=['Date'],how='left')
            finaldf[column_name] = finaldf[column_name].fillna(method='ffill')
        else:
            finaldf = pd.merge(finaldf,equitycurveunique[['Date',column_name]],on=['Date'],how='left')
            finaldf[column_name] = finaldf[column_name].fillna(method='ffill')
        current_time += step
    
    finaldf.to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\ShortStraddle\Outputs\{index}\Combined\\{index}_Straddle_CombinedNAV_{updated_asof}.xlsx")

get_daily_nav(index)
