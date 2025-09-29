import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import time
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
from scipy.stats import norm
import sys
sys.path.insert(0, 'G:\\Shared drives\\BackTests\\pycode\\DBUpdation\\')
import pg_redirect
from date_config import *

strike_selected = {0.35:'OTM3',0.4:'OTM2',0.45:'OTM1',0.5:'ATM',0.55:'ITM1',0.6:'ITM2',0.65:'ITM3'}
# Input Parameters
# last_updated_date = '2025-03-24'
# date_obj = datetime.strptime(last_updated_date, '%Y-%m-%d')
# lastupdated_asof = date_obj.strftime('%d%m%Y')

index = 'Nifty'
# enddate = '2025-04-25'
# date_obj = datetime.strptime(enddate, '%Y-%m-%d')
# updated_asof = date_obj.strftime('%d%m%Y')
gapcheck_starttime = '09:15:59'
gapcheck_endtime = '09:30:59'
strike_to_select = 0.55
strategy_starttime = '09:46:59' if gapcheck_endtime == '09:45:59' else '09:31:59'
strategy_endtime = '15:00:59'
interval = timedelta(minutes=15)
stoploss = float(0.3) # for monthly, 0.15 otherwise 0.3
stoploss_flag = 'Y'
target = float(0.70)
gapcheck_from_close_to_open = float(0.25)
target_flag = 'N'
trailingstoploss = float(0.3) # for monthly, 0.15 otherwise 0.3
sl_method = 'trailing' # hard or trailing

navpath = rf"C:\Vishwanath\PythonCodes\Strategy\LongOptions\Nifty\NAVs\GapCheck\915-930\Updated_{lastupdated_asof}\Nifty_TrailingSL_NAV.xlsx"

def calculate_greeks_vectorized(spot, strike, time_to_expiry, option_price, option_type):
    norm_dist = norm.cdf
    rate = 0.065
    dividend = 0.015
    is_call_option = np.where(option_type == 'CE', True, False)
    is_put_option = np.where(option_type == 'PE', True, False)
    target_option = np.where(is_call_option, option_price, 0)
    upper = np.where(is_call_option, 5.0, 0.1)
    lower = np.where(is_call_option, 0.0, 0.0)
    while np.any((upper - lower) > 0.00001):
        d_one = (np.log(spot / strike) + ((rate - dividend + (0.5 * (((upper + lower) / 2) ** 2))) * time_to_expiry)) / (((upper + lower) / 2) * (time_to_expiry ** 0.5))
        d_two = d_one - ((upper + lower) / 2) * time_to_expiry ** 0.5
        nd_one = norm_dist(d_one)
        nd_two = norm_dist(d_two)
        call_premium = (np.exp(-dividend * time_to_expiry) * (spot * nd_one)) - (strike * np.exp(-rate * time_to_expiry) * nd_two)
        mask = call_premium > target_option
        upper = np.where(mask, (upper + lower) / 2, upper)
        lower = np.where(mask, lower, (upper + lower) / 2)
    calldelta1=np.where(is_call_option,(nd_one) * np.exp(-dividend * time_to_expiry),0)
    call_delta=np.where((is_call_option)&(time_to_expiry==0)&(spot > strike), 1.0, calldelta1)
    call_iv = np.where(is_call_option, (upper + lower) / 2, 0.0)
    ndash_one = 1 / (2 * np.pi) ** 0.5 * (np.exp(-(d_one ** 2) / 2))
    call_vega = np.where(is_call_option, spot * ((time_to_expiry ** (1 / 2)) * ndash_one * np.exp(-dividend * time_to_expiry)) / 100, 0.0)
    call_gamma = np.where(is_call_option, ((((1 / np.sqrt((2 * np.pi))) * np.exp(((-1 * (d_one ** 2)) / 2))) * np.exp(((-1 * time_to_expiry) * dividend))) / ((spot * call_iv) * np.sqrt(time_to_expiry))), 0.0)
    call_theta =np.where(is_call_option,((((-1 * ((((spot * ((1 / np.sqrt((2 * np.pi))) * np.exp(((-1 * (d_one ** 2)) / 2)))) * call_iv) * np.exp(((-1 * time_to_expiry) * dividend))) / (2 * np.sqrt(time_to_expiry)))) + ((dividend * spot) * call_delta)) - (((rate * strike) * np.exp(((-1 * rate) * time_to_expiry))) * norm_dist(d_two)))) / 365,0)
    target_option1 = np.where(is_put_option, option_price, 0)
    upper1 = np.where(is_put_option, 5.0, 0.1)
    lower1 = np.where(is_put_option, 0.0, 0.0)
    while np.any((upper1 - lower1) > 0.00001):
        d_one1 = (np.log(spot / strike) + ((rate - dividend + (0.5 * (((upper1 + lower1) / 2) ** 2))) * time_to_expiry)) / (((upper1 + lower1) / 2) * (time_to_expiry ** 0.5))
        d_two1 = d_one1 - ((upper1 + lower1) / 2) * time_to_expiry ** 0.5
        nd_one1 = norm_dist(d_one1)
        nd_two1 = norm_dist(d_two1)
        put_premium =  np.exp(-rate * time_to_expiry) * strike * (norm_dist(-d_two1)) -np.exp(-dividend * time_to_expiry) *  spot* (norm_dist( -d_one1))
        mask1 = put_premium > target_option1
        upper1 = np.where(mask1, (upper1 + lower1) / 2, upper1)
        lower1 = np.where(mask1, lower1, (upper1 + lower1) / 2)
    put_delta1 = np.where(is_put_option, (nd_one1 - 1) * np.exp(-dividend * time_to_expiry), 0.0)
    put_delta=np.where((is_put_option)&(time_to_expiry==0)&(spot > strike), 1.0, put_delta1)
    put_iv = np.where(is_put_option, (upper1 + lower1) / 2, 0.0)
    ndash_one1 = 1 / (2 * np.pi) ** 0.5 * (np.exp(-(d_one1 ** 2) / 2))
    put_vega = np.where(is_put_option, spot * ((time_to_expiry ** (1 / 2)) * ndash_one1 * np.exp(-dividend * time_to_expiry)) / 100, 0.0)
    put_gamma = np.where(is_put_option, ((((1 / np.sqrt((2 * np.pi))) * np.exp(((-1 * (d_one1 ** 2)) / 2))) * np.exp(((-1 * time_to_expiry) * dividend))) / ((spot * put_iv) * np.sqrt(time_to_expiry))), 0.0)
    put_theta =  np.where(is_put_option,(((((-1 * ((((spot * ((1 / np.sqrt((2 * np.pi))) * np.exp(((-1 * d_one1 ** 2) / 2)))) * put_iv) * np.exp(((-1 * time_to_expiry) * dividend)))))/ (2 * np.sqrt(time_to_expiry))) - (((dividend * spot) * norm_dist((-1 * d_one1))) * np.exp(((-1 * time_to_expiry) * dividend)))) + (((rate * strike) * np.exp(((-1 * rate) * time_to_expiry))) * norm_dist((-1 * d_two1))))) / 365,0.0)
    iv= np.where(is_call_option,call_iv,put_iv)
    delta= np.where(is_call_option,call_delta,put_delta)
    gamma= np.where(is_call_option,call_gamma,put_gamma)
    theta= np.where(is_call_option,call_theta,put_theta)
    vega=np.where(is_call_option,call_vega,put_vega)
    return iv, delta, gamma, theta, vega

def greeks_process(eqdf,df):
    time1 = datetime.strptime('15:29:59', '%H:%M:%S').time()
    eqdf = eqdf.rename(columns={'Open' : 'EQ_Open','High' : 'EQ_High','Low' : 'EQ_Low','Close' : 'EQ_Close'})
    eqdf = eqdf[['Date', 'Time', 'EQ_Open', 'EQ_High', 'EQ_Low', 'EQ_Close']]
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    eqdf['Time'] = pd.to_datetime(eqdf['Time']).dt.time
    df['Time'] = pd.to_datetime(df['Time']).dt.time
    df = df.rename(columns={'Open' : 'Adj_Open','High' : 'Adj_High','Low' : 'Adj_Low','Close' : 'Adj_Close'})
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df = df[df['Time'] <= time1]
    df = pd.merge(df,eqdf, on=['Date','Time'], how='left')
    df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str), dayfirst=True,format = '%Y-%m-%d %H:%M:%S')
    df['Expiry_Datetime'] = pd.to_datetime(df['ExpiryDate'].astype(str) + ' ' + '15:30:00')
    df['MTE'] = df['Expiry_Datetime'] - df['Datetime']
    df['MTE'] = df['MTE'].dt.total_seconds()/60
    df['MTE'] = df['MTE']/(365 * 24 * 60)
    st = time.time()
    greeks = calculate_greeks_vectorized(df['EQ_Close'], df['StrikePrice'].astype(float), df['MTE'], df['Adj_Close'], df['Call_Or_Put'])
    print(time.time()-st)
    df['IV'], df['Delta'], df['Gamma'], df['Theta'], df['Vega'] = greeks[0], greeks[1], greeks[2], greeks[3], greeks[4]
    df.rename(columns={'Adj_Open':'Open','Adj_High':'High','Adj_Low':'Low','Adj_Close':'Close'},inplace=True)
    df = df[['Ticker','Date', 'Time', 'Open','High','Low','Close','Volume','Open Interest','EQ_Open','EQ_High','EQ_Low','EQ_Close','ExpiryDate','IV', 'Delta', 'Gamma', 'Theta', 'Vega',"Call_Or_Put",'StrikePrice','Label','TickerTag']]
    df = df.drop_duplicates().reset_index(drop=True)
    print("\nGreeks Generated!")
    return df

def get_last_updated_date():
    last_updated_nav_sheet = pd.read_excel(navpath)
    lastdate = last_updated_nav_sheet['Date'].dt.date.max()
    print(f"Last updated date is {lastdate}")
    return lastdate

def fetch_latest_data(lastdate):
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    sqlquery = f'''
        SELECT * FROM weekly_gdfl_min_opt
        WHERE "Date" > '{lastdate}' 
        AND "Label" = 'W1' 
        AND "Name" = '{index.upper()}'
        AND "Ticker" NOT LIKE '%-I%'; '''
    rawdata = pd.read_sql(sqlquery,connection)
    connection.close()
    print("Raw data fetched..")
    return rawdata

def fetch_latest_spotdata(lastdate):
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    sql_query = f'''
        SELECT * FROM spotdata
        WHERE "Symbol" = '{index.upper()}' 
        AND "Date" > '{lastdate}'; '''
    eqdata = pd.read_sql(sql_query,connection)
    connection.close()
    print("Spot data fetched..")
    return eqdata

def overnight_gap_check(equitydf,gapcheck_from_close_to_open):
    print("Checking gap of : ",gapcheck_from_close_to_open)
    equitydf['Cumcount'] = equitydf.groupby('Date').cumcount()
    eqdf = equitydf[(equitydf['Cumcount'] == 0) | (equitydf['Cumcount'] == equitydf.groupby('Date')['Cumcount'].transform('max'))]
    eqdf = eqdf.sort_values(by=['Date','Time']).reset_index(drop=True)
    eqdf['Change'] = np.where(eqdf['Cumcount'] == 0,round(eqdf['Close'].pct_change()*100,2),np.nan)
    eqdf['Flag'] = np.where((eqdf['Change'] != '') & (abs(eqdf['Change']) > gapcheck_from_close_to_open), 1, 0)
    gapeqdf = eqdf[eqdf['Flag'] == 1]
    return gapeqdf

def long_options_signal_generator(df):
    df = df[df['Time'] <= datetime.strptime(strategy_endtime,'%H:%M:%S').time()]
    df['CumTimeCount'] = df.groupby(['Date','Ticker'])['Time'].cumcount()
    df['BuySellFlag'] = np.where(df['High'] > df.groupby(['Date','Ticker'])['HighestPrice'].transform('first'),'Buy','')
    mask = df['BuySellFlag'].eq('Buy').groupby([df['Date'], df['Ticker']]).cumsum().gt(0)
    df['Flag'] = np.where(mask, 1, 0)
    df['BuySellFlag'] = np.where((df['CumTimeCount'] == df.groupby(['Date','Ticker'])['CumTimeCount'].transform('max')) & (df['Flag'] == 1),'Sell',df['BuySellFlag'])
    df['BuySellFlag_rn'] = df.groupby(['Date','Ticker','BuySellFlag'])['Time'].rank().astype(int)
    df.loc[(df['BuySellFlag_rn'] != 1), 'BuySellFlag_rn'] = np.nan
    signals_df = df[((df['BuySellFlag'] == 'Buy') | (df['BuySellFlag'] == 'Sell')) & (df['BuySellFlag_rn'] == 1)]
    signals_df['EntryTime'] = np.where(signals_df['BuySellFlag'] == 'Buy',signals_df['Time'],np.nan)
    signals_df['EntryPrice'] = np.where(signals_df['BuySellFlag'] == 'Buy',signals_df['Close'],np.nan)
    signals_df['ExitTime'] = np.where(signals_df['BuySellFlag'] == 'Sell',signals_df['Time'],np.nan)
    signals_df['ExitPrice'] = np.where(signals_df['BuySellFlag'] == 'Sell',signals_df['Close'],np.nan)

    signals_df = signals_df.sort_values(by=['Date','Call_Or_Put']).drop_duplicates(subset=['Date','Ticker','Time']).drop(columns=['HighestPrice','CumTimeCount','Flag','BuySellFlag_rn'])
    return signals_df

def stoploss_target(signalsdf,data,stoploss,target,stoploss_flag,target_flag):
    data = data[data['Time'] <= datetime.strptime(strategy_endtime,'%H:%M:%S').time()]
    data = pd.merge(data,signalsdf[signalsdf['BuySellFlag']=='Buy'][['Date','Ticker','EntryTime','EntryPrice']],on=['Date','Ticker'],how='inner')
    data = data[data['Time'] >= data['EntryTime']]
    data['CumCount'] = data.groupby(['Date','Ticker'])['Time'].cumcount()
    data['BuySellFlag'] = np.where((data['CumCount'] == 0), 'Buy', '')
    data['BuySellFlag'] = np.where((data['CumCount'] == data.groupby(['Date', 'Ticker'])['CumCount'].transform('max')), 'Sell', data['BuySellFlag'])    
    data['Stoploss'] = np.where((stoploss_flag == 'Y') & (data['Low'] <= data['EntryPrice'] * (1 - stoploss)) & (data['CumCount'] != 0), 'Triggered' , '')
    data['StoplossPrice'] = np.where((data['Stoploss'] == 'Triggered'), (data['EntryPrice'] * (1 - stoploss)), '')
    data['Target'] = np.where((target_flag == 'Y') & (data['Close'] >= data['EntryPrice'] * (1 + target)) & (data['CumCount'] != 0), 'Profithit' , '')
    data['TargetPrice'] = np.where((data['Target'] == 'Profithit'), (data['EntryPrice'] * (1 + target)), '')
    
    data['Stoploss_rn'] = data.groupby(['Date', 'Ticker', 'Stoploss'])['Time'].rank().astype(int)
    data['Target_rn'] = data.groupby(['Date','Ticker','Target'])['Time'].rank().astype(int)

    data.loc[(data["Stoploss_rn"] != 1) | (data['Stoploss'] != 'Triggered'), 'Stoploss_rn'] = np.nan
    data.loc[(data["Target_rn"] != 1) | (data['Target'] != 'Profithit'), 'Target_rn'] = np.nan

    data['BuySellFlag'] = np.where((data['Stoploss_rn'] == 1) | (data['Target_rn'] == 1), 'Sell' , data['BuySellFlag'])

    signals_df = data[(data["BuySellFlag"] == "Buy") | (data["BuySellFlag"] == "Sell")]
    signals_df['buysell_rn'] = signals_df.groupby(['Date', 'Ticker', 'BuySellFlag'])['Time'].rank().astype(int)
    signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
    signals_df.loc[signals_df["Stoploss"] == "Triggered", "sl_flag"] = "Hard"
    signals_df["Position"] = 1
    return signals_df

def sl_buy_signal_generator(merged_data):
    # Group by 'Date' and calculate the cumulative count of time within each group
    merged_data = merged_data[merged_data['Time'] > merged_data['sl_time']]
    merged_data['CumTimeCount'] = merged_data.groupby(['Date', 'Ticker']).cumcount()
    # Create 'BuySellFlag' column based on conditions
    merged_data['BuySellFlag'] = np.where((merged_data['CumTimeCount'] == merged_data.groupby(['Date', 'Ticker'])['CumTimeCount'].transform('max')), 'Sell', '')
    # Create 'Stoploss' column based on the stoploss condition and 'StoplossPrice' column to hold the stoploss price as exit price
    merged_data['Stoploss'] = np.where(merged_data['Low'] <= merged_data['sl_price'], 'Triggered', '')
    merged_data['StoplossPrice'] = np.where(merged_data['Low'] <= merged_data['sl_price'], merged_data['sl_price'], '')
    merged_data['BuySellFlag'] = np.where(merged_data["Stoploss"] == "Triggered", 'Sell', merged_data['BuySellFlag'])
    signals_df = merged_data[(merged_data["BuySellFlag"] == "Buy") | (merged_data["BuySellFlag"] == "Sell")]
    signals_df['buysell_rn'] = signals_df.groupby(['Date', 'Ticker', 'BuySellFlag'])['Time'].rank().astype(int)
    signals_df = signals_df[signals_df["buysell_rn"] == 1].sort_values(["Date", "Time"])
    return signals_df

def trailing_stoploss(signalsdf2,data,trailingstoploss):
    signalsdf2['sl_time'] = signalsdf2.groupby(['Date','Call_Or_Put'])['Time'].shift(1)
    signalsdf2.loc[signalsdf2['sl_time'].isin([strategy_starttime, strategy_endtime]), 'sl_time'] = np.nan
    signalsdf2["CumTimeCount"] = signalsdf2.groupby(['Date','Call_Or_Put'])['Time'].cumcount()
    new_signals_df = signalsdf2[signalsdf2["sl_time"].isna()]
    trailing_data = signalsdf2[(signalsdf2['CumTimeCount'] == 1) & (~signalsdf2['sl_time'].isna())][['Ticker','Date','sl_time']]
    sl_merged_data = pd.merge(trailing_data, data, on = ["Ticker", "Date"], how = "left")
    sl_merged_data = sl_merged_data[(sl_merged_data["Time"] >= sl_merged_data["sl_time"]) & (sl_merged_data['Time'] <= datetime.strptime(strategy_endtime,'%H:%M:%S').time()) ].reset_index(drop = True)
    sl_merged_data["sl_price"] = sl_merged_data.groupby(['Date','Call_Or_Put'])['Close'].transform('first') * (1 - trailingstoploss)
    sl_dates = sl_merged_data["Date"].unique()
    for date in sl_dates:
        sl_date_data = sl_merged_data[sl_merged_data["Date"] == date].reset_index(drop = True)
        for name, group in sl_date_data.groupby('Call_Or_Put'):
            high = group["High"].iloc[0]
            for index, row in group.iterrows():
                if row["High"] > high:
                    high = row["High"]
                    group.loc[index:, "sl_price"] = np.maximum(row["Close"] * (1 - trailingstoploss),group.loc[index-1, "sl_price"])
            sl_date_data.loc[group.index[0]:group.index[-1],'sl_price'] = group['sl_price']
        sl_signals_date_df = sl_buy_signal_generator(sl_date_data)
        sl_signals_date_df.loc[sl_signals_date_df["Stoploss"] == "Triggered", "sl_flag"] = "trailing"
        sl_signals_date_df["Position"] = 1
        new_signals_df = pd.concat([new_signals_df, sl_signals_date_df], ignore_index=True)
    new_signals_df = new_signals_df.sort_values(["Date", "Time"]).reset_index(drop = True)
    return new_signals_df

def trailing_sl_signal_generator(selectedstrikedf,equitydf,stoploss,trailing_sl,target,stoploss_flag,target_flag,gapcheck_from_close_to_open):
    
    overnight_gap = overnight_gap_check(equitydf,gapcheck_from_close_to_open)
    selectedstrikedf = pd.merge(selectedstrikedf,overnight_gap[['Date','Flag']],on=['Date'],how='inner')
    signalsdf = long_options_signal_generator(selectedstrikedf.copy())
    signalsdf = stoploss_target(signalsdf.copy(),selectedstrikedf.copy(),stoploss,target,stoploss_flag,target_flag).drop_duplicates(subset=['Date','Time','Ticker'])
    newsignalsdf = trailing_stoploss(signalsdf.copy(),selectedstrikedf.copy(),trailing_sl)
    return newsignalsdf

def tradebookgenerator(signals_df):
    commission = 1
    signals_df = signals_df[['Ticker', 'Date', 'Time', 'Close', 'Call_Or_Put', 'BuySellFlag', 'Stoploss', 'StoplossPrice','sl_flag', "Position","EQ_Close"]]
    entry_df = signals_df[signals_df["BuySellFlag"] == "Buy"].rename(columns = {"Time": "EntryTime", "Close": "EntryPrice"}).drop(["BuySellFlag","Stoploss", "StoplossPrice", "sl_flag"], axis = 1)
    exit_df = signals_df[signals_df["BuySellFlag"] == "Sell"].rename(columns = {"Time": "ExitTime", "Close": "ExitPrice"}).drop(["BuySellFlag", "Call_Or_Put","EQ_Close"], axis = 1)
    exit_df.loc[exit_df["Stoploss"] == "Triggered", "ExitPrice"] = pd.to_numeric(exit_df["StoplossPrice"], errors='coerce')
    exit_df = exit_df.drop(["StoplossPrice"], axis = 1)
    tradebook = pd.merge(entry_df, exit_df, on = ["Ticker", "Date", "Position"], how = "left")
    tradebook["PositionType"] = 'Long'
    tradebook['GrossPnL'] = tradebook['ExitPrice'] - tradebook['EntryPrice']
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * (commission/100))
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['EQ_Close']) * 100,2)
    tradebook = tradebook.sort_values(["Date", "EntryTime"])
    return tradebook

def strike_selector(df,deltavalues):
    delta_list = sorted([deltavalues])
    df['Difference'] = abs(df['EQ_Close'] - df['StrikePrice'].astype(float))
    df['Min'] = df.groupby(['Date', 'Time', 'Call_Or_Put'])['Difference'].transform('min')
    df = df.dropna(subset = ['EQ_Open'])
    min_index = df.groupby(['Date', 'Time', 'Call_Or_Put'])['Difference'].idxmin()
    atm_df = df.loc[min_index, ['Date', 'Time', 'Call_Or_Put', 'StrikePrice']]
    atm_df.rename(columns={'StrikePrice': 'At_The_Money'}, inplace=True)
    df = pd.merge(df, atm_df, on=['Date', 'Time', 'Call_Or_Put'], how='left')
    df['At_The_Money'].fillna(np.nan, inplace=True)
    # Loop through different delta values
    for delta in delta_list:
        if delta == 0.50:
            df.rename(columns={'Min' : f'Delta_{delta*100:.0f}_Diff_Min',
                                        'Difference' : f'Delta_{delta*100:.0f}_Diff'}, inplace=True)
            df[f'Delta_{delta*100:.0f}_Strike'] = df['At_The_Money']
        if delta != 0.50:
            # Adjust delta values for 'PE' option type
            delta_values = df['Delta'].copy()
            delta_values[df['Call_Or_Put'] == 'PE'] *= -1
            # Calculate absolute difference for each delta value
            df[f'Delta_{delta*100:.0f}_Diff'] = abs(delta_values - delta)
            # Calculate minimum difference for each group
            df[f'Delta_{delta*100:.0f}_Diff_Min'] = df.groupby(['Date', 'Time', 'Call_Or_Put'])[f'Delta_{delta*100:.0f}_Diff'].transform('min')
            mask = df[f'Delta_{delta*100:.0f}_Diff'] == df[f'Delta_{delta*100:.0f}_Diff_Min']
            delta_strike = df[mask].groupby(['Date', 'Time', 'Call_Or_Put'])['StrikePrice'].max()
            df[f'Delta_{delta*100:.0f}_Strike'] = df.set_index(['Date', 'Time', 'Call_Or_Put']).index.map(delta_strike)
        df = df.rename(columns={f'Delta_{delta*100:.0f}_Strike':'DeltaSelected'})
    columns = [col for col in df.columns if not '_diff' in col.lower()]
    df = df[columns]
    return df

def long_options_strike_selector(df,gapcheck_starttime,gapcheck_endtime,strike_to_select,strategy_starttime,strategy_endtime):
    ## option data for taking a position
    positionsdf = df[(df['Time'] >= datetime.strptime(strategy_starttime,'%H:%M:%S').time()) & (df['Time'] <= datetime.strptime(strategy_endtime,'%H:%M:%S').time())].sort_values(by=['Call_Or_Put','Time'])
    ## to check particular delta at 09:15
    gapcheckdf = df[(df['Time'] == datetime.strptime(gapcheck_starttime,'%H:%M:%S').time())]
    ## to check for the highest price within the time range
    gaprangecheckdf = df[(df['Time'] >= datetime.strptime(gapcheck_starttime,'%H:%M:%S').time()) & (df['Time'] <= datetime.strptime(gapcheck_endtime,'%H:%M:%S').time())]
    
    gapdf = strike_selector(gapcheckdf.copy(),strike_to_select)
    # gapdf = gapdf[gapdf['StrikePrice'] == gapdf['DeltaSelected']].drop(columns=['Difference','Min']).sort_values(by=['Call_Or_Put','Time'])
    gapdf = gapdf[gapdf['StrikePrice'] == gapdf['DeltaSelected']].sort_values(by=['Call_Or_Put','Time'])
    gapdf = pd.merge(gaprangecheckdf,gapdf[['Date','Ticker']],on=['Date','Ticker'],how='inner')
    gapdf['HighestPrice'] = gapdf.groupby(['Date','Call_Or_Put'])['High'].transform('max')
    maxdf = gapdf[gapdf['High'] == gapdf['HighestPrice']].sort_values(by=['Date'])
    positionsdf = pd.merge(positionsdf,maxdf[['Date','Ticker','HighestPrice']],on=['Date','Ticker'],how='inner')
    # return maxdf,positionsdf
    return positionsdf

if __name__ == '__main__':
    print("Updating Range Breakout backtest..")
    lastdate = get_last_updated_date()
    data = fetch_latest_data(lastdate)
    spotdata = fetch_latest_spotdata(lastdate)
    df = greeks_process(spotdata.copy(),data.copy())
    gapcheck_from_close_to_open = float(0.25)
    while(gapcheck_from_close_to_open <= 0.4):
        selectedstrikedf = long_options_strike_selector(df.copy(),gapcheck_starttime,gapcheck_endtime,strike_to_select,strategy_starttime,strategy_endtime)
        selectedstrikedf = selectedstrikedf.drop_duplicates(subset=['Date','Time','Ticker'])
        if sl_method == 'hard':
            print("SL Method: ",sl_method)
            start_time = time.time()
            overnight_gap = overnight_gap_check(spotdata.copy(),gapcheck_from_close_to_open)
            selectedstrikedf = pd.merge(selectedstrikedf,overnight_gap[['Date','Flag']],on=['Date'],how='inner')
            signalsdf = long_options_signal_generator(selectedstrikedf.copy())
            signalsdf = stoploss_target(signalsdf.copy(),selectedstrikedf.copy(),stoploss,target,stoploss_flag,target_flag).drop_duplicates(subset=['Date','Time','Ticker'])
            print("Hard Stoploss Signal Generator Completed in ", np.round(time.time() - start_time, 1), "s")
            signalsdf = signalsdf.dropna(subset=['EQ_Close'])

        if sl_method == 'trailing':
            print("SL Method: ",sl_method)
            start_time = time.time()
            signalsdf = trailing_sl_signal_generator(selectedstrikedf.copy(),spotdata.copy(),stoploss,trailingstoploss,target,stoploss_flag,target_flag,gapcheck_from_close_to_open)
            print("Trailing Stoploss Signal Generator Completed in ", np.round(time.time() - start_time, 1), "s")
            signalsdf = signalsdf.dropna(subset=['EQ_Close'])
            
        tradebook_main = tradebookgenerator(signalsdf.copy())

        contract_type = 'Weekly'
        time1 = {'09:15:59':'915'}[gapcheck_starttime]
        time2 = {'09:30:59':'930','09:45:59':'945'}[gapcheck_endtime]

        if not os.path.exists(fr"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\GapCheck\Weekly\{time1}-{time2}\\Updated_{updated_asof}\\"):
            os.mkdir(fr"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\GapCheck\Weekly\{time1}-{time2}\\Updated_{updated_asof}\\")
        
        tradefilepath = os.path.join(fr"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\GapCheck\Weekly\{time1}-{time2}\\Updated_{updated_asof}",f'{index}_{contract_type}_{time1}-{time2}_GapCheck{int(gapcheck_from_close_to_open*100)}%_{sl_method.capitalize()}SL.csv')
        tradebook_main.to_csv(tradefilepath,index=False)
        gapcheck_from_close_to_open += round(0.05,2)
    
def generate_equitycurve():
    lastdate = get_last_updated_date()
    spotdata = fetch_latest_spotdata(lastdate)
    dailyeqdf = spotdata[['Date']].drop_duplicates()
    dailyeqdf = dailyeqdf.sort_values(by=['Date']).reset_index(drop=True)
    # sl_methods = ['Hard','Trailing']
    sl_methods = ['Trailing']
    gapcheck_starttime = '09:15:59'
    gapcheck_endtime = '09:30:59'
    index = 'Nifty'
    time1 = {'09:15:59':'915'}[gapcheck_starttime]
    time2 = {'09:30:59':'930','09:45:59':'945'}[gapcheck_endtime]
    finaldf = pd.DataFrame()
    for sl_method in sl_methods:
        gap = 25
        while gap <= 40:
            column_name = f'NAV_{gap}%' if sl_method == 'Hard' else f'NAV_{gap}%_TSL'
            print(column_name)
            last_nav_file_path = rf"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\NAVs\GapCheck\{time1}-{time2}\Updated_{lastupdated_asof}\{index}_{sl_method}SL_NAV.xlsx"
            lastnavfile = pd.read_excel(last_nav_file_path)
            filepath = fr"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\GapCheck\Weekly\{time1}-{time2}\Updated_{updated_asof}\{index}_Weekly_{time1}-{time2}_GapCheck{gap}%_{sl_method}SL.csv"
            equitycurve = pd.read_csv(filepath)
            # equitycurve['DaySum'] = equitycurve.groupby(['Date'])['PnL_after_commission'].transform('sum')
            # equitycurve['DayPnL%'] = round((equitycurve['DaySum'] / equitycurve['EQ_Close']) * 100,2)
            equitycurve['DayPnL%'] = equitycurve.groupby('Date')['PnL%'].transform('sum')
            equitycurve_unique = equitycurve.drop_duplicates(subset=['Date']).reset_index(drop=True)
            equitycurve_unique.loc[0,column_name] = round(lastnavfile[column_name].iloc[-1] * (1+equitycurve_unique.loc[0,'DayPnL%']/100),2)
            print(round(lastnavfile[f'NAV_{gap}%'].iloc[-1],2)) if sl_method == 'Hard' else print(round(lastnavfile[f'NAV_{gap}%_TSL'].iloc[-1],2))
            for i in range(1,len(equitycurve_unique)):
                equitycurve_unique.loc[i,column_name] = round(equitycurve_unique.loc[i-1,column_name] * (1+equitycurve_unique.loc[i,'DayPnL%']/100),2)
            equitycurve_unique['Date'] = pd.to_datetime(equitycurve_unique['Date']).dt.date
            if gap == 25:
                finaldf = pd.merge(dailyeqdf,equitycurve_unique[['Date',column_name]],on=['Date'],how='left')
                finaldf[column_name] = finaldf[column_name].fillna(method='ffill')
            else:
                finaldf = pd.merge(finaldf,equitycurve_unique[['Date',column_name]],on='Date',how='left')
                finaldf[column_name] = finaldf[column_name].fillna(method='ffill')
            gap += 5

        # display(finaldf)
        if not os.path.exists(rf"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\NAVs\GapCheck\{time1}-{time2}\\Updated_{updated_asof}\\"):
            os.mkdir(rf"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\NAVs\GapCheck\{time1}-{time2}\\Updated_{updated_asof}\\")
        
        finaldf.to_excel(os.path.join(rf"C:\Vishwanath\PythonCodes\Strategy\LongOptions\{index}\NAVs\GapCheck\{time1}-{time2}\\Updated_{updated_asof}",f"{index}_{sl_method}SL_NAV.xlsx"))

generate_equitycurve()