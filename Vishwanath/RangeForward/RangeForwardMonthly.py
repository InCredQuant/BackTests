import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
import re
from sqlalchemy import create_engine
import sys
sys.path.insert(0, 'G:\\Shared drives\\BackTests\\pycode\\DBUpdation\\')
import pg_redirect
from date_config import *

index = 'NIFTY'
startdate = '2019-01-01'
# enddate = '2025-04-25'
strike_selected = {0.25:'OTM5',0.3:'OTM4',0.35:'OTM3',0.4:'OTM2',0.45:'OTM1',0.5:'ATM',0.55:'ITM1',0.6:'ITM2',0.65:'ITM3'}
strike_to_select = 0.35
# strikes_to_select = [0.55,0.6,0.65]
# strikes_to_select = [0.35,0.4,0.45]

def get_connection():
    conn_params = {
        'database': 'data',
        'user': 'postgres',
        'password': 'postgres',
        'host': '192.168.44.4',
        'port': 5432
    }
    try:
        conn = pg.connect(**conn_params)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def get_weeklyexpiry_dates(index,startdate):
    expiryquery = f'''
                    SELECT * FROM nseexpiry WHERE "MONTHLY" = 1 and "SYMBOL" = '{index.upper()}'
                    and "DATE" >= '{startdate}';
                    '''
    conn = get_connection()
    expdf = pd.read_sql(expiryquery,conn)
    conn.close()
    return expdf

def fetch_data(index,startdate,enddate):
    opt_query = f'''
                SELECT * FROM greeks_nsefno
                where "Date" >= '{startdate}' and "Date" <= '{enddate}'
                and "Ticker" like '{index}%';
                '''
    conn = get_connection()
    optdf = pd.read_sql(opt_query,conn)
    conn.close()
    return optdf

def first_digit_index(s):
    match = re.search(r'\d', s)
    return match.start() if match else None

def fetch_options(startdate,enddate):
    opt_query = f'''
                SELECT * FROM nsefno
                where "TIMESTAMP" >= '{startdate}' and "TIMESTAMP" <= '{enddate}'
                and "Ticker" like '{index}%';
                '''
    conn = get_connection()
    optdf = pd.read_sql(opt_query,conn)
    conn.close()
    return optdf

def fetch_spot_data(startdate,enddate):
    spotquery = f'''
                SELECT * FROM spotdata
                where "Date" >= '{startdate}' and "Date" <= '{enddate}'
                AND "Symbol" = '{index.upper()}';
                '''
    conn = get_connection()
    spotdf = pd.read_sql(spotquery,conn)
    conn.close()
    return spotdf

def convert_eq_to_daily_data(equitydf):
    equitydf = equitydf.sort_values(by=['Date','Time']).reset_index(drop=True)
    equitydf['Datetime'] = pd.to_datetime(equitydf['Date'].astype(str) + ' ' + equitydf['Time'].astype(str))
    equitydf = equitydf.set_index("Datetime")
    ddf = equitydf.groupby(['Date', pd.Grouper(freq='B')]).agg({"Open" : "first", "High" : "max",
                            "Low" : "min","Close" : "last", 'Volume' : 'sum'})
                
    ddf.columns = ["Open", "High", "Low", "Close", 'Volume']
    eqdf = ddf.reset_index().drop(columns=['Datetime'])
    eqdf['Date'] = pd.to_datetime(eqdf['Date'])
    return eqdf

def merge_spot(df,eqdf):
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    optdf = pd.merge(df,eqdf[['Date','Close']],on=['Date'],how='left',suffixes=['','_EQ'])
    return optdf

def ticker_change(df,flag='N'):
    if index == 'BANKNIFTY':
        df['Ticker'] = df['Ticker'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
    elif index == 'NIFTY':
        df['Ticker'] = df['Ticker'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('30MAR23','29MAR23').str.replace('29JUN23','28JUN23')
    return df

def expiry_changes(expirydf):
    if index == 'BANKNIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('30MAR23','29MAR23').str.replace('29JUN23', '28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-03-30').date()) | (expirydf['DATE'] == pd.to_datetime('2023-06-29').date()) | (expirydf['DATE'] == pd.to_datetime('2024-03-28').date()) | (expirydf['DATE'] == pd.to_datetime('2024-04-25').date()) | (expirydf['DATE'] == pd.to_datetime('2024-06-27').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    elif index == 'NIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('29JUN23', '28JUN23').str.replace('29MAR23','30MAR23')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-03-30').date()) | (expirydf['DATE'] == pd.to_datetime('2023-06-29').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    return expirydf.drop_duplicates()

def tradebook_generator(tradebook):
    commission = 0.5
    brokerage = 6
    tradebook['GrossPnL'] = np.where(tradebook['TradeType'] == 'Short',tradebook['EntryPrice'] - tradebook['ExitPrice'],tradebook['ExitPrice'] - tradebook['EntryPrice'])
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['Close_EQ']) * 100,2)
    # tradebook['Final_PnL%'] = tradebook['PnL%'] - (((brokerage*2) / (tradebook['Close_EQ'] * 15))*100)
    tradebook = tradebook.sort_values(by=['EntryDate'])
    tradebook = tradebook.drop_duplicates()
    return tradebook

def current_long(expdf,df,eqdf,strike_to_select):
    expdf = expdf.sort_values(by=['DATE']).reset_index(drop=True)
    expdf['SHIFTEDDATE'] = expdf['DATE'].shift(-1)
    df['ExpiryDate'] = pd.to_datetime(df['Ticker'].str[len(index):len(index)+7])
    mergeddf = pd.merge(df,expdf[['DATE','SHIFTEDDATE']],left_on=['Date'],right_on=['DATE'],how='left')
    filtereddf = mergeddf[(mergeddf['Date']==mergeddf['DATE']) & (mergeddf['ExpiryDate']==mergeddf['SHIFTEDDATE'])]
    filtered_spot = merge_spot(filtereddf,eqdf).sort_values(by=['Date','Ticker']).reset_index(drop=True)
    filtered_spot['OptionType'] = filtered_spot['Ticker'].str[len(index)+7:len(index)+9]
    filtered_spot['Strike'] = filtered_spot['Ticker'].str[len(index)+9:]
    filtered_spot = filtered_spot.dropna(subset=['Close'])
    filtered_spot['Difference'] = filtered_spot['Strike'].astype(float) - filtered_spot['Close']
    if strike_to_select < 0.5:
        filtered_spot['Filter'] = np.where(
        (filtered_spot['OptionType'] == 'CE') & (filtered_spot['Difference'] > 0), 1,
        np.where(
            (filtered_spot['OptionType'] == 'PE') & (filtered_spot['Difference'] < 0), 1, 0
        ))
    else:
        filtered_spot['Filter'] = np.where(
        (filtered_spot['OptionType'] == 'CE') & (filtered_spot['Difference'] < 0), 1,
        np.where(
            (filtered_spot['OptionType'] == 'PE') & (filtered_spot['Difference'] > 0), 1, 0
        ))
    filtered_spot = filtered_spot[filtered_spot['Filter']==1]
    filtered_spot['DeltaDiff'] = np.where(filtered_spot['OptionType']=='CE',filtered_spot['Delta'] - strike_to_select,((filtered_spot['Delta']*-1)-strike_to_select))
    filtered_spot_ = filtered_spot[filtered_spot['DeltaDiff'] > 0]
    longdf = filtered_spot_.loc[filtered_spot_.groupby(['Date', 'OptionType'])['DeltaDiff'].idxmin()]
    return longdf

# def next_short(expdf,longdf,df):
#     expdf = expdf.sort_values(by=['DATE']).reset_index(drop=True)
#     expdf['Shifted'] = expdf['DATE'].shift(-2)
#     longdf_ = longdf[['Ticker','Date','Strike','OptionType']]
#     df['OptionType'] = df['Ticker'].str[len(index)+7:len(index)+9]
#     df['Strike'] = df['Ticker'].str[len(index)+9:]
#     df['ExpiryDate'] = pd.to_datetime(df['Ticker'].str[len(index):len(index)+7])
#     longdf_ = pd.merge(longdf_,expdf[['DATE','Shifted']],left_on='Date',right_on='DATE',how='left')
#     longdf_['Shifted'] = pd.to_datetime(longdf_['Shifted'])
#     shortdf = pd.merge(df,longdf_[['Date','Strike','OptionType','Shifted']],left_on=['Date','Strike','OptionType','ExpiryDate'],right_on=['Date','Strike','OptionType','Shifted'],how='inner').sort_values(by='Date')
#     return shortdf

def nav_calculation(ddf_,dailyspot):
    def overnight_nav_calculator(df,tradebook,eqdf):
        df['Ticker'] = np.where(df['Ticker'].str.contains('29JUN23'),df['Ticker'].str.replace('29JUN23','28JUN23'),df['Ticker'])
        tradebook['EntryDate'] = pd.to_datetime(tradebook['EntryDate'],dayfirst=True).dt.date
        mergeddf = pd.merge(df,tradebook[['Ticker','EntryDate','ExitDate','TradeType']],on='Ticker',suffixes=['','_merged'])
        mergeddf['TIMESTAMP'] = pd.to_datetime(mergeddf['TIMESTAMP'],dayfirst=True)
        mergeddf['EntryDate'] = pd.to_datetime(mergeddf['EntryDate'],dayfirst=True)
        mergeddf = mergeddf[(mergeddf['TIMESTAMP']>=mergeddf['EntryDate']) & (mergeddf['TIMESTAMP']<=mergeddf['ExitDate'])].sort_values(by=['TIMESTAMP']).reset_index(drop=True)
        mergeddf['PrevClose'] = np.where(mergeddf['TIMESTAMP'] != mergeddf['EntryDate'],mergeddf.groupby(['Ticker','EntryDate'])['CLOSE'].shift(1),np.nan)
        mergeddf['DailyPnL'] = np.where(pd.notna((mergeddf['PrevClose']) == True) & (mergeddf['TradeType'] == 'Short'),mergeddf['PrevClose'] - mergeddf['CLOSE'],
                                            np.where(pd.notna((mergeddf['PrevClose']) == True) & (mergeddf['TradeType'] == 'Long'),mergeddf['CLOSE'] - mergeddf['PrevClose'],0))
        mergeddf['DaySum'] = mergeddf.groupby(['EntryDate','TIMESTAMP'])['DailyPnL'].transform('sum')
        idx = mergeddf.groupby('TIMESTAMP')['DaySum'].idxmax()
        result_df = mergeddf.loc[idx]
        mergeddf_unique = result_df.drop_duplicates(subset=['TIMESTAMP']).reset_index(drop=True)
        mergeddf_unique = mergeddf_unique[['TIMESTAMP','DaySum']]
        mergeddf_unique = pd.merge(mergeddf_unique,eqdf[['Date','Close']],left_on='TIMESTAMP',right_on='Date',how='left').rename(columns={'Close':'Close_EQ'})
        mergeddf_unique['DayPnL%'] = round((mergeddf_unique['DaySum'] / mergeddf_unique['Close_EQ']),5)
        mergeddf_unique.loc[0,'NAV'] = 100
        for i in range(1,len(mergeddf_unique)):
            mergeddf_unique.loc[i,'NAV'] = mergeddf_unique.loc[i-1,'NAV'] * (1+mergeddf_unique.loc[i,'DayPnL%'])
        return mergeddf_unique[['Date','NAV']]
    
    navpath = fr"C:\Vishwanath\PythonCodes\Strategy\RangeForward\NAV\Weekly\{index.capitalize()}\{datetime.today().date()}"
    os.makedirs(navpath, exist_ok=True)

    tradebook_path = fr"C:\Vishwanath\PythonCodes\Strategy\RangeForward\Tradebook\Weekly\\{index.capitalize()}\\{datetime.today().date()}"
    filenames = os.listdir(tradebook_path)

    for file in filenames:
        print(fr"Running for file {file}")
        tradebook_final = pd.read_csv(os.path.join(tradebook_path,file))
        nav = overnight_nav_calculator(ddf_.copy(),tradebook_final.copy(),dailyspot.copy())
        nav = pd.merge(nav,dailyspot[['Date']],on=['Date'],how='right').ffill().dropna(subset=['NAV']).reset_index(drop=True)
        nav.to_excel(os.path.join(navpath,f"{index.capitalize()}_NAV_{startdate[:4]}_{file.split('_')[-1][:4]}.xlsx"))

def backtest():
    expdf = get_weeklyexpiry_dates(index,startdate)
    optdf = fetch_data(index,startdate,enddate)
    spotdata = fetch_spot_data(startdate,enddate)
    dailyspot = convert_eq_to_daily_data(spotdata)
    ddf = fetch_options(startdate,enddate)
    expdf = expiry_changes(expdf)
    optdf_ = ticker_change(optdf.copy())
    ddf_ = ticker_change(ddf.copy(),'Y')
    ddf_ = ddf_[ddf_['SYMBOL']==index.upper()]
    optdf_['first_digit_index'] = optdf_['Ticker'].apply(first_digit_index)
    optdf_ = optdf_[optdf_['first_digit_index'] == len(index)].reset_index(drop=True)
    optdf_['Date'] = pd.to_datetime(optdf_['Date']).dt.date
    optdf_ = optdf_[optdf_['Date'].isin(expdf['DATE'])]
    for strike in strikes_to_select:
        print(f"Running for {strike_selected.get(strike)}")
        longdf = current_long(expdf.copy(),optdf_.copy(),dailyspot.copy(),strike)
        print(longdf)
#         shortdf = next_short(expdf.copy(),longdf.copy(),optdf_.copy())
#         entrylong = pd.merge(ddf_[['Ticker','TIMESTAMP','CLOSE']],longdf[['Ticker','Date','ExpiryDate','OptionType']],left_on=['Ticker','TIMESTAMP'],right_on=['Ticker','Date'],how='inner').sort_values(by='Date').reset_index(drop=True)
#         entryshort = pd.merge(ddf_[['Ticker','TIMESTAMP','CLOSE']],shortdf[['Ticker','Date','ExpiryDate','OptionType']],left_on=['Ticker','TIMESTAMP'],right_on=['Ticker','Date'],how='inner').sort_values(by='Date').reset_index(drop=True)
#         entrylong['TradeType'] = np.where(entrylong['OptionType']=='PE','Long','Short')
#         entryshort['TradeType'] = np.where(entryshort['OptionType']=='PE','Short','Long')
#         entrydf = pd.concat([entrylong,entryshort],ignore_index=True).sort_values(by='Date').reset_index(drop=True)
#         entrydf['ExitDate'] = entrydf.groupby('TIMESTAMP')['ExpiryDate'].transform('min')
#         entrydf = entrydf.drop_duplicates()
#         ddf_new = ddf_.copy()
#         ddf_new['TIMESTAMP'] = pd.to_datetime(ddf_new['TIMESTAMP'])
#         exitdf = pd.merge(ddf_new[['Ticker','TIMESTAMP','CLOSE']],entrydf[['Ticker','ExitDate']],left_on=['Ticker','TIMESTAMP'],right_on=['Ticker','ExitDate'],how='inner')
#         tradebook = pd.merge(entrydf,exitdf[['Ticker','CLOSE','ExitDate']],on=['Ticker','ExitDate'],how='inner')
#         tradebook['Date'] = pd.to_datetime(tradebook['Date'])
#         dailyspot['Date'] = pd.to_datetime(dailyspot['Date'])
#         tradebook_final = pd.merge(tradebook,dailyspot[['Date','Close']],on='Date',how='left',suffixes=['','_EQ']).rename(columns={'CLOSE_x':'EntryPrice','CLOSE_y':'ExitPrice','Close':'Close_EQ','TIMESTAMP':'EntryDate'})
#         tradesheet = tradebook_generator(tradebook_final.copy())
#         print(tradesheet['GrossPnL'].sum(),tradesheet['PnL_after_commission'].sum())
#         path = fr"C:\Vishwanath\PythonCodes\Strategy\RangeForward\Tradebook\Weekly\\{index.capitalize()}\\{datetime.today().date()}"
#         os.makedirs(path, exist_ok=True)
#         tradesheet.to_csv(os.path.join(path,f"{index.capitalize()}_Tradebook_{startdate[:4]}_{strike_selected.get(strike)}.csv"),index=False)
#     return optdf,ddf_,dailyspot

# def delta_calculator(ddf):
#     def initialization(df,nav,index_name,type):
#         df = df.dropna(subset=['PnL_after_commission'])
#         df['EntryDate'] = pd.to_datetime(df['EntryDate'], format='mixed',dayfirst=True)
#         df['ExpiryDate'] = pd.to_datetime(df['ExpiryDate'], format='mixed',dayfirst=True)
#         df['ExitDate'] = pd.to_datetime(df['ExitDate'], format='mixed',dayfirst=True)
#         lastdate = df['ExitDate'].dt.date.max()
#         nav['Date'] = pd.to_datetime(nav['Date'])
#         unique_dates = nav['Date'].dt.date.unique()
#         long_positions = {date: [] for date in unique_dates}
#         short_positions = {date: [] for date in unique_dates}
#         for index_, row in df.iterrows():
#             ticker = row['Ticker']
#             trade_type = row['TradeType']
#             entry_date = row['EntryDate']
#             expiry_date = row['ExpiryDate']
#             exit_date = row['ExitDate']
#             effective_end_date = min(exit_date, expiry_date)
#             for date in unique_dates:
#                 if entry_date.date() <= date < effective_end_date.date():
#                     if (expiry_date.date() != exit_date.date()) & (date < exit_date.date()):
#                         long_positions[date].append(ticker) if trade_type == 'Long' else short_positions[date].append(ticker)
#                     elif expiry_date.date() == exit_date.date():
#                         long_positions[date].append(ticker) if trade_type == 'Long' else short_positions[date].append(ticker)
#                 elif (date == lastdate) & (date == exit_date):
#                     long_positions[date].append(ticker) if trade_type == 'Long' else short_positions[date].append(ticker)
#         word = type
#         dates = []
#         long_tickers = []
#         short_tickers = []
#         for date in unique_dates:
#             dates.append(date)
#             long_tickers.append(long_positions[date])
#             short_tickers.append(short_positions[date])
#         summary_df = pd.DataFrame({
#             'Date': dates,
#             f'{word}Tickers': long_tickers if type == 'Long' else short_tickers
#         })
#         summary_df = summary_df[summary_df[f'{word}Tickers'].apply(len) > 0]
#         splittingdf = pd.DataFrame(summary_df)
#         splittingdf[[f'{word}Ticker1',f'{word}Ticker2']] = pd.DataFrame(splittingdf[f'{word}Tickers'].tolist(), index=splittingdf.index)
#         splittingdf = splittingdf.drop(columns=[f'{word}Tickers'])
#         ticker_columns = [f'{word}Ticker1', f'{word}Ticker2']
#         mask = splittingdf['Date'] == pd.to_datetime('2024-02-29').date()
#         for col in ticker_columns:
#             splittingdf.loc[mask, col] = splittingdf.loc[mask, col].str.replace('27MAR24', '28MAR24')

#         return splittingdf,index_name,type

#     def calculate_delta(df,nav,greeks_df,index_name,type):
#         engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'
#         db_conn = create_engine(engine_url, connect_args={"connect_timeout": 7})
#         db_conn.connect()
#         ddf,index,type = initialization(df.copy(),nav.copy(),index_name,type)
#         ddf = ddf.rename(columns={'TIMESTAMP':'Date'})
#         ddf = ddf.loc[:,~ddf.columns.str.contains('^Unnamed')]
#         # def replace_ticker_strikes(row, index):
#         #     strike = str(row[f'{index}Strike'])
#         #     row[f'{index}Ticker'] = row[f'{index}Ticker'].replace(strike, '')
#         #     return row
#         ddf = ddf.rename(columns={'TIMESTAMP': 'Date'})
#         ddf['Date'] = pd.to_datetime(ddf['Date']).dt.date
#         ddf = ddf.loc[:, ~ddf.columns.str.contains('^Unnamed')]
#         ddf.columns = ddf.columns.str.replace(' ', '', regex=False)
#         tickers = [col for col in ddf.columns if 'Ticker' in col]
#         new_tickers = [ticker.replace('Ticker', '') + 'Ticker' for ticker in tickers]
#         rename_mapping = {old: new for old, new in zip(tickers, new_tickers)}
#         ddf = ddf.rename(columns=rename_mapping)
#         for ticker in new_tickers:
#             ddf[ticker] = ddf[ticker].str.replace('.NFO', '')
#         ddf['Date'] = pd.to_datetime(ddf['Date'])
#         greeks_df['Date'] = pd.to_datetime(greeks_df['Date'])
#         merged_long1 = pd.merge(ddf, greeks_df[['Ticker','Date','Delta']], how='left', left_on=['Date', f'{type}1Ticker'], right_on=['Date', 'Ticker'])
#         merged_long1 = merged_long1.rename(columns={'Delta': f'Delta_{type}1'}).drop(columns='Ticker')
#         final_merged = pd.merge(merged_long1, greeks_df[['Ticker','Date','Delta']], how='left', left_on=['Date', f'{type}2Ticker'], right_on=['Date', 'Ticker'])
#         final_merged = final_merged.rename(columns={'Delta': f'Delta_{type}2'}).drop(columns='Ticker')
#         return final_merged
        
#     index_name = 'Nifty' #Nifty/BankNifty
#     types = ['Short','Long']
#     navpath = fr"C:\Vishwanath\PythonCodes\Strategy\RangeForward\NAV\Weekly\{index.capitalize()}\{datetime.today().date()}"
#     tradebook_path = fr"C:\Vishwanath\PythonCodes\Strategy\RangeForward\Tradebook\Weekly\{index.capitalize()}\{datetime.today().date()}"
#     deltapath = fr"C:\Vishwanath\PythonCodes\Strategy\RangeForward\Delta\Weekly\{index.capitalize()}\{datetime.today().date()}"
#     os.makedirs(deltapath, exist_ok=True)
#     greeksdf = ticker_change(ddf.copy())
#     tradebook_final = pd.read_csv(os.path.join(tradebook_path,f"{index_name.capitalize()}_Tradebook_2019_OTM3.csv"))
#     nav = pd.read_excel(os.path.join(navpath,f"{index_name.capitalize()}_NAV_2019_OTM3.xlsx"))
#     for type in types:
#         ddf_delta = calculate_delta(tradebook_final.copy(),nav.copy(),greeksdf.copy(),index_name,type)
#         ddf_delta.to_excel(os.path.join(deltapath,f"{index_name.capitalize()}_{type}Delta_2019_OTM3.xlsx"))

if __name__ == '__main__':
    print(f"Running Backtest for {index.capitalize()} Weekly Range Forward..")
    optdf,ddf_,dailyspot = backtest()
    # print("BACKTEST UPDATED!!")
    # print("GENERATING NAVs for WEEKLY RANGE FORWARD STRATEGY")
    # nav_calculation(ddf_.copy(),dailyspot.copy())
    # print("GENERATING DELTA for WEEKLY RANGE FORWARD STRATEGY")
    # delta_calculator(optdf.copy())
    # print("Completed.")

    

