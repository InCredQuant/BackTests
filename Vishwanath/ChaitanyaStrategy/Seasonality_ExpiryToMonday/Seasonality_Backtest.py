import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
import os

timeframe = 'MONTHLY'
index = 'BANKNIFTY'
instrument = 'OPTIDX'
exit_after_how_many_days = 5
startdate = 24
enddate = 4
method = 'Original' ## Original for the current strategy that we are following vs New for Expiry to t+5 days
create_tradesheet = 'N'

date_mapping = {
    ('MONTHLY', 'BANKNIFTY'): '2016-01-01',
    ('MONTHLY', 'NIFTY'): '2016-01-01'
}

startdate = date_mapping.get((timeframe, index))
enddate = '2025-02-27'
date_obj = datetime.strptime(enddate, '%Y-%m-%d')
updated_asof = date_obj.strftime('%d%m%Y')

def get_specific_trading_days(df,startdate = 24, exitdate = 4):
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')
    df.index = pd.to_datetime(df.index)
    def find_target_day(df, year, month):
        month_df = df.loc[(df.index.year == year) & (df.index.month == month)]
        if len(month_df) == 0:
            return None
        candidate_days = month_df[month_df.index.day <= startdate]
        # If no days found before or on 24th, use the last day of the month
        if len(candidate_days) == 0:
            return month_df.index[-1]
        # Return the last day less than or equal to 24th
        return candidate_days.index[-1]
    
    def find_next_month_day(df, year, month):
        # Determine next month and year
        if month == 12:
            next_year = year + 1
            next_month = 1
        else:
            next_year = year
            next_month = month + 1
        
        # Find days in the next month
        next_month_df = df.loc[(df.index.year == next_year) & (df.index.month == next_month)]
        
        # If no days found for next month, return None
        if len(next_month_df) == 0:
            return None
        
        # Create a target date for the 4th
        target_date = pd.Timestamp(next_year, next_month, exitdate)
        # Find days less than or equal to 4th
        days_up_to_4th = next_month_df[next_month_df.index <= target_date]
        
        # If no days found on or before 4th, return None
        if len(days_up_to_4th) == 0:
            return None
        
        # Return the closest day up to the 4th (the latest one)
        return days_up_to_4th.index[-1]
    
    # Collect dates
    selected_dates = [date for date in (find_target_day(df, year, month) for year in df.index.year.unique() for month in range(1, 13)) if date is not None] + [
        date for date in (find_next_month_day(df, year, month) for year in df.index.year.unique() for month in range(1, 13)) if date is not None]
    
    return df.loc[selected_dates]

def fetch_futures_data():
    query = f'''SELECT * FROM nsefno WHERE "TIMESTAMP" > '{startdate}' AND "TIMESTAMP" <= '{enddate}'
    AND "Ticker" LIKE '{index.upper()}%' AND "INSTRUMENT" = 'FUTIDX' and "SYMBOL" = '{index.upper()}'; '''
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    data = pd.read_sql(query,connection)
    data['EXPIRY_DT'] = pd.to_datetime(data['EXPIRY_DT'])
    mask = (data['EXPIRY_DT'] == data.groupby('TIMESTAMP')['EXPIRY_DT'].transform('min'))
    monthlydata = data[mask]
    return monthlydata

def ticker_change(df,flag):
    if index == 'BANKNIFTY':
        df['Ticker'] = df['Ticker'].str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
    elif index == 'NIFTY':
        df['Ticker'] = df['Ticker'].str.replace('29JUN23','28JUN23')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('29JUN23','28JUN23')
    return df

## Accounting for the changes in expiry - for example 29JUN23 expiry got changed to 30JUN23.
def expiry_changes(expirydf):
    if index == 'BANKNIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('29JUN23', '28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-06-29').date()) | (expirydf['DATE'] == pd.to_datetime('2024-03-28').date()) | (expirydf['DATE'] == pd.to_datetime('2024-04-25').date()) | (expirydf['DATE'] == pd.to_datetime('2024-06-27').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    elif index == 'NIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('29JUN23', '28JUN23')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-06-29').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    return expirydf

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

def update_data(df):
    optquery = f'''
    SELECT *
    FROM nsefno
    WHERE "TIMESTAMP" > '{df['TIMESTAMP'].max()}' AND "TIMESTAMP" <= '{enddate}'
    AND "Ticker" LIKE '{index}%'
    AND "INSTRUMENT" = '{instrument}';
    '''
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    data = pd.read_sql(optquery,connection)
    ddf = pd.concat([df,data],ignore_index=True)
    return ddf
    
def update_expiry_data(expirydf):
    expiryquery = f''' SELECT * FROM nseexpiry WHERE "INSTRUMENT" = '{instrument}'; '''
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    expirydata = pd.read_sql(expiryquery,connection)
    expirydata = expirydata[expirydata['SYMBOL']==index].sort_values(by=['DATE']).reset_index(drop=True)
    return expirydata

def update_spot(eqdf):
    spotquery = f''' SELECT * FROM spotdata WHERE "Symbol" = '{index}' AND "Date" > '{eqdf['Date'].max()}' order by "Date", "Time"; '''
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    equitydf = pd.read_sql(spotquery,connection)
    spotdata = convert_eq_to_daily_data(equitydf.copy())
    eqdf_ = pd.concat([eqdf,spotdata],ignore_index=True)
    return eqdf_

def merge_spot(df,eqdf):
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    df = pd.merge(df,eqdf[['Date','Close']],left_on=['TIMESTAMP'],right_on=['Date'],how='left',suffixes=['','_EQ']).rename(columns={'Close':'Close_EQ'})
    return df

def strike_selector(df):
    df = df.dropna(subset='Close_EQ')
    df['Difference'] = df['STRIKE_PR'].astype(float) - df.groupby('ExpiryDate')['Close_EQ'].transform('first')
    df['Difference'] = df['Difference'].abs()
    # df = df.dropna(subset=['Difference'])
    atmdf = df.loc[df.groupby(['ExpiryDate', 'OPTION_TYP'])['Difference'].idxmin()].drop(columns=['Difference'])
    # atmdf['EXPIRY_DATE'] = pd.to_datetime(atmdf['EXPIRY_DATE']).dt.date
    return atmdf

def getexitdf(df,atmdf_entry,tradetype):
    if tradetype == 'Long':
        atmdf_exit = pd.merge(df,atmdf_entry[['Ticker','ExpiryDate']],left_on=['Ticker','Date'],right_on=['Ticker','ExpiryDate'],how='inner',suffixes=['','_tracker'])
        atmdf_entry['TradeType'] = 'Long'
        atmdf_exit['TradeType'] = 'Sell'
    else:
        atmdf_exit = pd.merge(df,atmdf_entry[['Ticker','EXPIRY_DATE']],left_on=['Ticker','Date'],right_on=['Ticker','EXPIRY_DATE'],how='inner',suffixes=['','_tracker'])
        atmdf_entry['TradeType'] = 'Short'
        atmdf_exit['TradeType'] = 'Cover'
    return atmdf_entry, atmdf_exit

def get_index(atmdf_entry,eqdf,optdf):
    atmdf_entry = atmdf_entry[['Ticker','EXPIRY_DT','STRIKE_PR','OPTION_TYP','CLOSE','TIMESTAMP','ExpiryDate','Date','Close_EQ']]
    eqdf_reset = eqdf.reset_index()
    atmdf_entry['Date'] = pd.to_datetime(atmdf_entry['Date'])
    eqdf_reset['Date'] = pd.to_datetime(eqdf_reset['Date'])
    optdf['TIMESTAMP'] = pd.to_datetime(optdf['TIMESTAMP'])
    atmdf_entry = pd.merge(atmdf_entry,eqdf_reset[['index','Date']],on='Date',how='left')
    atmdf_entry['exitindex'] = atmdf_entry['index'] + exit_after_how_many_days
    atmdf_entry = pd.merge(atmdf_entry,eqdf_reset[['index','Date']],left_on='exitindex',right_on='index',how='left',suffixes=['','_Exit'])
    atmdf_entry = atmdf_entry[['Ticker','EXPIRY_DT','STRIKE_PR','OPTION_TYP','CLOSE','Date','Date_Exit','Close_EQ','ExpiryDate']]
    atmdf_entry['Date_Exit'] = pd.to_datetime(atmdf_entry['Date_Exit'])
    atmdf_exit = pd.merge(atmdf_entry[['Ticker','EXPIRY_DT','STRIKE_PR','OPTION_TYP','Date_Exit','ExpiryDate']],optdf[['TIMESTAMP','CLOSE']],left_on=['Date_Exit'],right_on=['TIMESTAMP'])
    return atmdf_entry,atmdf_exit

def get_trades(atmdf_entry,atmdf_exit):
    atmdf_entry['TradeType'] = np.where(atmdf_entry['OPTION_TYP']=='CE','Long','Short')
    atmdf_entry['TradeType'] = np.where(atmdf_entry['OPTION_TYP']=='XX','Long',atmdf_entry['TradeType'])
    entrytradesdf = atmdf_entry.rename(columns={'CLOSE':'EntryPrice','Date':'EntryDate'})
    entrytradesdf = entrytradesdf[['Ticker','TradeType','EntryDate','EntryPrice','STRIKE_PR','OPTION_TYP','EXPIRY_DT']].sort_values(by=['EntryDate'])
    exittradesdf = atmdf_exit.rename(columns={'CLOSE':'ExitPrice','Date_Exit':'ExitDate'})
    exittradesdf = exittradesdf[['Ticker','ExitDate','ExitPrice','EXPIRY_DT']].sort_values(by=['ExitDate'])
    tradebook = pd.merge(entrytradesdf, exittradesdf, on = ["Ticker", "EXPIRY_DT"], how = "left")
    return tradebook

def merge_entry_exit(entrydf,exitdf):
    tradesheet = entrydf.join(exitdf, how='left', lsuffix='_entry', rsuffix='_exit')
    return tradesheet

def gettingspotdata(tradebook,eqdf):
    tradebook['EntryDate'] = pd.to_datetime(tradebook['EntryDate']).dt.date
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    tradebook = pd.merge(tradebook,eqdf[['Date','Close']],left_on='EntryDate',right_on='Date',how='left').rename(columns={'Close':'Close_EQ'}).drop(columns=['Date'])
    return tradebook

def tradebook_generator(tradebook,eqdf):
    tradebook['GrossPnL'] = np.where(tradebook['TradeType'] == 'Short',tradebook['EntryPrice'] - tradebook['ExitPrice'],tradebook['ExitPrice'] - tradebook['EntryPrice'])
    tradebook['PnL_after_commission'] = (tradebook['ExitPrice'] * 0.997) - (tradebook['EntryPrice'] * 1.003)
    tradebook_ = gettingspotdata(tradebook,eqdf.copy())
    tradebook_['PnL%'] = round((tradebook_['PnL_after_commission'] / tradebook_['Close_EQ']) * 100,2)
    # tradebook['Final_PnL%'] = tradebook['PnL%'] - (((brokerage*2) / (tradebook['Close_EQ'] * 15))*100)
    tradebook_ = tradebook_.sort_values(by=['EntryDate'])
    tradebook_ = tradebook_.drop_duplicates()
    return tradebook_

def run_seasonality_backtest():
    if method != 'Original':
        futdata = fetch_futures_data()
        expirydf = pd.read_pickle(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\RawFiles\{index.capitalize()}\ExpiryDates{index.capitalize()}Optidx.pkl")
        expirydf = update_expiry_data(expirydf.copy())
        eqdf_old = pd.read_csv(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\RawFiles\{index.capitalize()}\{index}Spot.csv",parse_dates=['Date'],dayfirst=True)
        updatedeqdf = update_spot(eqdf_old.copy())
        eqdf = updatedeqdf.copy()
        print(f"Running Seasonality backtest for Expiry to Expiry+5 days version..")
        expiry_df = expiry_changes(expirydf.copy())
        futdata['ExpiryDate'] = pd.to_datetime(futdata['EXPIRY_DT'],errors='coerce').dt.date
        dates_to_ignore = pd.to_datetime(['2024-03-28','2024-04-25','2024-06-27','2024-09-26','2024-12-26','2025-01-29','2025-02-26']).date
        testexpiry = expiry_df[(expiry_df['DATE']>=pd.to_datetime('2016-01-01').date()) & (expiry_df['MONTHLY']==1)]
        testexpiry = testexpiry[~testexpiry['DATE'].isin(dates_to_ignore)] if index == 'BANKNIFTY' else testexpiry
        testexpiry['StartDate'] = testexpiry['DATE'].shift(1)
        testexpiry['EndDate'] = testexpiry['DATE']
        testexpiry = testexpiry.rename(columns={'DATE':'Date'})
        testexpiry_mask = (testexpiry['Date'] == testexpiry['StartDate']) & (testexpiry['StartDate'] == testexpiry['EndDate'])
        testexpiry_ = testexpiry[~testexpiry_mask]
        df = pd.merge(futdata,testexpiry_[['Date','StartDate','EndDate','EXPIRY']],left_on='ExpiryDate',right_on='Date',how='left').dropna(subset=['Date']).sort_values(by=['ExpiryDate'])
        mergeddf = merge_spot(df.copy(),eqdf.copy())
        atmdf_entry = mergeddf[mergeddf['TIMESTAMP'] == mergeddf['ExpiryDate']]
        atmdf_entry,atmdf_exit = get_index(atmdf_entry.copy(),eqdf.copy(),futdata.copy())
        tradebook = get_trades(atmdf_entry,atmdf_exit)
        tradesheet = tradebook_generator(tradebook.copy(),eqdf.copy())
        print(tradesheet['PnL_after_commission'].sum())
        os.makedirs(fr"C:\Vishwanath\PythonCodes\Strategy\ChaitanyaStrategy\Seasonality_ExpiryToMonday\Tradesheet\\",exist_ok=True)
        if create_tradesheet == 'Y':
            tradesheet.to_csv(fr"C:\Vishwanath\PythonCodes\Strategy\ChaitanyaStrategy\Seasonality_ExpiryToMonday\Tradesheet\\{index.capitalize()}_Seasonality_{datetime.today().date()}.csv",index=False)
        return tradesheet,futdata,eqdf
    else:
        futdata = fetch_futures_data()
        # futdata.to_csv("futdata.csv",index=False)
        eqdf_old = pd.read_csv(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\RawFiles\{index.capitalize()}\{index}Spot.csv",parse_dates=['Date'],dayfirst=True)
        updatedeqdf = update_spot(eqdf_old.copy())
        eqdf = updatedeqdf.copy()
        print(f"Running Seasonality backtest for {method} version..")
        futdata = futdata.sort_values(by='TIMESTAMP').reset_index(drop=True)
        futdata['Date'] = futdata['TIMESTAMP']
        futdata['Close'] = futdata['CLOSE']
        futdata = futdata.sort_values(by='TIMESTAMP').reset_index(drop=True)
        selected_trading_days = get_specific_trading_days(futdata)
        selected_trading_days_ = selected_trading_days.sort_values(by='Date').reset_index()
        selected_trading_days_.to_csv(f"days_2.csv",index=False)
        entrydf = selected_trading_days_[selected_trading_days_.index % 2 == 0].sort_values(by='Date').reset_index(drop=True)
        exitdf = selected_trading_days_[selected_trading_days_.index % 2 == 1].sort_values(by='Date').reset_index(drop=True)
        trades = merge_entry_exit(entrydf,exitdf)
        trades = trades[['Date_entry','Close_entry','Date_exit','Close_exit']].dropna(subset='Close_exit')
        trades['PnL'] = trades['Close_exit'] - trades['Close_entry']
        trades['PnL_after_commission'] = (trades['Close_exit'] * 0.997) - (trades['Close_entry'] * 1.003)
        trades['TradeType'] = 'Long'
        print(trades['PnL_after_commission'].sum())
        os.makedirs(fr"C:\Vishwanath\PythonCodes\Strategy\ChaitanyaStrategy\Seasonality_ExpiryToMonday\Tradesheet\\",exist_ok=True)
        if create_tradesheet == 'Y':
            trades.to_csv(fr"C:\Vishwanath\PythonCodes\Strategy\ChaitanyaStrategy\Seasonality_ExpiryToMonday\Tradesheet\\{index.capitalize()}_Seasonality_{datetime.today().date()}_{method}Version.csv",index=False)
        return trades,futdata,eqdf

def overnight_nav_calculator(tradebook,df,eqdf,index):
    df['Date'] = df['TIMESTAMP']
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['Ticker'] = np.where(df['Ticker'].str.contains('29JUN23'),df['Ticker'].str.replace('29JUN23','28JUN23'),df['Ticker'])
    if index.upper() == 'BANKNIFTY':
        df['Ticker'] = np.where((df['Ticker'].str.contains('30JAN25')) & (df['Date'] >= pd.to_datetime('2025-01-02').date()),df['Ticker'].str.replace('30JAN25','29JAN25'),df['Ticker'])
    if method == 'Original':
        tradebook['Date_entry'] = pd.to_datetime(tradebook['Date_entry']).dt.date
        tradebook['Ticker'] = index
        mergeddf = pd.merge(df,tradebook[['Ticker','Date_entry','Date_exit','TradeType']],left_on='SYMBOL',right_on='Ticker',suffixes=['','_trades'])
        mergeddf = mergeddf[(mergeddf['Date'] >= pd.to_datetime(mergeddf['Date_entry']).dt.date) & (mergeddf['Date'] <= pd.to_datetime(mergeddf['Date_exit']).dt.date)]
        mergeddf = mergeddf.sort_values(by=['Date']).reset_index(drop=True)
        mergeddf['PrevClose'] = np.where(mergeddf['Date'] != mergeddf['Date_entry'],mergeddf.groupby('Date_entry')['CLOSE'].shift(1),np.nan)
    else:
        tradebook['Ticker'] = index.upper()
        mergeddf = pd.merge(df,tradebook[['Ticker','EntryDate','ExitDate','TradeType']],left_on='SYMBOL',right_on='Ticker',suffixes=['','_trades'])
        mergeddf = mergeddf[(mergeddf['Date'] >= pd.to_datetime(mergeddf['EntryDate']).dt.date) & (mergeddf['Date'] <= pd.to_datetime(mergeddf['ExitDate']).dt.date)]
        mergeddf = mergeddf.sort_values(by=['Date']).reset_index(drop=True)
        mergeddf['PrevClose'] = np.where(mergeddf['Date'] != mergeddf['EntryDate'],mergeddf.groupby('EntryDate')['CLOSE'].shift(1),np.nan)
    mergeddf['DailyPnL'] = np.where(pd.notna((mergeddf['PrevClose']) == True) & (mergeddf['TradeType'] == 'Short'),mergeddf['PrevClose'] - mergeddf['CLOSE'],
                                        np.where(pd.notna((mergeddf['PrevClose']) == True) & (mergeddf['TradeType'] == 'Long'),mergeddf['CLOSE'] - mergeddf['PrevClose'],0))
    mergeddf['DaySum'] = mergeddf.groupby(['Date'])['DailyPnL'].transform('sum')
    # mergeddf.to_csv("mergeddf.csv",index=False)
    mergeddf_unique = mergeddf.drop_duplicates(subset=['Date']).reset_index(drop=True)
    mergeddf_unique = mergeddf_unique[['Date','DaySum']]
    mergeddf_unique['Date'] = pd.to_datetime(mergeddf_unique['Date']).dt.date
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    mergeddf_unique_ = pd.merge(mergeddf_unique,eqdf[['Date','Close']],on='Date',how='left').rename(columns={'Close':'Close_EQ'})
    mergeddf_unique_['DayPnL%'] = round((mergeddf_unique_['DaySum'] / mergeddf_unique_['Close_EQ']) * 100,3)
    mergeddf_unique_.loc[0,'NAV'] = 100
    for i in range(1,len(mergeddf_unique_)):
        mergeddf_unique_.loc[i,'NAV'] = mergeddf_unique_.loc[i-1,'NAV'] * (1+mergeddf_unique_.loc[i,'DayPnL%']/100)
    return mergeddf_unique_[['Date','NAV']]

def calculate_nav(df,tradebook,eqdf):
    
    nav = overnight_nav_calculator(df.copy(),tradebook.copy(),eqdf.copy(),index)
    nav['Date'] = pd.to_datetime(nav['Date'])
    eqdf['Date'] = pd.to_datetime(eqdf['Date'])
    nav = pd.merge(nav,eqdf[['Date']],on=['Date'],how='right').ffill().dropna(subset='NAV').reset_index(drop=True)

    base_dir = rf"C:\Vishwanath\PythonCodes\Strategy\ChaitanyaStrategy\Seasonality_ExpiryToMonday\NAVs\\"
    os.makedirs(base_dir, exist_ok=True)

    output_path = os.path.join(base_dir,f"{index.capitalize()}_Seasonality_{datetime.today().date()}_{method}Version.xlsx")
    if create_tradesheet == 'Y':
        nav.to_excel(output_path)

trades,df,eqdf = run_seasonality_backtest()
calculate_nav(trades,df,eqdf)
