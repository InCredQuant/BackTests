import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
warnings.filterwarnings("ignore")
from sqlalchemy import create_engine
import sys
sys.path.insert(0,r'G:\Shared drives\BackTests\pycode\MainLibs')
from intraday_db_postgres import *
sys.path.insert(0,r'G:\Shared drives\BackTests\pycode\DBUpdation')
import pg_redirect

timeframe = 'MONTHLY'
index = 'NIFTY'
instrument = 'OPTIDX'
tradetype = 'Short' # Short/Long

date_mapping = {
    ('MONTHLY', 'BANKNIFTY'): '2016-01-01',
    ('WEEKLY', 'BANKNIFTY'): '2016-05-27',
    ('WEEKLY', 'NIFTY'): '2019-02-11',
    ('MONTHLY', 'NIFTY'): '2016-01-01',
    ('MONTHLY', 'FINNIFTY'): '2016-01-01',
}

startdate = date_mapping.get((timeframe, index))
enddate = '2025-06-23'
date_obj = datetime.strptime(enddate, '%Y-%m-%d')
updated_asof = date_obj.strftime('%d%m%Y')

optdf = pd.read_pickle(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\Rawfiles\{index.capitalize()}\{index.capitalize()}_OPT.pkl")
expirydf = pd.read_pickle(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\RawFiles\{index.capitalize()}\ExpiryDates{index.capitalize()}Optidx.pkl")
eqdf = pd.read_csv(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\RawFiles\{index.capitalize()}\{index}Spot.csv",parse_dates=['Date'],dayfirst=True)

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

## To get only monthly contracts from daily Bhavcopy
def extract_monthly_contracts_from_bhavcopy(optdf,expirydf):
    filteredexpirydf = expirydf[(expirydf[f'{timeframe}']==1) & (expirydf['INSTRUMENT']==f'{instrument}') & (expirydf['SYMBOL'] == f'{index}')].reset_index(drop=True)
    filteredexpirydf['MonthTag'] = pd.to_datetime(filteredexpirydf['DATE']).dt.month
    filteredexpirydf['YearTag'] = pd.to_datetime(filteredexpirydf['DATE']).dt.year
    optdf['MonthTag'] = pd.to_datetime(optdf['EXPIRY_DT']).dt.month
    optdf['YearTag'] = pd.to_datetime(optdf['EXPIRY_DT']).dt.year
    mergeddf = pd.merge(optdf,filteredexpirydf[['EXPIRY','MonthTag','YearTag']],left_on=['EXPIRY_DT','MonthTag','YearTag'],right_on=['EXPIRY','MonthTag','YearTag'],how='inner')
    mergeddf['EXPIRY_DT'] = pd.to_datetime(mergeddf['EXPIRY_DT']).dt.date
    mask = (mergeddf['EXPIRY_DT'] != mergeddf.groupby('TIMESTAMP')['EXPIRY_DT'].transform('min')) ## to remove M-2, M-3 contracts
    mergeddf = mergeddf[mask]
    mergeddf = mergeddf.reset_index(drop=True).drop(columns=['MonthTag','YearTag']).rename(columns={"TIMESTAMP":'Date'})
    return mergeddf

def merge_spot(df,eqdf):
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    df = pd.merge(df,eqdf[['Date','Close']],left_on=['TIMESTAMP'],right_on=['Date'],how='left').rename(columns={'Close':'Close_EQ'})
    return df

def getentrydf(df):
    min_dates = df.groupby('EXPIRY_DT')['TIMESTAMP'].min()
    getentrydf = df[df['TIMESTAMP'].isin(min_dates)]
    return getentrydf

def strike_selector(df):
    df['Difference'] = df['STRIKE_PR'].astype(float) - df.groupby('ExpiryDate')['Close_EQ'].transform('first')
    df['Difference'] = df['Difference'].abs()
    # df = df.dropna(subset=['Difference'])
    atmdf = df.loc[df.groupby(['ExpiryDate', 'OPTION_TYP'])['Difference'].idxmin()].drop(columns=['Difference'])
    # atmdf['EXPIRY_DATE'] = pd.to_datetime(atmdf['EXPIRY_DATE']).dt.date
    return atmdf

def gettingspotdata(tradebook,eqdf):
    tradebook['EntryDate'] = pd.to_datetime(tradebook['EntryDate']).dt.date
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    tradebook = pd.merge(tradebook,eqdf[['Date','Close']],left_on='EntryDate',right_on='Date',how='left').rename(columns={'Close':'Close_EQ'}).drop(columns=['Date'])
    return tradebook

def getexitdf(df,atmdf_entry,tradetype):
    if tradetype == 'Long':
        atmdf_exit = pd.merge(df,atmdf_entry[['Ticker','ExpiryDate']],left_on=['Ticker','Date'],right_on=['Ticker','ExpiryDate'],how='inner',suffixes=['','_tracker'])
        atmdf_entry['TradeType'] = 'Long'
        atmdf_exit['TradeType'] = 'Sell'
    else:
        atmdf_exit = pd.merge(df,atmdf_entry[['Ticker','ExpiryDate']],left_on=['Ticker','Date'],right_on=['Ticker','ExpiryDate'],how='inner',suffixes=['','_tracker'])
        atmdf_entry['TradeType'] = 'Short'
        atmdf_exit['TradeType'] = 'Cover'
    return atmdf_entry, atmdf_exit

def get_trades(atmdf_entry,atmdf_exit):
    entrytradesdf = atmdf_entry.rename(columns={'CLOSE':'EntryPrice','TIMESTAMP':'EntryDate'})
    entrytradesdf = entrytradesdf[['Ticker','TradeType','EntryDate','EntryPrice','STRIKE_PR','OPTION_TYP','EXPIRY_DT']].sort_values(by=['EntryDate'])
    exittradesdf = atmdf_exit.rename(columns={'CLOSE':'ExitPrice','Date':'ExitDate'})
    exittradesdf = exittradesdf[['Ticker','ExitDate','ExitPrice','EXPIRY_DT']].sort_values(by=['ExitDate'])
    tradebook = pd.merge(entrytradesdf, exittradesdf, on = ["Ticker", "EXPIRY_DT"], how = "left")
    return tradebook

def tradebook_generator(tradebook,eqdf):
    commission = 0.75
    brokerage = 6
    tradebook['GrossPnL'] = np.where(tradebook['TradeType'] == 'Short',tradebook['EntryPrice'] - tradebook['ExitPrice'],tradebook['ExitPrice'] - tradebook['EntryPrice'])
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook = gettingspotdata(tradebook,eqdf.copy())
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['Close_EQ']) * 100,2)
    tradebook = tradebook.sort_values(by=['EntryDate'])
    tradebook = tradebook.drop_duplicates()
    return tradebook

def update_data(df):
    optquery = f'''
        SELECT * FROM nsefno
        WHERE "TIMESTAMP" > '{df['TIMESTAMP'].max()}' AND "TIMESTAMP" <= '{enddate}'
        AND "Ticker" LIKE '{index}%' AND "INSTRUMENT" = '{instrument}' AND "SYMBOL" = '{index}' ;
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

def monthly_short_straddle_backtest(optdf,eqdf,expirydf,updated_asof):
    opt_df = optdf[(optdf['TIMESTAMP'] >= pd.to_datetime(startdate).date()) & (optdf['TIMESTAMP'] <= pd.to_datetime(enddate).date())].reset_index(drop=True)
    expiry_df = expiry_changes(expirydf.copy())
    opt_df = ticker_change(opt_df.copy(),'Y')
    opt_df['ExpiryDate'] = pd.to_datetime(opt_df['EXPIRY_DT'],errors='coerce').dt.date
    dates_to_ignore = pd.to_datetime(['2024-03-28','2024-04-25','2024-06-27','2024-09-26','2024-12-26']).date
    testexpiry = expiry_df[(expiry_df['DATE']>=pd.to_datetime('2016-05-27').date()) & (expiry_df['MONTHLY']==1)]
    testexpiry = testexpiry[~testexpiry['DATE'].isin(dates_to_ignore)] if index == 'BANKNIFTY' else testexpiry
    testexpiry['StartDate'] = testexpiry['DATE'].shift(1)
    testexpiry['EndDate'] = testexpiry['DATE']
    testexpiry = testexpiry.rename(columns={'DATE':'Date'})
    testexpiry_mask = (testexpiry['Date'] == testexpiry['StartDate']) & (testexpiry['StartDate'] == testexpiry['EndDate'])
    testexpiry = testexpiry[~testexpiry_mask]
    df = pd.merge(opt_df,testexpiry[['Date','StartDate','EndDate','EXPIRY']],left_on='ExpiryDate',right_on='Date',how='left').dropna(subset=['Date']).sort_values(by=['ExpiryDate'])
    df = df[(df['TIMESTAMP'] == df['StartDate']) | (df['TIMESTAMP']==df['EndDate'])].dropna(subset=['StartDate']).sort_values(by=['ExpiryDate','TIMESTAMP']).drop(columns=['Date'])
    mergeddf = merge_spot(df.copy(),eqdf.copy())
    entrydf = getentrydf(mergeddf.copy()).dropna(subset=['Date'])
    atmdf_entry = strike_selector(entrydf.copy())
    atmdf_entry,atmdf_exit = getexitdf(mergeddf.copy(),atmdf_entry,'Short')
    atmdf_exit = atmdf_exit[atmdf_exit['TIMESTAMP']==atmdf_exit['ExpiryDate_tracker']]
    tradebook = get_trades(atmdf_entry,atmdf_exit)
    tradebook = tradebook_generator(tradebook,eqdf.copy())
    print(tradebook['PnL_after_commission'].sum())
    os.makedirs(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyStraddleSelling\{index}\Tradebook\\Updated_{updated_asof}",exist_ok=True)
    tradebook.to_csv(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyStraddleSelling\{index}\Tradebook\\Updated_{updated_asof}\\{index.capitalize()}_{tradetype}Straddle_Monthly.csv",index=False)
    return tradebook

def nav_calculation(tradebook):
    def file_reader():
        optdf = pd.read_pickle(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\Rawfiles\{index}\{index}_OPT.pkl")
        eqdf = pd.read_csv(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\Rawfiles\{index}\{index}Spot.csv",parse_dates=['Date'],dayfirst=True)
        return optdf,eqdf

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
        optquery = f'''SELECT * FROM nsefno WHERE "TIMESTAMP" > '{df['TIMESTAMP'].max()}' AND "TIMESTAMP" <= '{enddate}' AND "Ticker" LIKE '{index.upper()}%' AND "INSTRUMENT" = '{instrument}'; '''
        connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
        data = pd.read_sql(optquery,connection)
        ddf = pd.concat([df,data],ignore_index=True)
        return ddf

    def update_spot(eqdf):
        spotquery = f''' SELECT * FROM spotdata WHERE "Symbol" = '{index.upper()}' AND "Date" > '{eqdf['Date'].max()}' order by "Date", "Time"; '''
        connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
        equitydf = pd.read_sql(spotquery,connection)
        spotdata = convert_eq_to_daily_data(equitydf.copy())
        eqdf_ = pd.concat([eqdf,spotdata],ignore_index=True)
        return eqdf_

    def overnight_nav_calculator(df,tradebook,eqdf,index):
        df['Date'] = df['TIMESTAMP']
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df['Ticker'] = np.where(df['Ticker'].str.contains('29JUN23'),df['Ticker'].str.replace('29JUN23','28JUN23'),df['Ticker'])
        if index.upper() == 'BANKNIFTY':
            df['Ticker'] = np.where((df['Ticker'].str.contains('30JAN25')) & (df['Date'] >= pd.to_datetime('2025-01-02').date()),df['Ticker'].str.replace('30JAN25','29JAN25'),df['Ticker'])
        tradebook['EntryDate'] = pd.to_datetime(tradebook['EntryDate']).dt.date
        mergeddf = pd.merge(df,tradebook[['Ticker','EntryDate','ExitDate','TradeType']],on='Ticker')
        mergeddf = mergeddf[(mergeddf['Date'] >= pd.to_datetime(mergeddf['EntryDate']).dt.date) & (mergeddf['Date'] <= pd.to_datetime(mergeddf['ExitDate']).dt.date)]
        mergeddf = mergeddf.sort_values(by=['Date']).reset_index(drop=True)
        mergeddf['PrevClose'] = np.where(mergeddf['Date'] != mergeddf['EntryDate'],mergeddf.groupby('Ticker')['CLOSE'].shift(1),np.nan)
        mergeddf['DailyPnL'] = np.where(pd.notna((mergeddf['PrevClose']) == True) & (mergeddf['TradeType'] == 'Short'),mergeddf['PrevClose'] - mergeddf['CLOSE'],
                                            np.where(pd.notna((mergeddf['PrevClose']) == True) & (mergeddf['TradeType'] == 'Long'),mergeddf['CLOSE'] - mergeddf['PrevClose'],0))
        mergeddf['DaySum'] = mergeddf.groupby(['Date'])['DailyPnL'].transform('sum')
        mergeddf_unique = mergeddf.drop_duplicates(subset=['Date']).reset_index(drop=True)
        mergeddf_unique = mergeddf_unique[['Date','DaySum']]
        
        mergeddf_unique['Date'] = pd.to_datetime(mergeddf_unique['Date'])
        eqdf['Date'] = pd.to_datetime(eqdf['Date'])
        mergeddf_unique = pd.merge(mergeddf_unique,eqdf[['Date','Close']],on='Date',how='left').rename(columns={'Close':'Close_EQ'})
        mergeddf_unique['DayPnL%'] = round((mergeddf_unique['DaySum'] / mergeddf_unique['Close_EQ']),5)
        mergeddf_unique.loc[0,'NAV'] = 100
        for i in range(1,len(mergeddf_unique)):
            mergeddf_unique.loc[i,'NAV'] = mergeddf_unique.loc[i-1,'NAV'] * (1+mergeddf_unique.loc[i,'DayPnL%'])
        
        return mergeddf_unique[['Date','NAV']]

    def data_updater():
        optdf,eqdf = file_reader()
        updateddf = update_data(optdf.copy())
        optdf_ = updateddf.copy()
        optdf_ = optdf_[optdf_['SYMBOL']==index.upper()]
        updatedeqdf = update_spot(eqdf.copy())
        eqdf_ = updatedeqdf.copy()
        eqdf_['Date'] = pd.to_datetime(eqdf_['Date'])
        tradebook['EntryDate'] = pd.to_datetime(tradebook['EntryDate'])
        tradebook['ExitDate'] = pd.to_datetime(tradebook['ExitDate'])
        eqdf_ = eqdf_[(eqdf_['Date'] >= tradebook['EntryDate'].min()) & (eqdf_['Date'] <= tradebook['ExitDate'].max())].reset_index(drop=True)
        return tradebook,optdf_,eqdf_

    def calculate_nav():
        tradebook,optdf_,eqdf_ = data_updater()
        nav = overnight_nav_calculator(optdf_.copy(),tradebook.copy(),eqdf_.copy(),index)
        nav = pd.merge(nav,eqdf_[['Date']],on=['Date'],how='right').ffill()
        
        base_dir = rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyStraddleSelling\{index}\NAVs\Updated_{updated_asof}"
        os.makedirs(base_dir, exist_ok=True)

        output_path = os.path.join(base_dir,f"{index.capitalize()}_{tradetype}Straddle_{timeframe.capitalize()}_NAV.xlsx")
        nav.to_excel(output_path)
    
    calculate_nav()

def deltaCalculator():
    df = pd.read_csv(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyStraddleSelling\{index}\Tradebook\Updated_{updated_asof}\{index}_{tradetype}Straddle_{timeframe}.csv")
    nav = pd.read_excel(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyStraddleSelling\{index}\NAVs\Updated_{updated_asof}\{index}_{tradetype}Straddle_{timeframe}_NAV.xlsx")
    nav = nav.loc[:,~nav.columns.str.contains('^Unnamed')]

    def initialization(df,nav):
        df = df.dropna(subset=['PnL_after_commission'])
        df['Ticker'] = np.where(df['Ticker'].str.contains('28JUN23','29JUN23'),df['Ticker'].str.replace('28JUN23','29JUN23'),df['Ticker'])
        df['EntryDate'] = pd.to_datetime(df['EntryDate'], format='mixed',dayfirst=True)
        df['EXPIRY_DT'] = pd.to_datetime(df['EXPIRY_DT'], format='mixed',dayfirst=True)
        df['ExitDate'] = pd.to_datetime(df['ExitDate'], format='mixed',dayfirst=True)
        lastdate = df['ExitDate'].dt.date.max()
        nav['Date'] = pd.to_datetime(nav['Date'])
        unique_dates = nav['Date'].dt.date.unique()
        long_positions = {date: [] for date in unique_dates}
        short_positions = {date: [] for date in unique_dates}
        for index_, row in df.iterrows():
            ticker = row['Ticker']
            trade_type = row['TradeType']
            entry_date = row['EntryDate']
            expiry_date = row['EXPIRY_DT']
            exit_date = row['ExitDate']
            effective_end_date = min(exit_date, expiry_date)
            for date in unique_dates:
                if entry_date.date() <= date < effective_end_date.date():
                    if (expiry_date.date() != exit_date.date()) & (date < exit_date.date()):
                        long_positions[date].append(ticker) if trade_type == 'Long' else short_positions[date].append(ticker)
                    elif expiry_date.date() == exit_date.date():
                        long_positions[date].append(ticker) if trade_type == 'Long' else short_positions[date].append(ticker)
                elif (date == lastdate) & (date == exit_date):
                    long_positions[date].append(ticker) if trade_type == 'Long' else short_positions[date].append(ticker)
        word = tradetype
        dates = []
        long_tickers = []
        short_tickers = []
        for date in unique_dates:
            dates.append(date)
            long_tickers.append(long_positions[date])
            short_tickers.append(short_positions[date])
        summary_df = pd.DataFrame({
            'Date': dates,
            f'{word}Tickers': long_tickers if tradetype == 'Long' else short_tickers})
        summary_df = summary_df[summary_df[f'{word}Tickers'].apply(len) > 0]
        splittingdf = pd.DataFrame(summary_df)
        splittingdf[[f'{word}Ticker1',f'{word}Ticker2']] = pd.DataFrame(splittingdf[f'{word}Tickers'].tolist(), index=splittingdf.index)
        splittingdf = splittingdf.drop(columns=[f'{word}Tickers'])
        ticker_columns = [f'{word}Ticker1', f'{word}Ticker2']
        mask = splittingdf['Date'] == pd.to_datetime('2024-02-29').date()
        for col in ticker_columns:
            splittingdf.loc[mask, col] = splittingdf.loc[mask, col].str.replace('27MAR24', '28MAR24')

        return splittingdf,index,tradetype

    def calculate_delta(df,nav):
        engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'
        db_conn = create_engine(engine_url, connect_args={"connect_timeout": 7})
        db_conn.connect()
        ddf,index,tradetype = initialization(df.copy(),nav.copy())
        position = tradetype
        ddf = ddf.rename(columns={'TIMESTAMP':'Date'})
        ddf = ddf.loc[:,~ddf.columns.str.contains('^Unnamed')]
        def replace_ticker_strikes(row, index):
            strike = str(row[f'{index}Strike'])
            row[f'{index}Ticker'] = row[f'{index}Ticker'].replace(strike, '')
            return row
        ddf = ddf.rename(columns={'TIMESTAMP': 'Date'})
        ddf['Date'] = pd.to_datetime(ddf['Date']).dt.date
        ddf = ddf.loc[:, ~ddf.columns.str.contains('^Unnamed')]
        ddf.columns = ddf.columns.str.replace(' ', '', regex=False)
        tickers = [col for col in ddf.columns if 'Ticker' in col]
        new_tickers = [ticker.replace('Ticker', '') + 'Ticker' for ticker in tickers]
        rename_mapping = {old: new for old, new in zip(tickers, new_tickers)}
        ddf = ddf.rename(columns=rename_mapping)
        for ticker in new_tickers:
            ddf[ticker] = ddf[ticker].str.replace('.NFO', '')
            print(ticker)
            if (ddf[ticker].str[-1:].unique() == 'E'): ## to check if the ticker formatting is in the format we want
                index = ticker.replace('Ticker', '')
                ddf = ddf.apply(replace_ticker_strikes, axis=1, args=(index,))
                ddf[ticker] = ddf[ticker] + ddf[f'{index}Strike'].astype(str)
        for ticker in new_tickers:
            tickerlist = list(ddf[ticker])
            fieldName = 'Delta'
            ticker_str = "("+",".join(["'"+i.lower()+"'" for i in tickerlist])+")"
            query = f'select "Ticker", "Date",  \"{fieldName}\" from greeks_nsefno where lower("Ticker") in {ticker_str};'
            df = pd.read_sql(query, db_conn)
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            ddf_merged = pd.merge(ddf,df,left_on=[f'{ticker}','Date'],right_on=['Ticker','Date'],how='inner').rename(columns={'Delta':f"{ticker[:ticker.find('Ticker')]}Delta"})
            ddf = pd.merge(ddf,ddf_merged[['Date',f"{ticker[:ticker.find('Ticker')]}Delta"]],on='Date',how='inner')
            
        os.makedirs(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyStraddleSelling\{index}\Delta\LongShort\Updated_{updated_asof}\\",exist_ok=True)
        ddf.to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyStraddleSelling\{index}\Delta\LongShort\Updated_{updated_asof}\{index}_{position}StraddleDelta.xlsx")
    
    calculate_delta(df.copy(),nav.copy())

if __name__ == '__main__':
    updateddf = update_data(optdf.copy())
    optdf = updateddf.copy()
    optdf = optdf[optdf['SYMBOL']==index]
    expirydf = update_expiry_data(expirydf.copy())
    updatedeqdf = update_spot(eqdf.copy())
    eqdf = updatedeqdf.copy()
    print(f"Running Backtest for {index.capitalize()} Monthly {tradetype} Straddle")
    tradebook = monthly_short_straddle_backtest(optdf,eqdf,expirydf,updated_asof)
    print("BACKTEST UPDATED!!")
    print(f"GENERATING NAVs for {index} MONTHLY {tradetype.upper()} STRADDLE STRATEGY")
    nav_calculation(tradebook.copy())
    print(f"CALCULATING DELTA FOR {index} MONTHLY {tradetype.upper()} STRADDLE STRATEGY")
    deltaCalculator()
    print("COMPLETED!!")