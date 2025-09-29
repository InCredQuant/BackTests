import pandas as pd
import numpy as np
import os
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
from date_config import *

updated = 'Y' ## This flag to be turned on if we are updating the backtest
any_rollover_needed = 'N' ## incase you want to shift strikes based on move in underlying, N for basecase - always buying 3% OTM

type = 'Long'
timeframe = 'WEEKLY'
index = 'NIFTY'
instrument = 'OPTIDX'
# optiontype = 'PE'
optiontypes = ['PE','CE']

date_mapping = {
    ('WEEKLY', 'BANKNIFTY'): '2016-05-27',
    ('WEEKLY', 'NIFTY'): '2019-02-11'
}

startdate = date_mapping.get((timeframe, index))
# enddate = '2025-04-25'
# updated_asof = datetime.strptime(enddate, '%Y-%m-%d').strftime('%d%m%Y') ## Enter the date you're updating on in the format %dd%mm%yyyy
premium_percent = 3 if timeframe == 'WEEKLY' else 15

os.makedirs(fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index.capitalize()}\Tradebook\HedgeBuying\{timeframe.capitalize()}_Hedge\\Updated_{updated_asof}",exist_ok=True)

outputpath = fr"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index.capitalize()}\Tradebook\HedgeBuying\{timeframe.capitalize()}_Hedge\\Updated_{updated_asof}"

def get_data():

    optquery = f''' SELECT * FROM NSEFNO WHERE "SYMBOL" = '{index}' and "TIMESTAMP" >= '{startdate}' and "TIMESTAMP" <= '{enddate}' and "Ticker" like '{index}%' and "INSTRUMENT" = '{instrument}'; '''
    expiryquery = f''' SELECT * FROM nseexpiry WHERE "INSTRUMENT" = '{instrument}' AND "{timeframe}" = 1; '''
    spotquery = f''' SELECT * FROM spotdata WHERE "Symbol" = '{index}' and "Date" >= '{startdate}' order by "Date", "Time"; '''
    connection = pg.connect(database="data", user="postgres", password="postgres", host="192.168.44.4", port=5432)
    data = pd.read_sql(optquery,connection)
    expirydata = pd.read_sql(expiryquery,connection)
    expirydata = expirydata[expirydata['SYMBOL']==index].sort_values(by=['DATE']).reset_index(drop=True)
    spotdata = pd.read_sql(spotquery,connection)
    connection.close()
    return data,expirydata,spotdata

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

def preprocess(optdf,expirydf,optiontype):
    opt_df = optdf[(optdf['OPTION_TYP']==optiontype) & (optdf['SYMBOL']==index)]
    expiry_df = expiry_changes(expirydf.copy())
    opt_df = ticker_change(opt_df.copy(),'Y')
    opt_df = opt_df[(opt_df['TIMESTAMP'] >= pd.to_datetime(startdate).date()) & (opt_df['TIMESTAMP'] <= pd.to_datetime(enddate).date())].reset_index(drop=True)
    return opt_df,expiry_df

def filter_out_strikes(df):
    df['StrikeInterval'] = df['STRIKE_PR'].astype(float) % 100
    df = df[df['StrikeInterval'] == 0]
    return df

def ticker_change(df,flag):
    if index == 'BANKNIFTY':
        df['Ticker'] = df['Ticker'].str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('29JUN23','28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
    elif index == 'NIFTY':
        df['Ticker'] = df['Ticker'].str.replace('29JUN23','28JUN23').str.replace('24APR14','23APR14')
        if flag == 'Y':
            df['EXPIRY_DT'] = df['EXPIRY_DT'].str.replace('29JUN23','28JUN23').str.replace('24APR14','23APR14')
    return df

## Accounting for the changes in expiry - for example 29JUN23 expiry got changed to 30JUN23.
def expiry_changes(expirydf):
    if index == 'BANKNIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('29JUN23', '28JUN23').str.replace('07SEP23','06SEP23').str.replace('28MAR24','27MAR24').str.replace('25APR24','24APR24').str.replace('27JUN24','26JUN24')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-06-29').date()) | (expirydf['DATE'] == pd.to_datetime('2024-03-28').date()) | (expirydf['DATE'] == pd.to_datetime('2024-04-25').date()) | (expirydf['DATE'] == pd.to_datetime('2024-06-27').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    elif index == 'NIFTY':
        expirydf['EXPIRY'] = expirydf['EXPIRY'].str.replace('29JUN23', '28JUN23').str.replace('24APR14','23APR14')
        expirydf['DATE'] = np.where((expirydf['DATE'] == pd.to_datetime('2023-06-29').date()) | (expirydf['DATE'] == pd.to_datetime('2014-04-24').date()),expirydf['DATE'] - timedelta(days=1),expirydf['DATE'])
    return expirydf

def merge_spot(df,eqdf):
    eqdf['Date'] = pd.to_datetime(eqdf['Date']).dt.date
    df = pd.merge(df,eqdf[['Date','Close']],left_on=['TIMESTAMP'],right_on='Date',how='left').rename(columns={'Close':'Close_EQ'}).drop(columns='Date')
    return df

def strike_selector(df):
    df['Difference'] = df['STRIKE_PR'].astype(float) - df['Close_EQ']
    df['Difference'] = df['Difference'].abs()
    atmdf = df.loc[df.groupby(['TIMESTAMP'])['Difference'].idxmin()].drop(columns=['Difference'])
    # atmdf['TargetPrice'] = atmdf['CLOSE'] * premium_percent/100
    return atmdf

def hedge_strike_selector(period_df,df,premium_percent):
    df['Difference'] = df['STRIKE_PR'].astype(float) - (df['STRIKE_PR'].astype(float) * premium_percent/100)
    # df = df[df['Difference'] <= premium_percent] ## NEWLY ADDED
    maindf = pd.merge(period_df,df[['TIMESTAMP','EXPIRY_DT','Difference','DATE','NEXTDATE']],on=['TIMESTAMP','EXPIRY_DT'],how='inner')
    maindf['OTM%'] = (maindf['Close_EQ']/maindf['STRIKE_PR'].astype(float) - 1) * 100
    maindf = maindf[maindf['OTM%'] <= premium_percent] if optiontype == 'PE' else maindf[(maindf['OTM%']<=0) & ((maindf['OTM%']*-1) <= premium_percent)]
    maindf['StrikeDifference'] = (maindf['STRIKE_PR'].astype(float) - maindf['Difference']).abs()
    # maindf.to_csv(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\Nifty\Tradebook\HedgeBuying\Weekly_Hedge\Updated_16092024\Nifty_CEWeeklyHedge_3%_test_new.csv")
    hedgestrike = maindf.loc[maindf.groupby(['TIMESTAMP'])['StrikeDifference'].idxmin()].drop(columns=['Difference','StrikeDifference']) if optiontype == 'PE' else maindf.loc[maindf.groupby(['TIMESTAMP'])['StrikeDifference'].idxmax()].drop(columns=['Difference','StrikeDifference'])
    return hedgestrike

def check_if_rollover_needed(df,flag):
    df = df.reset_index(drop=True)
    df['STRIKE_PR'] = df['STRIKE_PR'].astype(float)
    df['StrikeToHedge'] = df['STRIKE_PR'].cummax() if flag == 'Y' else df['STRIKE_PR']
    return df

def getentrydf(df,hedgestrike):
    df['STRIKE_PR'] = df['STRIKE_PR'].astype(float)
    hedge_entry = pd.merge(df,hedgestrike[['DATE','NEXTDATE','StrikeToHedge']],left_on=['TIMESTAMP','EXPIRY_DT','STRIKE_PR'],right_on=['DATE','NEXTDATE','StrikeToHedge'],how='inner')
    return hedge_entry

def getexitdf(df,hedge_entry):
    hedge_exit = pd.merge(df,hedge_entry[['Ticker','EXPIRY_DT']],left_on=['Ticker','TIMESTAMP'],right_on=['Ticker','EXPIRY_DT'],how='inner',suffixes=['','_tracker'])
    hedge_entry['TradeType'] = 'Long'
    hedge_exit['TradeType'] = 'Sell'
    hedge_exit = hedge_exit.drop_duplicates()
    return hedge_entry, hedge_exit

def get_trades(hedge_entry,hedge_exit):
    hedge_entry['Price'] = np.where(hedge_entry['CONTRACTS']==0,hedge_entry['SETTLE_PR'],hedge_entry['CLOSE'])
    entrytradesdf = hedge_entry.rename(columns={'Price':'EntryPrice','TIMESTAMP':'EntryDate'})
    entrytradesdf = entrytradesdf[['Ticker','TradeType','EntryDate','EntryPrice','STRIKE_PR','OPTION_TYP','EXPIRY_DT','Close_EQ']].sort_values(by=['EntryDate'])
    exittradesdf = hedge_exit.rename(columns={'CLOSE':'ExitPrice','TIMESTAMP':'ExitDate'})
    exittradesdf = exittradesdf[['Ticker','ExitDate','ExitPrice','EXPIRY_DT']].sort_values(by=['ExitDate'])
    tradebook = pd.merge(entrytradesdf, exittradesdf, on = ["Ticker", "EXPIRY_DT"], how = "left")
    return tradebook.drop_duplicates()

def tradebook_generator(tradebook):
    commission = 0
    brokerage = 6
    tradebook['GrossPnL'] = np.where(tradebook['TradeType'] == 'Short',tradebook['EntryPrice'] - tradebook['ExitPrice'],tradebook['ExitPrice'] - tradebook['EntryPrice'])
    tradebook['PnL_after_commission'] = tradebook['GrossPnL'] - ((tradebook['EntryPrice'] + tradebook['ExitPrice']) * commission/100)
    tradebook['PnL%'] = round((tradebook['PnL_after_commission'] / tradebook['Close_EQ']) * 100,2)
    # tradebook['Final_PnL%'] = tradebook['PnL%'] - (((brokerage*2) / (tradebook['Close_EQ'] * 15))*100)
    tradebook = tradebook.sort_values(by=['EntryDate'])
    tradebook = tradebook.drop_duplicates()
    
    return tradebook

def periodic_put_buying(opt_df, expiry_df, eqdf,rollover_flag,period='MONTHLY'):
    # Select expiry dates based on period
    if period == 'YEARLY':
        period_expiry_df = expiry_df[(expiry_df['EXPIRY'].str.contains('DEC')) & (expiry_df['MONTHLY'] == 1)]
    elif period == 'QUARTERLY':
        period_expiry_df = expiry_df[(expiry_df['EXPIRY'].str.contains('MAR|JUN|SEP|DEC',regex=True)) & (expiry_df['MONTHLY'] == 1)]
    elif period == 'WEEKLY':
        period_expiry_df = expiry_df[expiry_df['WEEKLY'] == 1]
    else:  # MONTHLY
        period_expiry_df = expiry_df[expiry_df['MONTHLY'] == 1]
    
    period_expiry_df['NEXTDATE'] = period_expiry_df['DATE'].shift(-1)
    opt_df['EXPIRY_DT'] = pd.to_datetime(opt_df['EXPIRY_DT'], format='%d%b%y').dt.date
    filtered_opt_df = filter_out_strikes(opt_df.copy())
    period_df = filtered_opt_df[filtered_opt_df['TIMESTAMP'].isin(period_expiry_df['DATE'])]
    period_df = period_df.sort_values(by=['TIMESTAMP', 'EXPIRY_DT'])
    period_df = merge_spot(period_df.copy(), eqdf.copy())
    
    filtered_df = pd.merge(period_df, period_expiry_df[['DATE', 'NEXTDATE']], 
                           left_on=['TIMESTAMP'], right_on='DATE', how='inner')
    filtered_df = filtered_df[filtered_df['EXPIRY_DT'] == filtered_df['NEXTDATE']]
    atm_df = strike_selector(filtered_df.copy())
    # strike_to_choose = pd.merge(filtered_df, atm_df[['TIMESTAMP']], on='TIMESTAMP', how='inner')
    hedge_strike_entry = hedge_strike_selector(period_df.copy(),atm_df.copy(),premium_percent)
    # # hedge_strike_entry.to_csv(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\Nifty\Tradebook\HedgeBuying\Weekly_ShiftStrike\hedgestrikeentry.csv",index=False)
    hedgestrike = check_if_rollover_needed(hedge_strike_entry.copy(),rollover_flag)
    # hedgestrike.to_csv(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\Nifty\Tradebook\HedgeBuying\Weekly_ShiftStrike\hedgestrike.csv",index=False)
    hedgeentry = getentrydf(period_df.copy(),hedgestrike.copy())
    # hedgeentry.to_csv(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\Nifty\Tradebook\HedgeBuying\Weekly_ShiftStrike\hedgeentry.csv",index=False)
    hedgeentry, hedgexit = getexitdf(period_df.copy(), hedgeentry.copy())
    tradebook = get_trades(hedgeentry, hedgexit)
    tradesheet = tradebook_generator(tradebook.copy())
    output_file = f"{index.capitalize()}_{optiontype}{period.capitalize()}Hedge_{premium_percent}%.csv"
    tradesheet.to_csv(os.path.join(outputpath, output_file), index=False)

def backtest(optiontype):
    optdf, expirydf, eqdf = get_data()
    eqdf_daily = convert_eq_to_daily_data(eqdf.copy())
    opt_df, expiry_df = preprocess(optdf.copy(),expirydf.copy(),optiontype)
    periodic_put_buying(opt_df.copy(), expiry_df.copy(), eqdf_daily.copy(),any_rollover_needed,period=timeframe)
    return opt_df,expiry_df,eqdf_daily

def nav_calculation(optdf,expirydf,eqdf_daily):

    def overnight_nav_calculator(df,tradebook,eqdf,index):
        df['Date'] = df['TIMESTAMP']
        df['Ticker'] = np.where(df['Ticker'].str.contains('29JUN23'),df['Ticker'].str.replace('29JUN23','28JUN23'),df['Ticker'])
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
        mergeddf_unique = pd.merge(mergeddf_unique,eqdf[['Date','Close']],on='Date',how='left').rename(columns={'Close':'Close_EQ'})
        mergeddf_unique['DayPnL%'] = round((mergeddf_unique['DaySum'] / mergeddf_unique['Close_EQ']),5)
        mergeddf_unique.loc[0,'NAV'] = 100
        for i in range(1,len(mergeddf_unique)):
            mergeddf_unique.loc[i,'NAV'] = mergeddf_unique.loc[i-1,'NAV'] * (1+mergeddf_unique.loc[i,'DayPnL%'])
        
        return mergeddf_unique[['Date','NAV']]

    navfolderpath = rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index}\NAVs\HedgeBuying\WeeklyHedge\\"
    os.makedirs(os.path.join(navfolderpath,updated_asof),exist_ok=True)
    filepath = rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index}\Tradebook\HedgeBuying\\Weekly_Hedge\\Updated_{updated_asof}\\{index}_{optiontype}{timeframe.capitalize()}Hedge_{premium_percent}%.csv"
    tradebook = pd.read_csv(filepath,parse_dates=['EntryDate','ExitDate'],dayfirst=True)
    tradebook = tradebook.dropna(subset=['ExitDate'])
    opt_df, expiry_df = preprocess(optdf.copy(),expirydf.copy(),optiontype)
    eqdf_ = eqdf_daily.copy()
    eqdf_['Date'] = pd.to_datetime(eqdf_daily['Date']).dt.date
    nav = overnight_nav_calculator(opt_df.copy(),tradebook.copy(),eqdf_.copy(),index)
    nav = pd.merge(nav,eqdf_[['Date']],on=['Date'],how='right').ffill()
    output_path = rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index}\NAVs\\HedgeBuying\\WeeklyHedge\\{updated_asof}\\{index.capitalize()}_{optiontype}{timeframe.capitalize()}_{premium_percent}%_NAV.xlsx"
    nav.to_excel(output_path)

def deltaCalculator():
    df = pd.read_csv(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index}\Tradebook\HedgeBuying\\Weekly_Hedge\\Updated_{updated_asof}\{index}_{optiontype}WeeklyHedge_3%.csv")
    nav = pd.read_excel(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index}\NAVs\HedgeBuying\\WeeklyHedge\\{updated_asof}\\{index}_{optiontype}Weekly_3%_NAV.xlsx")
    
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
        word = type
        dates = []
        long_tickers = []
        short_tickers = []
        for date in unique_dates:
            dates.append(date)
            long_tickers.append(long_positions[date])
            short_tickers.append(short_positions[date])
        summary_df = pd.DataFrame({
            'Date': dates,
            f'{word}Tickers': long_tickers if type == 'Long' else short_tickers
        })
        summary_df = summary_df[summary_df[f'{word}Tickers'].apply(len) > 0]
        splittingdf = pd.DataFrame(summary_df)
        splittingdf[[f'{word}Ticker1']] = pd.DataFrame(splittingdf[f'{word}Tickers'].tolist(), index=splittingdf.index)
        splittingdf = splittingdf.drop(columns=[f'{word}Tickers'])
        ticker_columns = [f'{word}Ticker1']
        mask = splittingdf['Date'] == pd.to_datetime('2024-02-29').date()
        for col in ticker_columns:
            splittingdf.loc[mask, col] = splittingdf.loc[mask, col].str.replace('27MAR24', '28MAR24')

        return splittingdf,index,type

    def calculate_delta(df,nav):
        engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'
        db_conn = create_engine(engine_url, connect_args={"connect_timeout": 7})
        db_conn.connect()
        ddf,index,type = initialization(df.copy(),nav.copy())
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
                index_name = ticker.replace('Ticker', '')
                ddf = ddf.apply(replace_ticker_strikes, axis=1, args=(index_name,))
                ddf[ticker] = ddf[ticker] + ddf[f'{index_name}Strike'].astype(str)
        for ticker in new_tickers:
            tickerlist = list(ddf[ticker])
            fieldName = 'Delta'
            ticker_str = "("+",".join(["'"+i.lower()+"'" for i in tickerlist])+")"
            query = f'select "Ticker", "Date",  \"{fieldName}\" from greeks_nsefno where lower("Ticker") in {ticker_str};'
            df = pd.read_sql(query, db_conn)
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            ddf_merged = pd.merge(ddf,df,left_on=[f'{ticker}','Date'],right_on=['Ticker','Date'],how='inner').rename(columns={'Delta':f"{ticker[:ticker.find('Ticker')]}Delta"})
            ddf = pd.merge(ddf,ddf_merged[['Date',f"{ticker[:ticker.find('Ticker')]}Delta"]],on='Date',how='inner')
        
        os.makedirs(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index.capitalize()}\Delta\HedgeBuying\Updated_{updated_asof}\\",exist_ok=True)        
        ddf.to_excel(rf"C:\Vishwanath\PythonCodes\Strategy\MonthlyBuying\{index.capitalize()}\Delta\HedgeBuying\Updated_{updated_asof}\{index.capitalize()}_{optiontype}WeeklyHedge_3%_Delta.xlsx")
        
    calculate_delta(df.copy(),nav.copy())

if __name__ == '__main__':
    for optiontype in optiontypes:
        print(f"Running Backtest for {index.capitalize()} Weekly Hedge Buying - {optiontype} {premium_percent}% OTM")
        opt_df,expiry_df,eqdf_daily = backtest(optiontype)
        print("BACKTEST UPDATED!!")
        print(f"GENERATING NAVs for WEEKLY HEDGES {index} {optiontype} {timeframe} {premium_percent}%")
        nav_calculation(opt_df.copy(),expiry_df.copy(),eqdf_daily.copy())
        print(f"CALCULATING DELTA FOR {index} WEEKLY HEDGES {optiontype} {timeframe} {premium_percent}%")
        deltaCalculator()
        print("COMPLETED!!")