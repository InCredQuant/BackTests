from datetime import timedelta, date, datetime
import pandas as pd
import psycopg2 as pg
from io import StringIO
import os
import numpy as np
import warnings
warnings.simplefilter(action='ignore')
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO
import sys
sys.path.insert(0, 'G:\\Shared drives\\BackTests\\pycode\\DBUpdation\\')

output_path = rf"G:\Shared drives\BackTests\Vishwanath\Bhavcopy"
start_date = '2025-09-12'                        ## enter the date you want to fetch bhavcopy from
end_date = '2025-09-18'                          ## enter the date you want to fetch bhavcopy till

def date_traverser(start,end):
    results = []
    for n in range(int((end - start).days)+1):
        end_date = start+timedelta(n)
        if pd.Timestamp(end_date).dayofweek <= 5:
            newformat = end_date.strftime("%d%m%Y")
            finalformat = end_date.strftime("%d%b%Y").upper()
            year = end_date.strftime("%Y")
            month = end_date.strftime("%b").upper()
            date = end_date.strftime("%d")
            nummonth = end_date.strftime("%m")
            results.append((year, month, finalformat, date, newformat, nummonth))
    return results

def fetch_bhavcopy(exchange,start_date,end_date):
    path = output_path
    downloadpath = os.path.join(path,str(start_date.year))
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-sh-usage')
    prefs = {
    'download.default_directory': downloadpath,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing_for_trusted_sources_enabled": False,
    "safebrowsing.enabled": False
    }
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(service=service, options=options)
    date_values = date_traverser(start_date,end_date)
    for year, month, finalformat, date, newformat, nummonth in date_values:
        for security in ['fo','cm']:
            counter = 'Equity' if security in 'cm' else 'Derivative'
            try:
                link = f'https://nsearchives.nseindia.com/content/{security}/BhavCopy_NSE_{security.upper()}_0_0_0_{year}{nummonth}{date}_F_0000.csv.zip' if exchange == 'NSE' else f'https://www.bseindia.com/download/BhavCopy/{counter}/BhavCopy_BSE_{security.upper()}_0_0_0_{year}{nummonth}{date}_F_0000.CSV' 
                driver.get(link)
                time.sleep(2)
            except:
                print(link)

def eq_bhavcopy_upload(exchange):
    def ReadFile(zipfilename):
        columns = ['SYMBOL','SERIES','OPEN','HIGH','LOW','CLOSE','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN']
        # df = pd.read_csv(zipfilename, compression='zip')
        df = pd.read_csv(zipfilename)
        column_mapping = {'TckrSymb': 'SYMBOL','SctySrs': 'SERIES','OpnPric': 'OPEN','HghPric': 'HIGH','LwPric': 'LOW','ClsPric': 'CLOSE',
        'LastPric': 'LAST','PrvsClsgPric': 'PREVCLOSE','TtlTradgVol': 'TOTTRDQTY','TtlTrfVal': 'TOTTRDVAL','TradDt': 'TIMESTAMP','TtlNbOfTxsExctd': 'TOTALTRADES',
        'ISIN': 'ISIN'}
        df = df.rename(columns=column_mapping)
        df = df.loc[:,columns]
        df['SERIES'] = np.where(df['SERIES'].isna(),'NA',df['SERIES'])
        return df
    
    folder = os.path.join(output_path,fr"{datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y')}")
    for files in os.listdir(folder):
        if 'cm'.upper() in files:
            df = ReadFile(os.path.join(folder,files))
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.date
            temp_date = df['TIMESTAMP'][0]
            df.reset_index(drop=True,inplace=True)
            try:
                df.to_csv(os.path.join(output_path,rf"nse_cash_{temp_date}.csv"),index=False)
                print(f"Saved {exchange} Cash Bhavcopy for : {temp_date}")
            except Exception as e:
                print(e)

def opt_bhavcopy_upload(exchange):
    def ReadFile(zipfilename,exchange):
        cols = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN','HIGH', 'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH','OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']
        df = pd.read_csv(zipfilename, compression='zip') if exchange == 'NSE' else pd.read_csv(zipfilename)
        column_mapping = {'FinInstrmTp': 'INSTRUMENT','TckrSymb': 'SYMBOL','XpryDt': 'EXPIRY_DT','StrkPric': 'STRIKE_PR','OptnTp': 'OPTION_TYP',
        'OpnPric': 'OPEN','HghPric': 'HIGH','LwPric': 'LOW','SttlmPric': 'SETTLE_PR','TtlTradgVol': 'CONTRACTS',
        'TtlTrfVal': 'VAL_INLAKH','OpnIntrst': 'OPEN_INT','ChngInOpnIntrst': 'CHG_IN_OI','TradDt': 'TIMESTAMP'}
        if exchange == 'BSE':
            column_mapping['LastPric'] = 'CLOSE'
        elif exchange == 'NSE':
            column_mapping['ClsPric'] = 'CLOSE'
            column_mapping.pop('LastPric', None)
        instrument_mapping = {'IDO': 'OPTIDX','STO': 'OPTSTK','IDF': 'FUTIDX','STF': 'FUTSTK'}
        df = df.rename(columns=column_mapping)
        df = df.loc[:,cols]
        df['INSTRUMENT'] = df['INSTRUMENT'].map(instrument_mapping)
        df['VAL_INLAKH'] = df['VAL_INLAKH'] / pow(10,5)
        df['STRIKE_PR'] = np.where((df['INSTRUMENT'] == 'FUTIDX') | (df['INSTRUMENT'] == 'FUTSTK'), 0, df['STRIKE_PR'])
        df['OPTION_TYP'] = np.where((df['INSTRUMENT'] == 'FUTIDX') | (df['INSTRUMENT'] == 'FUTSTK'), 'XX', df['OPTION_TYP'])
        try:
            df['EXPIRY_DT'] = pd.to_datetime(df['EXPIRY_DT'], format="%Y-%m-%d").dt.strftime('%d%b%y').str.upper()
        except:
            try:
                df['EXPIRY_DT'] = pd.to_datetime(df['EXPIRY_DT'], format="%Y-%m-%d").dt.strftime('%d%b%y').str.upper()
            except:
                raise
        try:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format="%Y-%m-%d").dt.date
        except:
            try:
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format="%Y-%m-%d").dt.date
            except:
                raise
        temp1 = df.loc[df.STRIKE_PR.astype('int') == df.STRIKE_PR].copy()
        temp1.STRIKE_PR = temp1.STRIKE_PR.astype('int').astype('str')
        temp2 = df[df.STRIKE_PR.astype('int') != df.STRIKE_PR].copy()
        temp2.STRIKE_PR = temp2.STRIKE_PR.astype('str')
        df = pd.concat([temp1, temp2], axis = 0)
        new_col = df.SYMBOL + df.EXPIRY_DT + df.OPTION_TYP + df.STRIKE_PR.astype('str')
        df.insert(loc=0, column='Ticker', value=new_col)
        return df

    def copy_from_stringio(df):
        temp_date = df['TIMESTAMP'][0]
        df.to_csv(os.path.join(output_path,rf"nse_fno_{temp_date}.csv"),index=False)
        print(f"Saved {exchange} FnO Bhavcopy for : {temp_date}")
    
    folder = os.path.join(output_path,fr"{datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y')}")
    for files in os.listdir(folder):
        if 'fo'.upper() in files:
            df = ReadFile(os.path.join(folder,files),exchange)
            copy_from_stringio(df)

# exchanges = ['BSE','NSE']
exchanges = ['NSE']
for exchange in exchanges:
    print(f"Running for {exchange}")
    startdate = datetime.strptime(start_date, '%Y-%m-%d').date()
    enddate = datetime.strptime(end_date, '%Y-%m-%d').date()
    print(f"Updating for {startdate} to {enddate}")
    # fetch_bhavcopy(exchange,startdate,enddate)
    eq_bhavcopy_upload(exchange)
    opt_bhavcopy_upload(exchange)

