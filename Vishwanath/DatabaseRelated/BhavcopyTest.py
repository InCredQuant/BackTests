from datetime import timedelta, date
import pandas as pd
import psycopg2 as pg
from io import StringIO
from sqlalchemy import create_engine
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

def eq_bhavcopy_upload():
    
    def ReadFile(zipfilename):
        columns = ['SYMBOL','SERIES','OPEN','HIGH','LOW','CLOSE','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN']
        df = pd.read_csv(zipfilename, compression='zip')
        column_mapping = {'TckrSymb': 'SYMBOL','SctySrs': 'SERIES','OpnPric': 'OPEN','HghPric': 'HIGH','LwPric': 'LOW','ClsPric': 'CLOSE',
        'LastPric': 'LAST','PrvsClsgPric': 'PREVCLOSE','TtlTradgVol': 'TOTTRDQTY','TtlTrfVal': 'TOTTRDVAL','TradDt': 'TIMESTAMP','TtlNbOfTxsExctd': 'TOTALTRADES',
        'ISIN': 'ISIN'}
        df = df.rename(columns=column_mapping)
        df = df.loc[:,columns]
        df['SERIES'] = np.where(df['SERIES'].isna(),'NA',df['SERIES'])
        return df
    
    folder = rf"C:\Vishwanath\FundLevelData\Bhavcopy"
    for files in os.listdir(folder):
        if 'cm'.upper() in files:
            df = ReadFile(os.path.join(folder,files))
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.date
            temp_date = df['TIMESTAMP'][0]
            df.reset_index(drop=True,inplace=True)
            try:
                df.to_csv(rf"C:\Vishwanath\Files\\{files}")
            except Exception as e:
                print(e)

def opt_bhavcopy_upload():
    def ReadFile(zipfilename,exchange="NSE"):
        cols = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN','HIGH', 'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH','OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']
        df = pd.read_csv(zipfilename, compression='zip')
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

    def copy_from_stringio(files,df):
        df.to_csv(rf"C:\Vishwanath\Files\\{files}",index=False)
    
    folder = rf"C:\Vishwanath\FundLevelData\Bhavcopy"
    # folder = rf"C:\Vishwanath\Files\2025"
    for files in os.listdir(folder):
        if 'fo'.upper() in files:
            df = ReadFile(os.path.join(folder,files))
            print(df)
            copy_from_stringio(files,df)

opt_bhavcopy_upload()
eq_bhavcopy_upload()