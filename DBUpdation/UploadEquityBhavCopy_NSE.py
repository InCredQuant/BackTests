import os
import re
import sys
from datetime import datetime
import pandas as pd
import psycopg2
#current = os.path.dirname(os.path.realpath(__file__))
#parent = os.path.dirname(current)
#sys.path.append(parent)
from config import config
from sqlalchemy import create_engine
'''
config = {
    'nfunds': 4,
    'fund_ids': [1,2,3,4],
    'fund_names': {1:'Liquid Fund', 2: 'EquityPlus Fund', 3: 'SectorRotation Fund', 4: 'Credit Fund'},
    'db_name' : 'autodash',
    'histdb_name' : 'data',
    'pfdb_name': 'portfolio',
    'username' : 'postgres',
    'password': 'postgres',
    'host': '192.168.44.4',
    'port': '5432',
    'pnl_table_columns' : ['ID', 'DATE', 'SEGMENT', 'SYMBOL', 'STRIKE', 'EXPIRY', 'OPTIONTYPE', 'UNIQUEID', 'STRATEGYID', 'CLOSE', 'PREVCLOSE','TRADEDQTY', 'ACTIVEQTY', 'DAILYPNL'],
    'order_table_columns': ['ID','PKEY', 'DATE', 'TIME', 'EXCHANGE', 'SEGMENT', 'SYMBOL', 'EXPIRY', 'STRIKE', 'OPTIONTYPE', 'ORDERTYPE', 'QUANTITY', 'PRICE', 'STRATEGYID', 'BROKER', 'COMMENT'],
    'exposure_table_columns': ['ID', 'DATE', 'STRATEGYID', 'DELTA', 'pDELTA', 'GROSSEXPOSURE', 'NETEXPOSURE', 'DAILYPNL'],
    'greek_table_columns': ['ID', 'TICKER', 'DATE', 'IV', 'DELTA', 'RHO', 'THETA', 'VEGA', 'GAMMA'],
    'nse_fno_table_columns': ['Ticker', 'INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP'],
    'pnl_tablename': 'DAILYPORTFOLIO',
    'order_tablename': 'ORDER',
    'hist_eq_tablename': 'nsecash',
    'hist_fno_tablename': 'nsefno',
    'db_uri': "postgresql+psycopg2://postgres:postgres@192.168.44.4:5432/autodash",
    'histdb_uri': "postgresql+psycopg2://postgres:postgres@192.168.44.4:5432/data",
    'pfdb_uri': "postgresql+psycopg2://postgres:admin@192.168.44.4:5432/portfolio",
    'get_prices_columns': ['SEGMENT', 'SYMBOL', 'STRIKE', 'EXPIRY', 'OPTIONTYPE'],
    'get_nse_fno_columns' : ['Ticker', 'Instrument', 'Scrip', 'Expiry', 'Strike', 'OptionType', 'Open', 'High', 'Low', 'Close', 'SettlePrice', 'Contracts', 'Val_Lakh', 'OI', 'ChgOI', 'Date'],
    'greek_df_columns': ['Ticker', 'Date', 'IV', 'Delta', 'Rho', 'Theta', 'Vega', 'Gamma'],
}
'''
# Parent folder path
#parent_folder = r'C:/Users/bbg.quant_incredalts/Desktop/Nilesh/PnL-Exposure/Data'
parent_folder = 'G:/Shared drives/BackTests/DB/BhavCopy/NSE/Cash/temp'

#G:\Shared drives\BackTests\DB\Bhavcopy\NSE\Cash\temp
columns = ['SYMBOL','SERIES','OPEN','HIGH','LOW','CLOSE','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN']
engine = create_engine(config['histdb_uri'])
TableName = config['hist_eq_tablename']


# Walk through the parent folder and its subdirectories
def upload_bulk(parent_folder, engine):
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            # Check if the file is a CSV file
            if file.endswith('.zip'):
                path = parent_folder + '/' +  file# + '/' + file
                df = pd.read_csv(path, compression='zip')
                #print(type(df['TIMESTAMP'][0]))
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.date
                #print(type(df['TIMESTAMP'][0]))
                temp_date = df['TIMESTAMP'][0]
                df = df[columns]
                #df.drop(['Unnamed: 13'], axis=1, inplace=True)
                df.reset_index(drop=True,inplace=True)
                #print(df.head())
                try:
                    df.to_sql(TableName, engine, if_exists='append', index=False)
                    print(f'Updated data for : {temp_date}')
                except Exception as e:
                    print(e)

            
def upload(file_path, engine):
    df = pd.read_csv(file_path)
    if df.empty:
        print(f'File not found')
        return None

    df = df[columns]
    date = df['TIMESTAMP'][0]
    df.reset_index(drop=True, inplace=True)
    print(df.head())

    try:
        df.to_sql(TableName, engine, if_exists='append', index=False)
        print(f'Updated data for : {date}')
    except Exception as e:
        print(f'Error: {e}')

upload_bulk(parent_folder=parent_folder, engine=engine)

# upload(r'C:\Users\bbg.quant_incredalts\Desktop\Nilesh\PnL-Exposure\Data\cm28APR2023bhav.csv\cm28APR2023bhav.csv', engine)
# upload(r'C:\Users\bbg.quant_incredalts\Desktop\Nilesh\PnL-Exposure\Data\cm20JUN2023bhav.csv\cm20JUN2023bhav.csv', engine)
# upload(r'C:\Users\bbg.quant_incredalts\Desktop\Nilesh\PnL-Exposure\Data\cm21JUN2023bhav.csv\cm21JUN2023bhav.csv', engine)
# upload(r'C:\Users\bbg.quant_incredalts\Desktop\Nilesh\PnL-Exposure\Data\cm22JUN2023bhav.csv\cm22JUN2023bhav.csv', engine)