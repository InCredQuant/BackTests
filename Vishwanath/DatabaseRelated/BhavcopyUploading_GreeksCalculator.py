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
import sys
sys.path.insert(0, 'G:\\Shared drives\\BackTests\\pycode\\DBUpdation\\')
from config import config
from utils import calculate_greeks
from get_bhavcopy_data_utils import getIndicesSpotData, GetConn
import pdb
import pg_redirect

user = 'postgres'
password = 'postgres'
host = '192.168.44.4'
port = '5432'
dbname = 'data'
schema = 'public'

def date_list(table):
    conn = pg.connect(host=host,dbname=dbname,user=user,password=password,port=port)
    cursor = conn.cursor()
    cursor.execute(f''' SELECT MAX("TIMESTAMP") FROM {table}''')
    max_date = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    today_date = date.today()
    if max_date is None:
        print("No records found in the table.")
    else:
        max_date = pd.to_datetime(max_date).date()
        date_list = pd.date_range(start=max_date + timedelta(days=1), end=today_date - timedelta(days=1)).date
    
    return date_list

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

def eq_bhavcopy_upload(exchange):
    
    def ReadFile(zipfilename,table):
        columns = ['SYMBOL','SERIES','OPEN','HIGH','LOW','CLOSE','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN']
        df = pd.read_csv(zipfilename, compression='zip') if table == 'nsecash' else pd.read_csv(zipfilename)
        column_mapping = {'TckrSymb': 'SYMBOL','SctySrs': 'SERIES','OpnPric': 'OPEN','HghPric': 'HIGH','LwPric': 'LOW','ClsPric': 'CLOSE',
        'LastPric': 'LAST','PrvsClsgPric': 'PREVCLOSE','TtlTradgVol': 'TOTTRDQTY','TtlTrfVal': 'TOTTRDVAL','TradDt': 'TIMESTAMP','TtlNbOfTxsExctd': 'TOTALTRADES',
        'ISIN': 'ISIN'}
        df = df.rename(columns=column_mapping)
        df = df.loc[:,columns]
        df['SERIES'] = np.where(df['SERIES'].isna(),'NA',df['SERIES'])
        return df

    exchange_to_table = {'NSE': 'nsecash', 'BSE': 'bsecash'}
    path = rf"G:\Shared drives\BackTests\Vishwanath\Bhavcopy\\"
    folder = os.path.join(path,str(start_date.year))
    TableName = exchange_to_table.get(exchange, '')
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:5432/data')
    # connection = engine.raw_connection()
    for files in os.listdir(folder):
        if 'cm'.upper() in files:
            df = ReadFile(os.path.join(folder,files),TableName)
            # display(df)
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.date
            temp_date = df['TIMESTAMP'][0]
            df.reset_index(drop=True,inplace=True)
            try:
                df.to_sql(TableName, engine, if_exists='append', index=False)
                print(f'Updated {exchange} EQ Bhavcopy for : {temp_date}')
            except Exception as e:
                print(e)
            
def opt_bhavcopy_upload(exchange,TableName):
    def ReadFile(zipfilename,table,exchange):
        cols = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN','HIGH', 'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH','OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']
        df = pd.read_csv(zipfilename, compression='zip') if table == 'nsefno' else pd.read_csv(zipfilename)
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

    def copy_from_stringio(conn, df, table):
        buffer = StringIO()
        temp_date = df['TIMESTAMP'][0]
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep=",")
            conn.commit()
        except (Exception, pg.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print(f"Updated {exchange} FnO Bhavcopy for : {temp_date}")
        cursor.close()
    
    conn = pg.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    path = rf"G:\Shared drives\BackTests\Vishwanath\Bhavcopy\\"
    folder = os.path.join(path,str(start_date.year))
    print(TableName)
    for files in os.listdir(folder):
        if 'fo'.upper() in files:
            df = ReadFile(os.path.join(folder,files),TableName,exchange)
            # display(df)
            copy_from_stringio(conn = conn, df = df, table = TableName)
        
class Greeks:
    def __init__(self,exchange=None):
        self.exchange = exchange
        self.Tablename = 'greeks_bsefno' if exchange == 'bse' else 'greeks_nsefno'
        self.engine = create_engine(config['histdb_uri'])
        self.conn = pg.connect(dbname=config['histdb_name'], user=config['username'], password='postgres', host=config['host'], port='5432')
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
    
    def create_table(self):
        sql = '''
            CREATE TABLE IF NOT EXISTS {}(
            "Ticker" VARCHAR(128) NOT NULL, "Date" DATE NOT NULL, "IV" FLOAT, 
            "Delta" FLOAT, "Rho" FLOAT,"Theta" FLOAT, "Vega" FLOAT, "Gamma" Float);
        '''.format(self.Tablename)
        try:
            # print(sql)
            # self.cur.execute(sql)
            # self.conn.commit()
            print(f'{self.Tablename}: Table created sucessfully!')
        except Exception as e:
            print(f'Error: {e}')
            # self.conn.rollback()

    def read_oneday_bhavcopy(self,table,date:None):
        basesql = f''' SELECT * FROM public.{table} WHERE "TIMESTAMP" = '{date}'; '''
        try:
            # print(f'Starting data search query.....')
            self.cur.execute(basesql)
            res = self.cur.fetchall()
            df = pd.DataFrame(res, columns=config['get_nse_fno_columns'])
            print(f'Fetched {exchange.upper()}FNO BhavCopy For date: {date}')
            return df
        except Exception as e:
            print(f'Error: {e}')

    def upload_greeks_bulk(self,exchange,date=None):
       
        spf = getIndicesSpotData()
        tablename = 'nsefno' if exchange == 'nse' else 'bsefno'
        if date != None:
            alldates = self.get_all_dates_fno(tablename,date=date)
        else:
            alldates = self.get_all_dates_fno(tablename)
        print(f'Starting Greeks Calculation....')
        startindx = 0 if date != None else 1
        for date in alldates[startindx:]:
            df = self.read_oneday_bhavcopy(tablename,date[0])
            df['Close'] = np.where((df['Close'] == 0) & (exchange == "bse"), df['SettlePrice'], df['Close'])
            greeks = calculate_greeks(df, spf)
            greek_ = greeks.loc[greeks['Instrument'].isin(['OPTSTK', 'OPTIDX']), config['greek_df_columns']]
            greek_ = greek_.dropna()
            buffer = StringIO()
            greek_.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor = self.conn.cursor()
            try:
                cursor.copy_from(buffer, self.Tablename, sep=",")
                self.conn.commit()
            except (Exception, pg.DatabaseError) as error:
                print("Error: %s" % error)
                self.conn.rollback()
                cursor.close()
                return 1
            print("Saved on day greeks DataFrame")
            cursor.close()
        
    def get_all_dates_fno(self,table,date=None):
        if date != None:
            basesql = f''' SELECT DISTINCT \"TIMESTAMP\" FROM public.{table} WHERE "TIMESTAMP" >= '{date}' ORDER BY \"TIMESTAMP\" DESC; '''
        else:     
            basesql = f''' SELECT DISTINCT \"TIMESTAMP\" FROM public.{table} ORDER BY \"TIMESTAMP\" DESC; '''
        try:
            print(f'Starting date search query.....')
            self.cur.execute(basesql)
            res = self.cur.fetchall()
            dates = list(res)
            print(f'Fetched total: {len(dates)} Dates')
            return dates
        except Exception as e:
            print(f'Error: {e}')
     
# exchanges = ['BSE','NSE']
exchanges = ['NSE']
exchange_to_table = {'NSE': 'nsefno', 'BSE': 'bsefno'}
for exchange in exchanges:
    TableName = exchange_to_table.get(exchange, '')
    dates = date_list(TableName)
    # print(dates)
    print(f"Running for {exchange}")
    if len(dates) == 0:
        print("Updated till latest date!!")
    
    else:
        start_date = sorted(dates)[0]
        end_date = sorted(dates)[-1]
        print(f"Updating for {start_date} to {end_date}")
        opt_bhavcopy_upload(exchange,TableName)
        eq_bhavcopy_upload(exchange)
        g = Greeks(exchange=exchange.lower())
        g.upload_greeks_bulk(exchange.lower(),start_date)