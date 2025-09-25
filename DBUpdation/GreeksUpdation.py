import datetime
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO
import sys
sys.path.insert(0, 'G:\\Shared drives\\BackTests\\pycode\\DBUpdation\\')

from config import config
from utils import calculate_greeks
from get_bhavcopy_data_utils import getIndicesSpotData, GetConn
import pdb
#import warnings


class Greeks:
    def __init__(self):
        self.TableName = 'greeks_nsefno'#'GREEKS_NSEFNO'
        self.engine = create_engine(config['histdb_uri'])
        self.conn = psycopg2.connect(dbname=config['histdb_name'], user=config['username'], password='postgres', host=config['host'], port='5432')
        self.conn.autocommit = False

        self.cur = self.conn.cursor()

    def get_conn(self):
        """
        If connection closed or connection not started Start new connection else return connection object
        returns:
        --------
            conn: 
                psycopg2 connection obejct 
        """
        if self.conn is None:
            self.conn = psycopg2.connect(dbname=config['histdb_name'], user=config['username'], password='postgres', host=config['host'], port='5432')
            self.conn.autocommit = False
            self.cur = self.conn.cursor()
            return self.conn
        else:
            print('Connection already established')
            return self.conn
    

    def close_conn(self):
        """
        Close connection if not closed
        """        
        if self.conn != None:
            self.conn.commit()
            self.conn.close()
            print('Connection closed!')

    def create_table(self):
        '''
        Creates a New table if not exists
        Parameters:
        -----------
        '''

        #id SERIAL PRIMARY KEY,
        sql = '''
            CREATE TABLE IF NOT EXISTS {}(
            "DATE" DATE NOT NULL,
            "TICKER" VARCHAR(128) NOT NULL,
            "IV" FLOAT,
            "DELTA" FLOAT,
            "THETA" FLOAT,
            "RHO" FLOAT,
            "GAMMA" FLOAT,
            "VEGA" FLOAT
            );
        '''.format(self.TableName)
        try:
            #print(sql)
            self.cur.execute(sql)
            print(f'{self.TableName}: Table created sucessfully!')
        except Exception as e:
            print(f'Error: {e}')

    def update_greeks(self, df):
        '''
        Parameters:
        -----------
        df =  pandas DataFrame of today's greeks
        '''        
        try:
            df.to_sql(self.TableName, self.engine, if_exists='append', index=False)
            print(f'Updated todays greek!')
        except Exception as e:
            print(f'Error: {e}')

    def get_all_dates_nsefno(self, date=None):
        """
        Parameters:
        -----------
        date: None or datetime.date() (date before which all distict dates are required)

        Returns:
        --------
        dates: List (distinct dates from NSEFNO)
        """
        if date != None:
            basesql = ''' SELECT DISTINCT \"TIMESTAMP\" FROM public.nsefno WHERE "TIMESTAMP" >= '{}' ORDER BY \"TIMESTAMP\" DESC; '''.format(date)
        else:     
            basesql = ''' SELECT DISTINCT \"TIMESTAMP\" FROM public.nsefno ORDER BY \"TIMESTAMP\" DESC; '''
        try:
            print(f'Starting date search query.....')
            self.cur.execute(basesql)
            res = self.cur.fetchall()
            dates = list(res)
            print(f'Fetched total: {len(dates)} Dates')
            return dates
        except Exception as e:
            print(f'Error: {e}')

    def read_oneday_bhavcopy(self, date: datetime.date):
        """
        READ ONE DAY FNO BHAVCOPY TO CALCULATE GREEKS

        Parameters:
        -----------
        date: datetime.date (date to search for in database)

        Returns:
        --------
        df: pandas Dataframe of one day BhavCopy of FnO
        """
        basesql = ''' SELECT * FROM public.nsefno WHERE \"TIMESTAMP\" = '{}'; '''.format(date)
        try:
            print(f'Starting data search query.....')
            self.cur.execute(basesql)
            res = self.cur.fetchall()
            df = pd.DataFrame(res, columns=config['get_nse_fno_columns'])
            print(f'Fetched NSEFNO BhavCopy For date: {date}')
            return df
        except Exception as e:
            print(f'Error: {e}')



    def upload_greeks_bulk(self, date=None):
        """
        POPULATE GREEKS TABLE FROM BEGINNING IN CASE OF DATA LOSE
        FIRST CALL UPLOAD_GREEKS FOR LATEST DATE THEN USE THIS FUNCTION TO POPULATE DB
        
        Parameters:
        -----------
        date: datetime.date (latest date to bulk upload from)
        Returns:
        --------
        on ERROR: EXCEPTION MESSAGE
        """

        spf = getIndicesSpotData()
        #print(spf.head())
        
        if date != None:
            alldates = self.get_all_dates_nsefno(date=date)
        else:
            alldates = self.get_all_dates_nsefno()

        print(f'Starting Greeks Calculation....')
        startindx = 0 if date != None else 1
        for date in alldates[startindx:]:
            df = self.read_oneday_bhavcopy((date[0]))
            greeks = calculate_greeks(df, spf)
            greek_ = greeks.loc[greeks['Instrument'].isin(['OPTSTK', 'OPTIDX']), config['greek_df_columns']]
            greek_ = greek_.dropna()
            buffer = StringIO()

            greek_.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
    
            cursor = self.conn.cursor()
            try:
                cursor.copy_from(buffer, self.TableName, sep=",")
                self.conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                self.conn.rollback()
                cursor.close()
                return 1
            print("Saved on day greeks DataFrame")
            cursor.close()

    def upload_greeks(self, date):
        """
        UPLOAD GREEKS FOR ONE DAY

        Parameters:
        -----------
        date: datetime.date() (date for which greeks has to be calculated)

        Returns:
        --------
        on ERROR: EXCEPTION MESSAGE
        """
        spf = getIndicesSpotData(ToDate=date)
        print(spf.head())
        print(f'Starting Greeks upload for: {date}')
        df = self.read_oneday_bhavcopy(date)
        greeks = calculate_greeks(df, spf)
        greek_ = greeks.loc[greeks['Instrument'].isin(['OPTSTK', 'OPTIDX']), config['greek_df_columns']]
        self.update_greeks(greek_)

    def greeks(self, date):
        """
        GET GREEKS FOR DATE

        Parameters:
        date: datetime.date (date for which greeks is to be fetched)

        Returns:
        --------
        df: Pandas DataFrame of greeks with ticker
        """
        basesql = ''' SELECT * FROM public.\"GREEKS\" WHERE \"Date\" = '{}'; '''.format(date)
        try:
            print(f'Starting greeks search query.....')
            self.cur.execute(basesql)
            res = self.cur.fetchall()
            df = pd.DataFrame(res, columns=config['greek_df_columns'])
            print(f'Fetched GREEKS For date: {date}')
            return df
        except Exception as e:
            print(f'Error: {e}')

g = Greeks()
g.create_table()
g.upload_greeks_bulk(datetime.date(2024,5,31))