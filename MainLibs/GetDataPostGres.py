'''
31Oct2023
@Author: Started by Ankur@InCred
@Author2: Improved by  Viren@InCred

Similar to old GetData File, it is expected to be New GetData File, which will connect to the Postgres for fetching the data
DataBases and Tables are as mentioned below:
'autodash':  dailyportfolio, f1_rder, f1_activeorder, f1_portfolio
'data': gdfl_min, greeks_nsefno, nsecash, nseexpiry, nsefno, stocks

 engine_url = f'postgresql+psycopg2://{cf.user_name}:{cf.pwd}@{cf.host}:{cf.port}/{cf.db_name}'
 db_obj = DataBaseConnect(engine_url)
 '''
import pandas as pd
import config as cf
from sqlalchemy import create_engine

import pandas as pd
#import config as cf
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pdb
# postgres version

class DataBaseConnect:
    def __init__(self, user = 'postgres', password = 'postgres', host = '10.147.0.69', port = '5432', database = 'data'):
        engine_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        self.db_conn = create_engine(engine_url, connect_args={"connect_timeout": 10})
    
    def CommaSeparatedList(MyList, appendapost = 1):
        if all([i.isnumeric() for i in MyList]):#
            appendapost = 0
            
        if appendapost == 1:
            temp = ",".join(["'" + i + "'" for i in MyList])
        else:
            temp = ','.join([str(i) for i in MyList])    
        temp = "(" + temp + ")"
        return temp
    
    def Connect(self):
        '''
        Trying to Connect the Database
        '''
        try:
            self.db_conn.connect()
        except OperationalError as e:
            print(f"Failed to connect to the database: {e}")
            return False
        return True
            
    def getSingleTickerMinData(self, ticker):
        query = """select * from gdfl_min where "Ticker"='%s';"""%(ticker)
        df = pd.read_sql(query, self.db_conn)
        return df

    def getCurrentDateMinData(self, inDate, ticker):
        query = """select * from gdfl_min where "Ticker" = '%s' and "Date"=date('%s');"""%(ticker, inDate)
        df = pd.read_sql(query, self.db_conn)
        return df
    
    def getOneTimeMinData(self, inTime, ticker):
        query = """select * from gdfl_min where "Time"='%s' and "Ticker"='%s';"""%(inTime, ticker)
        df = pd.read_sql(query, self.db_conn)
        return df
    
    def getCurrentDateTimeMinData(self, inDate, inTime, ticker):
        query = """select * from gdfl_min where "Date"=date('%s') and "Time"='%s' and "Ticker"='%s';"""%(inDate, inTime, ticker)
        df = pd.read_sql(query, self.db_conn)
        return df

    def getMultiTickersDateMinData(self, inDate, tickers):
        ticker_str = self.CommaSeparatedList(tickers)        
        #ticker_str = "("+",".join(["'"+i+"'" for i in tickers])+")"
        query = f'select * from gdfl_min where "Ticker" in ' + ticker_str + ' and "Date" = ' +"'"+inDate.strftime('%Y-%m-%d')+"';"
        df = pd.read_sql(query, self.db_conn)
        return df
    
    def getIndexSpotMinData(self, indexName, fromDate, toDate, fieldName = 'Close'):
        ticker_str = "("+",".join(["'"+i+"'" for i in indexName])+")"
        #query = f'select "Symbol", "Date", "Time", \"{fieldName}\" from spotdata where "Symbol" = \'{indexName}\' and "Date" >= date(\'{fromDate}\')  and "Date" <= date(\'{toDate}\');'
        query = f'select "Symbol", "Date", "Time", \"{fieldName}\" from spotdata where "Symbol" in {ticker_str} and "Date" >= date(\'{fromDate}\')  and "Date" <= date(\'{toDate}\');'
        df = pd.read_sql(query, self.db_conn)
        
        df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Time'])
        df.index = df['DateTime']
        gg = df.groupby('Symbol')        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper()}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF
        
    def getMultiTickersMultiTimeMinData(self, fromDate, toDate, inTimeList, tickers, fieldName):
        ticker_str = "("+",".join(["'"+i+"'" for i in tickers])+")"#self.CommaSeparatedList(tickers)
        time_str = "("+",".join(["'"+i+"'" for i in inTimeList])+")"#self.CommaSeparatedList(inTimeList)
        query = f'select "Date", "Time", "Ticker", \"{fieldName}\" from gdfl_min where "Ticker" in {ticker_str} and "Date" >= date(\'{fromDate}\')  and "Date" <= date(\'{toDate}\') and "Time" in {time_str};'
        df = pd.read_sql(query, self.db_conn)
        df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Time'])
        df.index = df['DateTime']
        gg = df.groupby('Ticker')        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper().replace('.NFO', '')}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF
    
    def get_px(self, symbol, input_date, input_time, ticker, px='close'):
        df = self.getCurrentDateTimeMinData(input_date, input_time, ticker)
        if not df.empty:
            if px == 'close':
                return df['Close'].values[0]
            elif px == 'high':
                return df['High'].values[0]
            else:
                return None
    def getIntraDaySeries(self, tickers: list, fromDate: str, toDate: str, fieldName = 'Close'):
        """
        Parameters
        ----------
        tickers : TYPE
            Provide the list of Tickers of the Fut/Option with ".NFO" suffix, for index Fut Data suffix with "-I|II|III.NFO".
        fromDate : TYPE
            starting Date, as String in "YYYY-MM-DD" format.
        toDate : TYPE
            Till Date, as String in "YYYY-MM-DD" format.
        px : TYPE, optional
            Which Field to Fetch. The default is 'Close', Options are Open, High, Low, CLose, Volume, Open Interest, StrikePrice, Call_Or_Put.

        Returns
        -------
        DataFrame: with Tickers in Column Name and Values in the Data Frame Values
        """
        ticker_str = self.CommaSeparatedList(tickers)
        #ticker_str = "("+",".join(["'"+i+"'" for i in tickers])+")"
        query = f'select "Date", "Time", "Ticker", "' + fieldName + '" from gdfl_min where "Ticker" in ' + ticker_str + ' and "Date" >= ' +"'"  + fromDate +"'" + ' and "Date" <= ' +"'"+toDate+"';"
        df = pd.read_sql(query, self.db_conn)
        df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Time'])
        df.index = df['DateTime']
        gg = df.groupby('Ticker')        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper().replace('.NFO', '')}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF
    
    def getExpiryDates(self, expiryType = 'w', instrument = 'OPTIDX', symbol = 'NIFTY'):
        '''
        Parameters
        ----------
        expiryType : TYPE, optional
            DESCRIPTION. The default is 'w': Weekly 
            Options: 'm': Monthly.
        instrument : TYPE, optional
            DESCRIPTION. The default is 'optidx': for Option Index.
            Options: 'OPTIDX', 'FUTSTK', 'FUTIDX'
        symbol : TYPE, optional
            DESCRIPTION. The default is ''.
            If '': BLANK, then it will give for all instruments

        Returns
        -------
        List of Expiry Dates.
        '''
        if symbol == '':
            query = f'select "DATE", "EXPIRY" from public.nseexpiry where "INSTRUMENT" = ' +"'"+ instrument + "'"+ ' and '
        else:
            query = f'select "DATE", "EXPIRY" from public.nseexpiry where "SYMBOL" = ' +"'"+ symbol + "'"+ ' and '
        
        if expiryType.lower()[0] == 'w':
            query = query + '"WEEKLY" = \'1\';'
        elif expiryType.lower()[0] == 'm':
            query = query + '"MONTHLY" = \'1\';'            
        df = pd.read_sql(query, self.db_conn)
        dates = list(set(df.DATE))
        dates.sort()
        return dates
        
    def getDatabyExpiry(self, lowerStrike, upperStrike, week = '', expiryDate = '28JAN24', name = 'NIFTY', fieldName = 'Close'):
        '''
        Parameters
        ----------
        expiryDate : TYPE, optional
            DESCRIPTION. The default is '28JAN24'.
        name : TYPE, optional
            DESCRIPTION. The default is 'NIFTY'.
        lowerStrike : Lowest Strike Price
            DESCRIPTION.
        upperStrike : Maximum Strike Price
            DESCRIPTION.

        Returns
        -------
        None.

        '''
        postappend = ';'
        if week != '':
            fromTable = 'weekly_gdfl_min_opt'
            postappend = f' and "Label" = \'{week.upper()}\';'
        else:
            fromTable = 'gdfl_min'
        query = f'select "Date", "Time", "Ticker", \"{fieldName }\" from {fromTable} where "Name" = \'{name}\' and "ExpiryDate" = \'{expiryDate}\''
        if 'lowerStrike' in locals():
            query = query + ' and "StrikePrice" >= '+ "'" + str(lowerStrike) + "'"
        if 'upperStrike' in locals():
            query = query + ' and "StrikePrice" <= '+ "'" + str(upperStrike) + "'"
        
        query = query+ postappend
        
        df = pd.read_sql(query, self.db_conn)
        df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Time'])
        df.index = df['DateTime']
        gg = df.groupby('Ticker')        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper().replace('.NFO', '')}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF
     

    def getNseBhavCopyDataSymbols(self, symbols: list, fromDate: str, toDate: str, expiryDate: str, fieldName:str = 'CLOSE'):
         """
         Parameters
         ----------
         symbols : 
         fromDate : TYPE
             starting Date, as String in "YYYY-MM-DD" format.
         toDate : TYPE
             Till Date, as String in "YYYY-MM-DD" format.
         px : TYPE, optional
             Which Field to Fetch. The default is 'Close', Options are Open, High, Low, CLose, Volume, Open Interest, StrikePrice, Call_Or_Put.

         Returns
         -------
         DataFrame: with Tickers in Column Name and Values in the Data Frame Values
         """
         #symbols_str = self.CommaSeparatedList(symbols)
         symbols_str = "("+",".join(["'"+i+"'" for i in symbols])+")"
         query = f'select "Ticker", "TIMESTAMP" as "Date", \"{fieldName.upper()}\" from nsefno where "SYMBOL" in {symbols_str} and "TIMESTAMP" >= \'{fromDate}\' and "TIMESTAMP" <= \'{toDate}\' and "EXPIRY_DT" = \'{expiryDate}\';'
         #query = f'select "Ticker", "TIMESTAMP" as "Date", "' + fieldName.upper() + '" from nsefno where "SYMBOL" in ' + symbols_str + ' and "TIMESTAMP" >= ' +"'"  + fromDate +"'" + ' and "TIMESTAMP" <= ' +"'"+toDate+"';"
         df = pd.read_sql(query, self.db_conn)
         #df['Date'] = pd.to_datetime(df['TIMESTAMP']) + pd.to_timedelta(df['Time'])
         df.index = df['Date']
         gg = df.groupby('Ticker')
         del df
         temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName.upper()]).rename(columns={fieldName.upper(): grp.upper().replace('.NFO', '')}) for grp in gg.groups.keys()]
         finalDF = pd.concat(temp_dfs, axis=1)
         finalDF = finalDF.sort_index()
         return finalDF
     
def main():
    #engine_url = f'postgresql+psycopg2://{cf.user_name}:{cf.pwd}@{cf.host}:{cf.port}/{cf.db_name}'
    engine_url = f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}'
    db_obj = DataBaseConnect(engine_url)
    # SELECT * FROM NIFTY WHERE "DATE" = "2022-11-29" AND "TIME"="12:49:59" AND "TICKER" = "NIFTY01DEC2218600PE.NFO";
    # df = db_obj.get_data('NIFTY','2022-11-29','12:49:59','NIFTY01DEC2218600PE.NFO')
    tickers = ['NIFTY01DEC2218600PE.NFO','NIFTY01DEC2218600CE.NFO']
    df = db_obj.get_data_tickers('2022-11-29', tickers)
    print(df.head())



if __name__ == '__main__':
    main()