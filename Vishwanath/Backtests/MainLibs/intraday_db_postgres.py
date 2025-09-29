#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 23-01-2023 09:26
# @Author : Ankur

import pandas as pd
import datetime as dt
#import config as cf
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import pdb
import re
# postgres version

class DataBaseConnect:
    def __init__(self, engine_url):
        self.db_conn = create_engine(engine_url, connect_args={"connect_timeout": 7})
    
    def connect(self):        
        try:
            self.db_conn.connect()
        except OperationalError as e:
            print(f"Failed to connect to the database: {e}")
            return False
        return True
    
    def getNSEBhavCopyGreeks(self, tickers: list, fieldName: str):
        ticker_str = "("+",".join(["'"+i.lower()+"'" for i in tickers])+")"
        query = f'select "Ticker", "Date",  \"{fieldName}\" from greeks_nsefno where lower("Ticker") in {ticker_str};'
        df = pd.read_sql(query, self.db_conn)
        df.Date = pd.to_datetime(df.Date)    
        df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
        gg = df.groupby('Ticker')        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper()}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF
        
    def get_ticker_data(self, ticker):
        query = """
        select * from public.gdfl_min where "Ticker"='%s'
        """%(ticker)
        df = pd.read_sql(query, self.db_conn)
        return df

    def get_data_current(self, symbol, input_date, ce_ticker, pe_ticker):
        query = """
        select * from public.gdfl_min where "Ticker" in ('%s','%s') and "Date"=date('%s')  
        """%(ce_ticker, pe_ticker, input_date)
        df = pd.read_sql(query, self.db_conn)
        return df

    def get_data(self, symbol, input_date, input_time, ticker):
        query = """
        select * from public.gdfl_min where "Date"=date('%s') and "Time"='%s' and "Ticker"='%s' 
        """%(input_date, input_time, ticker)
        df = pd.read_sql(query, self.db_conn)
        return df

    def get_data_tickers(self, input_date, tickers):
        ticker_str = "("+",".join(["'"+i+"'" for i in tickers])+")"
        query = f'select * from public.gdfl_min where "Ticker" in ' + ticker_str + ' and "Date" = ' +"'"+input_date+"'"
        df = pd.read_sql(query, self.db_conn)
        return df


    def get_px(self, symbol, input_date, input_time, ticker, px='close'):
        df = self.get_data(symbol, input_date, input_time, ticker)
        if not df.empty:
            if px == 'close':
                return df['Close'].values[0]
            elif px == 'high':
                return df['High'].values[0]
            else:
                return None
    def get_IntraDaySeries(self, tickers: list, fromDate: str, toDate: str, fieldName = 'Close', spot = False):
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
        if spot:
            pattern = r'-(III|II|I)\.NFO$'
            tickers = list(set([re.sub(pattern, '', it) for it in tickers]))
        ticker_str = "("+",".join(["'"+i+"'" for i in tickers])+")"
        
        query = f'select "Date", "Time", "Ticker", "' + fieldName + '" from gdfl_min where "Ticker" in ' + ticker_str + ' and "Date" >= ' +"'"  + fromDate +"'" + ' and "Date" <= ' +"'"+toDate+"'"
        if spot:
            query = f'select "Date", "Time", "Symbol" as "Ticker", "' + fieldName + '" from spotdata where "Symbol" in ' + ticker_str + ' and "Date" >= ' +"'"  + fromDate +"'" + ' and "Date" <= ' +"'"+toDate+"'"
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
        
    def getDatabyExpiry(self, lowerStrike, upperStrike, expiryDate = '28JAN24', name = 'NIFTY', fieldName = 'Close'):
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
        query = f'select "Date", "Time", "Ticker", "' + fieldName + '" from public.gdfl_min where "Name" = ' + "'"+ name + "'"+' and "ExpiryDate" = ' +"'"  + expiryDate +"'"
        if 'lowerStrike' in locals():
            query = query + ' and "StrikePrice" >= '+ "'" + lowerStrike + "'"
        if 'upperStrike' in locals():
            query = query + ' and "StrikePrice" <= '+ "'" + upperStrike + "'"
            
        df = pd.read_sql(query, self.db_conn)
        df['DateTime'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Time'])
        df.index = df['DateTime']
        gg = df.groupby('Ticker')        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper().replace('.NFO', '')}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF
    


    def GetNSEBhavCopyFutsData(self, secNames: list, fieldName: str, expiry: str, fromDate: dt.date, toDate: dt.date):    
        # secNames = ['SBILIFE', 'STER']
        # fieldName = 'Close'
        # expiry = '31DEC09'
        # fromDate = datetime.date(2009, 11, 27)
        # toDate = datetime.date(2009, 12, 31)    
        #conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
        
        fieldName = fieldName.upper()
        tickers = [(i+expiry.upper()+'XX0').lower() for i in secNames]
        ticker_str = "("+",".join(["'"+i+"'" for i in tickers])+")"
        
        
        query = f'select "SYMBOL" as "Name", "TIMESTAMP" as "Date", \"{fieldName}\" from nsefno where lower("Ticker") in {ticker_str} and "EXPIRY_DT" = \'{expiry}\' and "TIMESTAMP"  >= \'{fromDate}\' and "TIMESTAMP" <= \'{toDate}\';'
        df = pd.read_sql(query, self.db_conn)
        df.Date = pd.to_datetime(df.Date)    
        df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
        gg = df.groupby('Name')        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper()}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF

    def GetNSEBhavCopyFutsDatabyTicker(self, tickers : list, fieldName: str):    
        # secNames = ['SBILIFE', 'STER']
        # tickers = ['CNXIT25MAR04XX0', 'NIFTY29JAN04XX0']
        # fieldName = 'Close'
        # expiry = '31DEC09'
        # fromDate = datetime.date(2009, 11, 27)
        # toDate = datetime.date(2009, 12, 31)    
        # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
        fieldName = fieldName.upper()
        #tickers = [(i+expiry.upper()+'XX0').lower() for i in secNames]
        ticker_str = "("+",".join(["'"+i.lower()+"'" for i in tickers])+")"        
        query = f'select "Ticker", "TIMESTAMP" as "Date", \"{fieldName}\" from nsefno where lower("Ticker") in {ticker_str};'
        df = pd.read_sql(query, self.db_conn)
        df.Date = pd.to_datetime(df.Date)    
        df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
        gg = df.groupby('Ticker')
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper()}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF
        

    def GetNSEBhavCopyOptsData(self, secName: str, fieldName: str, strikes: list, expiry: str, call_put: list, fromDate: dt.date, toDate: dt.date):
        # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
        # secNames = 'CNXIT'
        # fieldName = 'Close'
        # strikes = [20800, 20900, 21000]
        # expiry = '26FEB04'
        # call_put = ['CE', 'PE']
        # fromDate = datetime.date(2004, 1, 1)
        # toDate = datetime.date(2004, 2, 26)    
        
        fieldName = fieldName.upper()
        tickers = [(secName.upper() + expiry + cType + str(iR)).lower() for iR in strikes for cType in call_put]
        ticker_str = "("+",".join(["'"+i.lower()+"'" for i in tickers])+")"
        
        query = f'select "Ticker", "TIMESTAMP" as "Date", \"{fieldName}\" from nsefno where lower("Ticker") in {ticker_str} and "TIMESTAMP" >= \'{fromDate}\' and "TIMESTAMP" <= \'{toDate}\';'
        df = pd.read_sql(query, self.db_conn)
        df.Date = pd.to_datetime(df.Date)
        
        df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
        gg = df.groupby('Ticker')
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName]).rename(columns={fieldName: grp.upper()}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF

    def GetNSEBhavCopyStrikePointsDiff(self, secNames: list, expiry: str, getStrikes = False):
        #conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
        #secNames = ['CNXIT', 'NTPC', 'ONGC', 'PNB', 'POWERGRID', 'RANBAXY', 'ICICIBANK']
        #expiry = '31AUG17'
        
        secNames_Str = "("+",".join(["'"+i.lower()+"'" for i in secNames])+")"
        query = f'select "SYMBOL" as "Name", "STRIKE_PR" as "Strike" from nsefno where lower("SYMBOL") in {secNames_Str} and "INSTRUMENT" in (\'OPTIDX\', \'OPTSTK\') and "EXPIRY_DT" = \'{expiry}\';'
        df = pd.read_sql(query, self.db_conn)
        
        df.Strike = df.Strike.astype('float')
        df.index = df.Name
        del df['Name']    
        gg = df.groupby('Name')
        dtemp = []
        ok = [dtemp.append((grp, gg.get_group(grp).rename(columns = {'Strike' : grp.upper()}).diff().median().values[0])) for grp in gg.groups.keys()]    
        finalDict = dict(dtemp)
        return df if getStrikes else finalDict
        
    def GetNSEBhavCopyDatabyTicker(self, tickers : list, fieldName: str):
        # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
        # tickers = ['ITC29JAN04CA1050', 'IPCL25MAR04PA210', 'IOC26FEB04PA410', 'INFOSYSTCH26FEB04PA5100', 'ICICIBANK26FEB04PA300', 'I-FLEX26FEB04CA800']
        #fieldName = 'Close'
        # tickers = ['BANKNIFTY25APR19XX0']
        fieldName = fieldName.upper()
        #tickers = [(secName.upper() + expiry + cType + str(iR)).lower() for iR in strikes for cType in call_put]
        ticker_str = "("+",".join(["'"+i.lower()+"'" for i in tickers])+")"
        
        query = f'select "Ticker", "TIMESTAMP" as "Date", \"{fieldName}\" from nsefno where lower("Ticker") in {ticker_str};'
        df = pd.read_sql(query, self.db_conn)
        df.Date = pd.to_datetime(df.Date)
        df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
        del df['Date']
        gg = df.groupby('Ticker')
        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName.upper()]).rename(columns={fieldName.upper(): grp.upper()}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF

    def GetNSEBhavCopyAllTickersDailyData(self, symbols: list, fieldName: str, expiry: str,  fromDate: dt.date, toDate: dt.date, options = True ):
        '''
        Parameters
        ----------
        conn : str
            Connection to DB, Either Local Drive or GDrive.
        symbols : list
            List of the NSE Symbols.
        fieldName : str
            Name of the Required Data Field.
        expiry : str
            Expiry Date in DDMMMYY format.
        fromDate : datetime.date
            Starting Date.
        toDate : datetime.date
            Ending Date.
        options: Binary
            If True then for Options, False then for Futures for the provided list.
        
        Returns
        -------
        It Returns the Required Field Data between two dates, for the provided tickers list and Expiry Date.

        '''
        # conn = GetConn('BhavCopy', gDrive = 'N' if os.path.exists('Z:/LiveDB/') else 'Y')
        # symbols = ['VEDL', 'AXISBANK', 'HCLTECH', 'HINDUNILVR', 'SUNPHARMA', 'WIPRO',
        #         'NTPC', 'TATAMOTORS', 'BHARTIARTL', 'HDFC', 'LT', 'ICICIBANK', 'INFY',
        #         'SBIN', 'HDFCBANK', 'COALINDIA', 'ITC', 'ONGC', 'RELIANCE', 'TCS']
        # fieldName = 'Close'
        # expiry = '26FEB04'
        # fromDate = datetime.date(2004, 1, 1)
        # toDate = datetime.date(2004, 2, 26)
        # options = True
        if options:
            instrument = ['OPTIDX', 'OPTSTK']
        elif not options:
            instrument = ['FUTIDX', 'FUTSTK']

        fieldName = fieldName.upper()
        #tickers = [(secName.upper() + expiry + cType + str(iR)).lower() for iR in strikes for cType in call_put]
        symbols_str = "("+",".join(["'"+i.lower()+"'" for i in symbols])+")"
        instrument_str = "("+",".join(["'"+i.lower()+"'" for i in instrument])+")"
        
        query = f'select "Ticker", "TIMESTAMP" as "Date", \"{fieldName}\" from nsefno where lower("SYMBOL") in {symbols_str} and "EXPIRY_DT" = \'{expiry}\' and "TIMESTAMP" between \'{fromDate}\' and \'{toDate}\' and lower("INSTRUMENT") in {instrument_str};'
        df = pd.read_sql(query, self.db_conn)
        df.Date = pd.to_datetime(df.Date) 
        df.index = df.Date#[datetime.datetime.combine(it[0], datetime.time.fromisoformat(it[1] if len(it[1]) == 8 else '0'+ it[1])) for it in df.loc[:, ['Date', 'Time']].values]    
        del df['Date']
        gg = df.groupby('Ticker')
        
        temp_dfs = [pd.DataFrame(gg.get_group(grp)[fieldName.upper()]).rename(columns={fieldName.upper(): grp.upper()}) for grp in gg.groups.keys()]
        finalDF = pd.concat(temp_dfs, axis=1)
        finalDF = finalDF.sort_index()
        return finalDF

    def getStrikeString(iNum):
        iNum = float(iNum)
        if iNum == int(iNum):
            return str(int(iNum))
        #mat = re.match(r'(\d+\.?[1-9]+)', str(iNum))
        mat = re.match(r'\d+\.?[1-9]*[0]*[1-9]+', str(iNum))
        return mat.group()    
            
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

