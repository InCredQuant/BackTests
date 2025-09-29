import os
import pandas as pd
import psycopg2 as pg
from io import StringIO
from datetime import datetime
import sys
sys.path.insert(0,r'G:\Shared drives\BackTests\pycode\DBUpdation')
import pg_redirect

def get_db_connection():
    return pg.connect(dbname="data", user="postgres", password="postgres", host="192.168.44.4")

def get_max_dates_for_symbols(conn, symbols):
    max_dates = {}
    with conn.cursor() as cur:
        placeholders = ', '.join(["'%s'" % symbol for symbol in symbols])
        query = f'''SELECT "Symbol", MAX("Date") FROM spotdata 
                    WHERE "Symbol" IN ({placeholders})
                    GROUP BY "Symbol";'''
        cur.execute(query)
        results = cur.fetchall()
        
        for symbol, max_date in results:
            max_dates[symbol] = max_date
    for symbol in symbols:
        if symbol not in max_dates:
            max_dates[symbol] = datetime.min.date()
            
    return max_dates

def process_file(file_path, symbol_mapping):
    file_name = os.path.basename(file_path)
    symbol = symbol_mapping.get(file_name.replace('.csv', ''), '')
    if not symbol:
        print(f"Symbol not found for file: {file_name}")
        return None
    df = pd.read_csv(file_path)
    df['time'] = pd.to_datetime(df['date']).dt.time
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['symbol'] = symbol
    df.columns = map(str.capitalize, df.columns)
    return df[['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']]

def copy_from_dataframe(conn, df, table_name):
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False, sep='\t')
    buffer.seek(0)
    with conn.cursor() as cursor:
        try:
            cursor.copy_from(buffer, table_name, sep='\t', null='')
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            conn.rollback()
            print(f"Error during copy_from: {e}")
            raise

def update():
    symbol_mapping = {
        'NIFTY 50': 'NIFTY',
        'NIFTY BANK': 'BANKNIFTY',
        'SENSEX': 'SENSEX',
        'NIFTY MID SELECT': 'MIDCAPNIFTY',
        'NIFTY FIN SERVICE': 'FINNIFTY'
    }
    
    main_path = r"G:\Shared drives\BackTests\Spot Data 1min\New"
    files = [f for f in os.listdir(main_path) if f.replace('.csv', '') in symbol_mapping]
    symbols = [symbol_mapping[f.replace('.csv', '')] for f in files]
    with get_db_connection() as conn:
        max_dates = get_max_dates_for_symbols(conn, symbols)
        total_rows_inserted = 0
        
        for file in files:
            symbol = symbol_mapping.get(file.replace('.csv', ''), '')
            print(f"Processing {file} for symbol {symbol}")
            df = process_file(os.path.join(main_path, file), symbol_mapping)
            if df is None:
                continue
                
            max_date = max_dates.get(symbol, datetime.min.date())
            df_filtered = df[df['Date'] > max_date]
            
            if not df_filtered.empty:
                print(f"Found {len(df_filtered)} new rows for {symbol}")
                rows_inserted = copy_from_dataframe(conn, df_filtered, 'spotdata')
                total_rows_inserted += rows_inserted
                print(f"Inserted {rows_inserted} rows for {symbol}")
            else:
                print(f"{symbol} updated till latest date")
    
        if total_rows_inserted > 0:
            print(f"Successfully inserted {total_rows_inserted} total rows")
        else:
            print("No new data to insert")
        
    print("Done")

if __name__ == "__main__":
    update()