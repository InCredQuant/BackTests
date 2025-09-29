import pandas as pd
import re
from sqlalchemy import create_engine
import time
import os
import numpy as np
from datetime import datetime, timedelta
import psycopg2 as pg
import warnings
from io import StringIO
import re
warnings.filterwarnings("ignore")

# startdate = '2025-04-11'
# enddate = '2025-04-11'
startdate = enddate = (datetime.today().date() - timedelta(days=1)).strftime('%Y-%m-%d')

def process_gdfl_files(input_path, connection_params):
    start_time = time.time()
    engine_url = f'postgresql+psycopg2://{connection_params["user"]}:{connection_params["pwd"]}@{connection_params["host"]}:{connection_params["port"]}/{connection_params["db_name"]}'
    engine = create_engine(engine_url)
    for filename in os.listdir(input_path):
        file_path = os.path.join(input_path, filename)
        print(f"Processing {file_path}")
        df = pd.read_csv(file_path, compression='zip')
        df['Date'] = pd.to_datetime(df['Date'].str.strip(), format='%d/%m/%Y').dt.date
        df = parse_ticker_info(df)
        df.to_sql('gdfl_min', engine, if_exists='append', index=False, schema=connection_params.get('schema'))
        print(f'Completed processing file: {filename}')
    
    elapsed_minutes = round((time.time() - start_time) / 60, 1)
    print(f'Total time taken: {elapsed_minutes} minutes')

def parse_ticker_info(df):
    special_mask = df['Ticker'].str.contains('-I|-II|-III')
    std_tickers = df[~special_mask]['Ticker']
    idx_positions = std_tickers.str.extract(r'(\d)').index
    std_tickers_list = std_tickers.tolist()
    df['Name'] = None
    df['ExpiryDate'] = None
    df['StrikePrice'] = None
    df['Call_Or_Put'] = None
    for i, ticker_idx in enumerate(idx_positions):
        ticker = std_tickers_list[i]
        idx = re.search(r'\d', ticker).span()[0]
        df.loc[ticker_idx, 'Name'] = ticker[:idx]
        df.loc[ticker_idx, 'ExpiryDate'] = ticker[idx:idx+7]
        df.loc[ticker_idx, 'StrikePrice'] = ticker[idx+7:-6]
        df.loc[ticker_idx, 'Call_Or_Put'] = ticker[-6:-4]
    special_tickers = df[special_mask]['Ticker']
    special_indices = special_tickers.index
    if not special_indices.empty:
        df.loc[special_indices, 'Name'] = special_tickers.str.split('-').str[0]
    
    return df

def connection_details():
    conn = pg.connect(database=db_name, user=user_name, password=pwd, host=host, port=port)
    return conn

def get_tickerlist():
    # This is to basically get the updated list
    connection = connection_details()
    ticker_list = pd.read_sql(f''' select distinct("Ticker"),"Name","Date","StrikePrice","ExpiryDate" FROM gdfl_min where "Date" >= '{startdate}' and "Date" <= '{enddate}' and "Ticker" not like '%-I%'; ''',connection)
    connection.close()
    return ticker_list

def get_index_tickers(ticker_list):
    index_tickers = ticker_list[(ticker_list['Name'].str.contains('NIFTY')) & (~ticker_list['Name'].str.contains('FUT'))]
    index_tickers = index_tickers.sort_values(by=['Date','Name'])
    index_tickers['Call_Or_Put'] = index_tickers['Ticker'].str.replace('.NFO','').str[-2:]
    return index_tickers

def read_expiry():
    connection = connection_details()
    exp_df = pd.read_sql(''' select * FROM nseexpiry ''',connection)
    exp_df['DATE'] = pd.to_datetime(exp_df['DATE']).dt.date
    return exp_df

def pre_weekly(df):
    df['TickerLen'] = df['Ticker'].apply(lambda x: re.search('\d+', x).start())
    df['NewTicker'] = np.array([ticker[length:] for ticker,length in zip(df['Ticker'].values,df['TickerLen'].values)])
    df['NewTicker'] = np.array([ticker[:-6-length] for ticker,length in zip(df['NewTicker'].values,df['StrikePrice'].astype(int).astype(str).str.len().values)])
    df['TickerExpiry'] = pd.to_datetime(df['NewTicker'],errors='coerce').dt.date
    df.sort_values(by=['TickerExpiry'],inplace=True)
    return df 

def define_weekly(df):
    mask = df['NewTicker'].str.len() == 7
    group_numbers = df[mask].groupby(['Date'])['TickerExpiry'].transform(lambda x: x.groupby(x).ngroup() + 1)
    labels = group_numbers.where(group_numbers <= 5, None).apply(lambda x: f'W{int(x)}' if pd.notnull(x) else None)
    df['Label'] = np.where(mask, labels, '')
    df['TickerTag'] = df['Name'] + df['Label'] + df['StrikePrice'].astype(int).astype(str) + df['Call_Or_Put']    
    return df

def get_weekly_ticker(index_tickers):
    # indices = ['BANKNIFTY','NIFTY','FINNIFTY','MIDCPNIFTY']
    indices = ['NIFTY']
    result = {}
    for index in indices:
        individual_ticker = index_tickers[(index_tickers['Name']==f'{index}')]
        individual_ticker = pre_weekly(individual_ticker)
        individual_ticker['Date'] = pd.to_datetime(individual_ticker['Date']).dt.date
        individual_ticker = individual_ticker[individual_ticker['Date'] <= individual_ticker['TickerExpiry']]
        weekly_tickers = define_weekly(individual_ticker.dropna(subset=['TickerExpiry']))
        weekly_tickers['Date'] = pd.to_datetime(weekly_tickers['Date']).dt.date
        result[index] =weekly_tickers

    return result
    
def merge_weeklycontracts_withalldata(weekly_tickers,index):
    connection = connection_details()
    df = pd.read_sql(f''' select * FROM gdfl_min where "Date" >= '{startdate}' and "Date" <= '{enddate}' and "Ticker" like '{index}%' and "Ticker" not like '%YIT%' and "Ticker" not like '%-I%'; ''',connection)
    connection.close()
    indexdf = pd.merge(df,weekly_tickers[['Date','Ticker','Label','TickerTag']],on=['Date','Ticker'],how='left')
    print(indexdf.shape[0],indexdf.dropna(subset=['TickerTag']).shape[0])
    indexdf = indexdf.dropna(subset=['Label'])
    print(f"StartDate: {indexdf['Date'].min()}, EndDate: {indexdf['Date'].max()}, Index: {indexdf['Name'].unique()}")
    upload_weekly_contracts(indexdf)
    # indexdf.to_csv(rf"C:\Vishwanath\Files\\{index}_weekly.csv",index=False)
    print(f"Uploaded Weekly contracts for {index}")

def upload_weekly_contracts(df):
    conn = connection_details()
    table = 'weekly_gdfl_min_opt'
    buffer = StringIO()
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
    
    cursor.close()
    conn.close()

def create_weeklycontracts():
    exp_df = read_expiry()
    ticker_list = get_tickerlist()
    index_tickers = get_index_tickers(ticker_list)
    result = get_weekly_ticker(index_tickers)
    for index, weekly_tickers in result.items():
        print(f"Processing {index}...")
        merge_weeklycontracts_withalldata(weekly_tickers,index)

def main():
    input_path = r"C:\\Vishwanath\\Files\\GDFL\\"
    connection_params = {
        'user': user_name,
        'pwd': pwd,
        'host': host,
        'port': port,
        'db_name': db_name,
        'schema': schema
    }
    process_gdfl_files(input_path, connection_params)
    create_weeklycontracts()

if __name__ == "__main__":
    user_name = 'postgres'
    pwd = 'postgres'
    host = '10.147.0.69' #'localhost'
    port = '5432'
    db_name = 'data' #'postgres'
    schema = 'public'  #'public'
    main()