# PNLDB_NAME = 'autodash'
# PNLDB_USERNAME = 'postgres'
# PNLDB_PASSWORD = 'admin'
# PNLDB_HOST = 'localhost'
# PNLDB_PORT = '5432'
# PNLDB_URI = 'postgres+psycopg2://'+PNLDB_USERNAME+':'+PNLDB_PASSWORD+'@'+PNLDB_HOST+':'+PNLDB_PORT+'/'+PNLDB_NAME
# PNLDB_COLUMNS = ['ID', 'DATE', 'SEGMENT', 'SYMBOL', 'EXPIRY', 'STRIKE', 'OPTIONTYPE', 'UNIQUEID', 'TRADEDQTY', 'ACTIVEQTY', 'STRATEGYID', 'CLOSE', 'PREVCLOSE', 'DAILYPNL']
# PNLDB_TABLE = 'DAILYPORTFOLIO'

# DB_NAME = 'autodash'
# DB_USERNAME = 'postgres'
# DB_PASSWORD = 'admin'
# DB_HOST = 'localhost'
# DB_PORT = '5432'
# DB_URI = 'postgres+psycopg2://'+DB_USERNAME+':'+DB_PASSWORD+'@'+DB_HOST+':'+DB_PORT+'/'+DB_NAME
# ORDER_DB_COLUMNS = ['ID','PKEY', 'DATE', 'TIME', 'EXCHANGE', 'SEGMENT', 'SYMBOL', 'EXPIRY', 'STRIKE', 'OPTIONTYPE', 'ORDERTYPE', 'QUANTITY', 'PRICE', 'STRATEGYID', 'BROKER', 'COMMENT']

config = {
    'nfunds': 4,
    'fund_ids': [1,2,3,4],
    'fund_names': {1:'Liquid Fund', 2: 'EquityPlus Fund', 3: 'SectorRotation Fund', 4: 'Credit Fund'},
    'db_name' : 'autodash',
    'histdb_name' : 'data',
    'pfdb_name': 'portfolio',
    'username' : 'postgres',
    'password': 'admin',
    'host': 'localhost',
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
    'db_uri': "postgresql+psycopg2://postgres:admin@localhost:5432/autodash",
    'histdb_uri': "postgresql+psycopg2://postgres:postgres@localhost:5433/data",
    #'histdb_uri': "postgresql+psycopg2://postgres:admin@localhost:5432/nseinfo",
    'pfdb_uri': "postgresql+psycopg2://postgres:admin@localhost:5432/portfolio",
    'get_prices_columns': ['SEGMENT', 'SYMBOL', 'STRIKE', 'EXPIRY', 'OPTIONTYPE'],
    'get_nse_fno_columns' : ['Ticker', 'Instrument', 'Scrip', 'Expiry', 'Strike', 'OptionType', 'Open', 'High', 'Low', 'Close', 'SettlePrice', 'Contracts', 'Val_Lakh', 'OI', 'ChgOI', 'Date'],
    'greek_df_columns': ['Ticker', 'Date', 'IV', 'Delta', 'Rho', 'Theta', 'Vega', 'Gamma'],
}