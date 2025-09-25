
"""
Created on Tuesday, 12 Sep 2023, 12:49:30
@author: Viren@InCred
Fetches the Trades Data from Data Bases

"""
#%matplotlib notebook
#from ipywidgets import *
#from ipywidgets import interactive
#%matplotlib inline
import sys
sys.path.insert(0, 'G:\\Shared drives\\BackTests\\pycode\\DBUpdation\\')
import pg_redirect

import psycopg2
import pandas as pd
import datetime as dt
import numpy as np

#import getStrikeString

from MainLibs.GetData import getStrikeString, CommaSeparatedList

import warnings
warnings.filterwarnings("ignore")
startDate = '2023-05-02'
endDate = '2025-08-28'

#STT_ = {'EQ': 0.10/100, 'FUTSTK': 0.0125/100, 'FUTIDX' : 0.0125/100, 'OPTSTK' : 0.0625/100, 'OPTIDX': 0.0625/100} #Old STT: Equity (Buying/Selling): 10bps, Options(Selling): 6.25bps, Futs(Selling): 1.25bps
STT_ = {'EQ': 0.10/100, 'FUTSTK': 0.02/100, 'FUTIDX' : 0.02/100, 'OPTSTK' : 0.10/100, 'OPTIDX': 0.10/100} #New STT: Equity (Buying/Selling): 10bps, Options(Selling): 10bps, Futs(Selling): 2bps

print('Done: Importing')
# Fetching Daily Trades Data
portConn = psycopg2.connect(dbname= 'autodash', user= 'postgres', password='postgres', host='192.168.44.4', port='5432')
portCurs = portConn.cursor()

basesql = '''Select * from public."f1_order" where "DATE" >= date(%s) and "DATE" <= date(%s);'''
portCurs.execute(portCurs.mogrify(basesql, (startDate, endDate, )))

tradesDF = pd.DataFrame(portCurs.fetchall(), columns =  [rec[0] for rec in portCurs.description])
tradesDF.DATE = pd.to_datetime(tradesDF.DATE)

# fetching Daily Active Quantity Data
portCurs.execute('''select max("DATE") as maxDate from public."f1_activeorder" where "DATE"< date(%s);''', (startDate, ))
actQtyDate = portCurs.fetchall()[0][0]
actQtyDate = actQtyDate if type(actQtyDate) == dt.date else (dt.datetime.strptime(startDate, '%Y-%m-%d') - dt.timedelta(1)).date()

portCurs.execute('''select * from public."f1_activeorder" where "DATE">= date(%s) and "DATE" <= date(%s);''', (actQtyDate, endDate))
activeQtyDF = pd.DataFrame(portCurs.fetchall(), columns =  [rec[0] for rec in portCurs.description])
activeQtyDF.DATE = pd.to_datetime(activeQtyDF.DATE)
activeQtyDF = activeQtyDF[activeQtyDF.ACTIVEQTY !=0]
print('Done: Active Quantity Data')


# Tickers List for fetching Data from BhavCopy
# Cash BhavCopy

tradesTemp1 = tradesDF[tradesDF['SEGMENT'] =='EQ']
tradesTemp1.index = tradesTemp1.SYMBOL
tradesTemp2 = tradesDF[tradesDF['SEGMENT'] !='EQ']
tradesTemp2['STRIKE'] = tradesTemp2['STRIKE'].fillna(0)
tradesTemp2['OPTIONTYPE'][tradesTemp2['OPTIONTYPE'] == 'nan'] = 'XX'
tradesTemp2.index = tradesTemp2['SYMBOL']+tradesTemp2['EXPIRY']+tradesTemp2['OPTIONTYPE']+tradesTemp2['STRIKE'].apply(getStrikeString)
tradesModDF = pd.concat([tradesTemp1, tradesTemp2], axis = 0)

activeTemp1 = activeQtyDF[activeQtyDF['SEGMENT'] =='EQ']
activeTemp1.index = activeTemp1.SYMBOL
activeTemp2 = activeQtyDF[activeQtyDF['SEGMENT'] !='EQ']
activeTemp2['STRIKE'] = activeTemp2['STRIKE'].fillna(0)
activeTemp2['OPTIONTYPE'][activeTemp2['OPTIONTYPE'] == 'nan'] = 'XX'
activeTemp2.index = activeTemp2['SYMBOL']+activeTemp2['EXPIRY']+activeTemp2['OPTIONTYPE']+activeTemp2['STRIKE'].apply(getStrikeString)
activeModDF = pd.concat([activeTemp1, activeTemp2], axis = 0)

eqScrips = set.union(set(tradesModDF[tradesModDF.SEGMENT == 'EQ'].index), set(activeModDF[activeModDF.SEGMENT == 'EQ'].index))
fnoScrips = set.union(set(tradesModDF[tradesModDF.SEGMENT != 'EQ'].index), set(activeModDF[activeModDF.SEGMENT != 'EQ'].index))
print('Scrips Fetched!')

## Fetching Price Data for Cash Eq Bhav Copy and F&O bhavcopy
priceConn = psycopg2.connect(dbname= 'data', user= 'postgres', password='postgres', host='192.168.44.4', port='5432')#host='192.168.44.9'
priceCurs = priceConn.cursor()

eqScrips_array = psycopg2.extensions.AsIs(','.join(["'%s'" % s for s in eqScrips]))
priceCurs.execute('''select "SYMBOL", "CLOSE", "TIMESTAMP" as "DATE" from public.nsecash where "TIMESTAMP" >= date(%s) and "TIMESTAMP" <= date(%s) and "SERIES" in ('EQ', 'E1', 'BE', 'ST') and "SYMBOL" in (%s);''', (actQtyDate, endDate, eqScrips_array,))
eqBhavDF = pd.DataFrame(priceCurs.fetchall(), columns =  [rec[0] for rec in priceCurs.description])
eqBhavDF.DATE = pd.to_datetime(eqBhavDF.DATE).dt.date
eqBhavDF.index = eqBhavDF.DATE
gg = eqBhavDF.groupby('SYMBOL')

#df.drop_duplicates(subset='A', keep='first', inplace=True)
#df.set_index('A', inplace=True)

temp_dfs = [pd.DataFrame(gg.get_group(grp).rename(columns={'CLOSE': grp}).drop_duplicates(subset = 'DATE', keep = 'last', inplace = False)[grp]) for grp in gg.groups.keys()]
#temp_dfs = [pd.DataFrame(gg.get_group(grp)['CLOSE']).rename(columns={'CLOSE': grp.upper()}).drop_duplicates(keep = 'last', inplace = False) for grp in gg.groups.keys()]
eqBhavDF = pd.concat(temp_dfs, axis=1)
eqBhavDF = eqBhavDF.sort_index()
print('Done: Cash Equity Price Data')
print("NA in Cash BhavCopy:", [it for it in eqScrips if it not in eqBhavDF.columns])

fnoScrips_array = psycopg2.extensions.AsIs(','.join(["'%s'" % s for s in fnoScrips]))
priceCurs.execute('''select "Ticker", "CLOSE", "TIMESTAMP" as "DATE" from public.nsefno where "TIMESTAMP" >= date(%s) and "Ticker" in (%s);''', (actQtyDate, fnoScrips_array,))
fnoBhavDF = pd.DataFrame(priceCurs.fetchall(), columns =  [rec[0] for rec in priceCurs.description])
fnoBhavDF.DATE = pd.to_datetime(fnoBhavDF.DATE).dt.date
fnoBhavDF.index = fnoBhavDF.DATE
gg = fnoBhavDF.groupby('Ticker')
temp_dfs = [pd.DataFrame(gg.get_group(grp).rename(columns={'CLOSE': grp}).drop_duplicates(subset = 'DATE', keep = 'last', inplace = False)[grp]) for grp in gg.groups.keys()]
#temp_dfs = [pd.DataFrame(gg.get_group(grp)['CLOSE']).rename(columns={'CLOSE': grp.upper()}).drop_duplicates(keep = 'last', inplace = False) for grp in gg.groups.keys()]
fnoBhavDF = pd.concat(temp_dfs, axis=1)
fnoBhavDF = fnoBhavDF.sort_index()
print('Done: F&O Price Data')
print("NA in F&O BhavCopy:", [it for it in fnoScrips if it not in fnoBhavDF.columns])

#Modify Daily momentum models Strategy id to make the grouping easy
tradesModDF['Ticker'] = tradesModDF.index
tradesModDF.DATE = pd.to_datetime(tradesModDF.DATE).dt.date
tradesModDF.index = tradesModDF.id

#['STFDMOM', 'NFOISTR', 'BNOISTR', 'NFOIDIR', 'BNOIDIR', 'BNOIRSN', 'BNOIRST', 'NFOIRSN', 'NFOIRST', 'FNOIRST', 'FNOIRSN']
CHG_MODELS = ['STFDMOM', 'NFOIDIR', 'NFOISTR', 'NFOIRST', 'NFOIRSN', 'NFOWEMA', 'NFOWMAC', 'NFOIRNG', 'BNOIDIR', 'BNOISTR', 'BNOIRST', 'BNOIRSN', 'BNOWEMA', 'BNOWMAC', 'BNOIRNG', 'FNOIRST', 'FNOIRSN']

tradesModDF.STRATEGYID = [itr[:7] if itr[:7] in CHG_MODELS else itr for itr in tradesModDF.STRATEGYID]#("STFDMOM" in itr or "NFOISTR" in itr or "BNOISTR" in itr or "NFOIDIR" in itr or "BNOIDIR" in itr)
tradesModDF.ORDERTYPE = np.where(tradesModDF.ORDERTYPE == "SELL", -1, 1)
tradesModDF['UID1'] = tradesModDF.SEGMENT + tradesModDF.Ticker + tradesModDF.STRATEGYID# + tradesModDF.DATE.dt.strftime('%d%m%Y')
tradesModDF['UID2'] = tradesModDF.SEGMENT + tradesModDF.Ticker

activeModDF['Ticker'] = activeModDF.index
activeModDF.DATE = pd.to_datetime(activeModDF.DATE).dt.date
activeModDF.index = activeModDF.id
activeModDF.STRATEGYID = [itr[:7] if itr[:7] in CHG_MODELS else itr for itr in activeModDF.STRATEGYID]
activeModDF['UID1'] = activeModDF.SEGMENT + activeModDF.Ticker + activeModDF.STRATEGYID
activeModDF['UID2'] = activeModDF.SEGMENT + activeModDF.Ticker

# Include Price in the Trades Data Frame and Active Quantity Data Frame
tradesModDF['CLOSE'] = [fnoBhavDF.loc[tradesModDF.loc[it, 'DATE'] , tradesModDF.loc[it, 'Ticker']] if tradesModDF.loc[it, 'Ticker'] in fnoBhavDF.columns else eqBhavDF.loc[tradesModDF.loc[it, 'DATE'] , tradesModDF.loc[it, 'Ticker']] for it in tradesModDF.index]
tradesModDF['CLOSE'] = tradesModDF['CLOSE'].fillna(0)
tradesModDF['PnL']  = np.multiply(np.subtract(tradesModDF.CLOSE, tradesModDF.PRICE), np.multiply(tradesModDF.ORDERTYPE, tradesModDF.QUANTITY))

## STT impact Calculation
#STT_ = {'EQ': 0.10/100, 'FUTSTK': 0.0125/100, 'FUTIDX' : 0.0125/100, 'OPTSTK' : 0.0625/100, 'OPTIDX': 0.0625/100}
tradesModDF['STT'] = 0.0

eq_stt = tradesModDF[tradesModDF.SEGMENT == 'EQ']
tradesModDF.loc[eq_stt.index, 'STT' ]= np.multiply(np.multiply(eq_stt.QUANTITY, eq_stt.PRICE), STT_['EQ']).abs()

fut_stk_stt =  tradesModDF[(tradesModDF.SEGMENT == 'FUTSTK') & (tradesModDF.ORDERTYPE == -1)]
tradesModDF.loc[fut_stk_stt.index, 'STT' ]= np.multiply(np.multiply(fut_stk_stt.QUANTITY, fut_stk_stt.PRICE), STT_['FUTSTK']).abs()

fut_idx_stt =  tradesModDF[(tradesModDF.SEGMENT == 'FUTIDX') & (tradesModDF.ORDERTYPE == -1)]
tradesModDF.loc[fut_idx_stt.index, 'STT' ]= np.multiply(np.multiply(fut_idx_stt.QUANTITY, fut_idx_stt.PRICE), STT_['FUTIDX']).abs()

opt_stk_stt =  tradesModDF[(tradesModDF.SEGMENT == 'OPTSTK') & (tradesModDF.ORDERTYPE == -1)]
tradesModDF.loc[opt_stk_stt.index, 'STT' ]= np.multiply(np.multiply(opt_stk_stt.QUANTITY, opt_stk_stt.PRICE), STT_['OPTSTK']).abs()

opt_idx_stt =  tradesModDF[(tradesModDF.SEGMENT == 'OPTIDX') & (tradesModDF.ORDERTYPE == -1)]
tradesModDF.loc[opt_idx_stt.index, 'STT' ]= np.multiply(np.multiply(opt_idx_stt.QUANTITY, opt_idx_stt.PRICE), STT_['OPTIDX']).abs()

#tradesModDF['PnL'] = tradesModDF['PnL'] - tradesModDF['STT']
################

ALLDATES = list(set(activeModDF.DATE))
ALLDATES.append(actQtyDate)
ALLDATES.append(dt.date(2025, 8, 28))
ALLDATES = list(set(ALLDATES))
ALLDATES.sort()

# Calculates Price Diff for PnL Calculation
eqBhavDiff = eqBhavDF.diff()
fnoBhavDiff = fnoBhavDF.diff()

#activeModDF = activeModDF[~((activeModDF.SYMBOL == 'UPLPP') & (activeModDF.DATE > dt.date(2025, 1, 28)))]


result = []
for it in activeModDF.index:
    if (activeModDF.loc[it, 'DATE'].strftime('%Y-%m-%d') == endDate):
        result.append(ALLDATES[ALLDATES.index(activeModDF.loc[it, 'DATE'])])
    else:
        result.append(ALLDATES[ALLDATES.index(activeModDF.loc[it, 'DATE'])+1])

activeModDF['NextDate'] = [ALLDATES[ALLDATES.index(activeModDF.loc[it, 'DATE']) if (activeModDF.loc[it, 'DATE'].strftime('%Y-%m-%d') == endDate) else ALLDATES.index(activeModDF.loc[it, 'DATE'])+1] for it in activeModDF.index]
activeModDF = activeModDF[activeModDF.DATE != activeModDF.NextDate]

activeModDF['PriceDiff'] = [fnoBhavDiff.loc[activeModDF.loc[it, 'NextDate'] , activeModDF.loc[it, 'Ticker']] if activeModDF.loc[it, 'Ticker'] in fnoBhavDF.columns else eqBhavDiff.loc[activeModDF.loc[it, 'NextDate'] , activeModDF.loc[it, 'Ticker']] for it in activeModDF.index]
activeModDF['PriceDiff'] = activeModDF['PriceDiff'].fillna(0)
activeModDF['PnL'] = np.multiply(activeModDF.PriceDiff, activeModDF.ACTIVEQTY)


############### SEGMENT PNL
sgGrpTrades = tradesModDF.groupby('SEGMENT')
temp_dfs = [pd.DataFrame(sgGrpTrades.get_group(grp).rename(columns={'PnL': grp}))[[grp, 'DATE']].groupby('DATE').sum() for grp in sgGrpTrades.groups.keys()]
TradesPNL_seg = pd.concat(temp_dfs, axis=1)
TradesPNL_seg = TradesPNL_seg.sort_index()

sgGrpPosition = activeModDF.groupby('SEGMENT')
temp_dfs = [pd.DataFrame(sgGrpPosition.get_group(grp).rename(columns={'PnL': grp}))[[grp, 'NextDate']].groupby('NextDate').sum() for grp in sgGrpPosition.groups.keys()]
PositionPNL_seg = pd.concat(temp_dfs, axis=1)
PositionPNL_seg = PositionPNL_seg.sort_index()

segment_PNL = TradesPNL_seg.add(PositionPNL_seg, fill_value=0)
segment_PNL.index = pd.to_datetime(segment_PNL.index).date
segment_PNL['Fund'] = segment_PNL.sum(axis = 1)

############### STRATEGY PNL
sgGrpTrades = tradesModDF.groupby('STRATEGYID')
temp_dfs = [pd.DataFrame(sgGrpTrades.get_group(grp).rename(columns={'PnL': grp}))[[grp, 'DATE']].groupby('DATE').sum() for grp in sgGrpTrades.groups.keys()]
TradesPNL_strat = pd.concat(temp_dfs, axis=1)
TradesPNL_strat = TradesPNL_strat.sort_index()

sgGrpPosition = activeModDF.groupby('STRATEGYID')
temp_dfs = [pd.DataFrame(sgGrpPosition.get_group(grp).rename(columns={'PnL': grp}))[[grp, 'NextDate']].groupby('NextDate').sum() for grp in sgGrpPosition.groups.keys()]
PositionPNL_strat = pd.concat(temp_dfs, axis=1)
PositionPNL_strat = PositionPNL_strat.sort_index()

Strategy_PNL = TradesPNL_strat.add(PositionPNL_strat, fill_value=0)
Strategy_PNL.index = pd.to_datetime(Strategy_PNL.index).date
Strategy_PNL['Fund'] = Strategy_PNL.sum(axis = 1)


############### TICKER PNL
# sgGrpTrades = tradesModDF.groupby('Ticker')
# temp_dfs = [pd.DataFrame(sgGrpTrades.get_group(grp).rename(columns={'PnL': grp}))[[grp, 'DATE']].groupby('DATE').sum() for grp in sgGrpTrades.groups.keys()]
# TradesPNL_tick = pd.concat(temp_dfs, axis=1)
# TradesPNL_tick = TradesPNL_tick.sort_index()

# sgGrpPosition = activeModDF.groupby('Ticker')
# temp_dfs = [pd.DataFrame(sgGrpPosition.get_group(grp).rename(columns={'PnL': grp}))[[grp, 'NextDate']].groupby('NextDate').sum() for grp in sgGrpPosition.groups.keys()]
# PositionPNL_tick = pd.concat(temp_dfs, axis=1)
# PositionPNL_tick = PositionPNL_tick.sort_index()

# Ticker_PNL = TradesPNL_tick.add(PositionPNL_tick, fill_value=0)
# Ticker_PNL.index = pd.to_datetime(Ticker_PNL.index).date
# Ticker_PNL['Fund'] = Ticker_PNL.sum(axis = 1)

############## Combining Trades and Posiiton P&L
#sgGrpTradesDt = tradesModDF.groupby('DATE')
#temp_dfs = [pd.DataFrame(tradesModDF.groupby(['Ticker', 'DATE']).agg({'PnL': 'sum'})) for grp in sgGrpTradesDt.groups.keys()]
#temp_dfs = [pd.DataFrame(sgGrpTradesDt.get_group(grp))[['DATE', 'Ticker', 'PnL']].groupby(['Ticker', 'DATE']).agg({'PnL': 'sum'}) for grp in sgGrpTradesDt.groups.keys()]
TradesPNL_tickDt = pd.DataFrame(tradesModDF.groupby(['SEGMENT', 'Ticker', 'DATE']).agg({'PnL': 'sum'})).rename(columns={'PnL': 'TradesPnL'})#pd.concat(temp_dfs, axis=0)
PositionPNL_tickDt = pd.DataFrame(activeModDF.groupby(['SEGMENT', 'Ticker', 'NextDate']).agg({'PnL': 'sum'})).rename(columns={'PnL': 'PositionPnL'})
PositionPNL_tickDt.index.names = ['SEGMENT', 'Ticker', 'DATE']
#TradesPNL_tick = TradesPNL_tick.sort_index()
TickerPnL = pd.concat([TradesPNL_tickDt, PositionPNL_tickDt], axis = 1)
TickerPnL = TickerPnL.fillna(0)
TickerPnL['Total'] = TickerPnL.sum(axis = 1)
TickerPnL = TickerPnL.sort_index(level = 'DATE')

#################### Read Fund and Strategies Weights Data
weights_st = pd.read_excel('G:/Shared drives/QuantFunds/Liquid1/PnLReports/HistoricalWeights-LiquidFund-Models.xlsx', sheet_name = 'Weights', header = 1, index_col = 0)
FundSize = pd.read_excel('G:/Shared drives/QuantFunds/Liquid1/PnLReports/HistoricalWeights-LiquidFund-Models.xlsx', sheet_name = 'FundSize', header = 0, index_col = 0)
strategyLookup = pd.read_excel('G:/Shared drives/QuantFunds/Liquid1/PnLReports/HistoricalWeights-LiquidFund-Models.xlsx', sheet_name = 'LookUpTable', header = 0, index_col = 3, usecols =  range(0, 7) )


stExposure = [np.multiply(weights_st.loc[:, col], FundSize.FundSize) for col in weights_st.columns]
StrategyExp = pd.DataFrame(stExposure).transpose().shift(1)
StrategyExp.columns = weights_st.columns
#StrategyExp = np.multiply(weights_st, FundSize).shift(1)


Strategy_PNL = Strategy_PNL.fillna(0)
StrategyExp = StrategyExp.loc[Strategy_PNL.index, Strategy_PNL.columns]

Strategy_Rets = np.divide(Strategy_PNL, StrategyExp)
tempdf = pd.DataFrame(np.zeros(len(Strategy_Rets.columns)), columns = [Strategy_Rets.index[0] - dt.timedelta(1)], index = Strategy_Rets.columns).transpose()
Strategy_Rets = pd.concat([tempdf, Strategy_Rets], axis = 0)
Strategy_Rets = Strategy_Rets.fillna(0)
Strategy_NAV = (1+ Strategy_Rets)
Strategy_NAV.iloc[0, :] = 100
Strategy_NAV = Strategy_NAV.cumprod(axis = 0)


## Make the copy of P&L and Exposure data frame for calculating the returns of Strategy Groups and Baskets
import copy
baskets_pnl1 = copy.deepcopy(Strategy_PNL)
baskets_pnl2 = copy.deepcopy(Strategy_PNL)
baskets_exp1 = copy.deepcopy(StrategyExp)
baskets_exp2 = copy.deepcopy(StrategyExp)

baskets_pnl1.columns = [strategyLookup.loc[it, 'StrategyGroup'] if it != 'Fund' else 'Fund' for it in baskets_pnl1.columns]
baskets_pnl2.columns = [strategyLookup.loc[it, 'Basket'] if it != 'Fund' else 'Fund' for it in baskets_pnl2.columns]
baskets_exp1.columns = [strategyLookup.loc[it, 'StrategyGroup'] if it != 'Fund' else 'Fund' for it in baskets_exp1.columns]
baskets_exp2.columns = [strategyLookup.loc[it, 'Basket'] if it != 'Fund' else 'Fund' for it in baskets_exp2.columns]

orderdeColumns = ['DailyStockMom', 'MonthlyBasket', 'StockMomDics', 'MSCI', 'BankNiftyDisp', 'StockOptions', 'Arbitrage', 'SpecialSits', 'PairTrades', 
                  'NiftyFuts', 'BankNiftyFuts', 'NiftyMaxOI', 'BankNiftyMaxOI', 'BankNiftySpread', 'BankNiftyCondor', 'IndexOptions', 'IntraDayOptions', 'PositionalOptions', 'DirHedge', 'DeltaHedge',
                  'FutsLongShort', 'RelativeValue', 'IndexF&O', 'Hedge', 'Fund']

baskets_pnl = pd.concat([baskets_pnl1.groupby(baskets_pnl1.columns, axis = 1).sum(), baskets_pnl2.groupby(baskets_pnl2.columns, axis = 1).sum()], axis = 1)
baskets_pnl = baskets_pnl.loc[:, ~baskets_pnl.columns.duplicated()]
baskets_exp = pd.concat([baskets_exp1.groupby(baskets_exp1.columns, axis = 1).sum(), baskets_exp2.groupby(baskets_exp2.columns, axis = 1).sum()], axis = 1)
baskets_exp = baskets_exp.loc[:, ~baskets_exp.columns.duplicated()]
baskets_rets = np.divide(baskets_pnl, baskets_exp)#.fillna(0)
baskets_rets = baskets_rets[orderdeColumns]
baskets_rets_attribution = baskets_pnl.divide(baskets_exp.Fund, axis = 0)
baskets_rets_attribution  = baskets_rets_attribution[orderdeColumns]

reportRets = copy.deepcopy(baskets_rets_attribution)
reportRets.index = pd.to_datetime(reportRets.index)
reportRets = reportRets.resample('m', convention = 'end').sum()
reportRets.index = reportRets.index.strftime('%b%Y')

report1 = reportRets.loc[:, ['FutsLongShort', 'RelativeValue', 'IndexF&O', 'Hedge', 'Fund']]
report2 = reportRets.loc[:, ['DailyStockMom', 'MonthlyBasket', 'StockMomDics', 'MSCI', 'FutsLongShort']]
report3 = reportRets.loc[:, ['BankNiftyDisp', 'StockOptions', 'Arbitrage', 'SpecialSits', 'PairTrades', 'RelativeValue']]
report4 = reportRets.loc[:, ['NiftyFuts', 'BankNiftyFuts', 'NiftyMaxOI', 'BankNiftyMaxOI',  'BankNiftySpread', 'BankNiftyCondor', 'IndexOptions', 'IntraDayOptions', 'PositionalOptions', 'IndexF&O']]
report5 = reportRets.loc[:, ['DirHedge', 'DeltaHedge', 'Hedge']]

##########################
#modelized = ['BNFDMOM04', 'BNFDMOM06', 'BNFDMOM10', 'BNFDMOM11', 'BNFDMOM12', 'BNFDMOM13', 'BNFDMOM14', 'BNFDMOM15', 'BNFDMOM16','BNFSPREAD', 'BNODMAXOI', 'BNOMDISP1', 'BNOMDISP2', 'BNOMDISP3', 'BNOMDISP4', 'BNOWCOND', 'NFFDMOM06', 'NFFDMOM01', 'NFFDMOM12', 'NFFDMOM13', 'NFFDMOM14',  'NFFDMOM15', 'NFFDMOM16', 'STFDMOM', 'STFMMOM6M', 'STFMREV1M', 'STFMSEC2W', 'STFMVOL6M', 'STFDMOM', 'NFOISTR', 'BNOISTR', 'NFOIDIR', 'BNOIDIR', 'BNOIRSN', 'BNOIRST', 'NFOIRSN', 'NFOIRST', 'Fund']#'FNOIRST', 'FNOIRSN', 

modelized = ['STFDMOM', 'STFMMOM6M', 'STFMSEC2W', 'STFMREV1M', 'STFMVOL6M', 'STFMNMOLS', 'BNOMDISP1', 'BNOMDISP2', 'BNOMDISP3', 'BNOMDISP4', 'NFODMAXOI', 'BNODMAXOI', 'BNFSPREAD', 'BNOWCOND', 'NFFDMOM14', 'NFFDMOM15', 'NFFDMOM16', 'NFFDMOM01', 'NFFDMOM06', 'NFF2MOM09', 'NFFDMOM12', 'NFFDMOM13', 'BNFDMOM14', 'BNFDMOM15', 'BNFDMOM16', 'BNF2MOM01', 'BNF2MOM03', 'BNFDMOM04', 'BNFDMOM06', 'BNF2MOM09', 'BNFDMOM10', 'BNFDMOM11', 'BNFDMOM12', 'BNFDMOM13', 'NFOISTR', 'BNOISTR', 'NFOIDIR', 'BNOIDIR', 'NFOIRST', 'NFOIRSN', 'BNOIRST', 'BNOIRSN', 'FNOIRST', 'FNOIRSN', 'NFOIRNG', 'BNOIRNG', 'NFOWEMA', 'BNOWEMA', 'FNOWEMA', 'NFOWMAC', 'BNOWMAC', 'FNOWMAC', 'NFOIMAXOI', 'BNOIMAXOI', 'FNOIMAXOI', 'NFOWSSTRD', 'BNOWSSTRD', 'NFOWLSTRD', 'BNOWLSTRD', 'NFOMLSTRD', 'BNOMLSTRD', 'BNOMDDSPR', 'NFOWRNGF1', 'BNOIHEDG', 'NFDELTHED', 'NFODDIRHG', 'Fund']

for imod in ['FNOWEMA', 'FNOWMAC', 'FNOIMAXOI', 'NFOWSSTRD'] :
    if imod in modelized:
        modelized.remove(imod)
## Make the copy of P&L and Exposure data frame for calculating the returns of Strategy Groups and Baskets explained by Models, as mentioned above by the modelized strateegy codes
basketsMod_pnl1 = copy.deepcopy(Strategy_PNL)
basketsMod_pnl2 = copy.deepcopy(Strategy_PNL)
basketsMod_exp1 = copy.deepcopy(StrategyExp)
basketsMod_exp2 = copy.deepcopy(StrategyExp)

basketsMod_pnl1 = basketsMod_pnl1.loc[:, modelized]
basketsMod_pnl2 = basketsMod_pnl2.loc[:, modelized]
basketsMod_exp1 = basketsMod_exp1.loc[:, modelized]
basketsMod_exp2 = basketsMod_exp2.loc[:, modelized]

# Updating fund p&L by explained by models parts
basketsMod_pnl1.Fund = basketsMod_pnl1.sum(axis = 1, )-basketsMod_pnl1.loc[:, 'Fund']
del basketsMod_pnl2['Fund']

basketsMod_pnl1.columns = [strategyLookup.loc[it, 'StrategyGroup'] if it != 'Fund' else 'Fund' for it in basketsMod_pnl1.columns]
basketsMod_pnl2.columns = [strategyLookup.loc[it, 'Basket'] if it != 'Fund' else 'Fund' for it in basketsMod_pnl2.columns]
basketsMod_exp1.columns = [strategyLookup.loc[it, 'StrategyGroup'] if it != 'Fund' else 'Fund' for it in basketsMod_exp1.columns]
basketsMod_exp2.columns = [strategyLookup.loc[it, 'Basket'] if it != 'Fund' else 'Fund' for it in basketsMod_exp2.columns]

basketsMod_pnl = pd.concat([basketsMod_pnl1.groupby(basketsMod_pnl1.columns, axis = 1).sum(), basketsMod_pnl2.groupby(basketsMod_pnl2.columns, axis = 1).sum()], axis = 1)
basketsMod_pnl = basketsMod_pnl.loc[:, ~basketsMod_pnl.columns.duplicated()]
basketsMod_exp = pd.concat([basketsMod_exp1.groupby(basketsMod_exp1.columns, axis = 1).sum(), basketsMod_exp2.groupby(basketsMod_exp2.columns, axis = 1).sum()], axis = 1)
basketsMod_exp = basketsMod_exp.loc[:, ~basketsMod_exp.columns.duplicated()]
basketsMod_rets = np.divide(basketsMod_pnl, basketsMod_exp)#.fillna(0)

writer = pd.ExcelWriter('G:/Shared drives/QuantFunds/Liquid1/PnLReports/LiquidFund_PnL_'+ dt.datetime.now().strftime('%d%b%Y')+'.xlsx')
report1.to_excel(writer, 'ContReport', startcol = 0)
report2.to_excel(writer, 'ContReport', startcol = report1.shape[1]+3)
report3.to_excel(writer, 'ContReport', startcol = report1.shape[1]+3+report2.shape[1]+2)
report4.to_excel(writer, 'ContReport', startcol = report1.shape[1]+3+report2.shape[1]+2+report3.shape[1]+2)
report5.to_excel(writer, 'ContReport', startcol = report1.shape[1]+3+report2.shape[1]+2+report3.shape[1]+2+report4.shape[1]+2)

rp1 = copy.deepcopy(Strategy_Rets)
rp2 = copy.deepcopy(baskets_rets)
rp1.index = pd.to_datetime(rp1.index)
rp2.index = pd.to_datetime(rp2.index)
rp1 = rp1.resample('m').sum()
rp1.index = report1.index
rp2 = rp2.resample('m').sum()
rp2.index = report1.index
rp1.to_excel(writer, 'MonthlyRets', startrow = 0)
rp2.to_excel(writer, 'MonthlyRets', startrow = rp1.shape[0]+3)

segment_PNL.to_excel(writer,'Segment')
Strategy_PNL.to_excel(writer,'Strategy')
baskets_pnl.to_excel(writer, 'Baskets')
Strategy_Rets.to_excel(writer, 'StrategyRets')
baskets_rets.to_excel(writer, 'BasketsRets')
baskets_rets_attribution.to_excel(writer, 'BasketRetsAttribution')
Strategy_NAV.to_excel(writer, 'StrategyNAV')
basketsMod_pnl.to_excel(writer, 'ModelsExpPnL')
basketsMod_rets.to_excel(writer, 'ModelsExpRets')

TickerPnL.to_excel(writer,'Ticker')
tradesModDF.TIME = pd.to_datetime(tradesModDF.TIME).dt.tz_localize(None)
tradesModDF.to_excel(writer,'Trades', index = False)
activeModDF.TIME = pd.to_datetime(activeModDF.TIME).dt.tz_localize(None)
activeModDF.to_excel(writer,'Positions', index = False)


#writer.save()
writer.close()
print('Completed@', dt.datetime.now().time())
#############################################