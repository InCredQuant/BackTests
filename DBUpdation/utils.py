import sqlite3
import datetime 
import numpy as np
import pandas as pd
import mibian
from sqlalchemy import create_engine
from config import config

from get_bhavcopy_data_utils import GetNSEBhavCopyFutsData, GetNSEBhavCopyOptsData, getStrikeString, CommaSeparatedList
import py_vollib.black_scholes_merton.implied_volatility
import py_vollib.black_scholes.greeks.numerical
import pdb
def check_empty(obj):
    if isinstance(obj, pd.DataFrame):
        if not obj.empty: 
            return obj
        else:
            return pd.DataFrame()




def get_prices(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date):
    """
    Get Data for all securities: Futures, Options, Equities

    Parameters
    -----------
    df: 
        Pandas DataFrame with info of securities [SEGMENT, SYMBOL, STRIKE, EXPIRY, OPTIONTYPE]
    start_date: 
        From date (in datetime.date(yyyy, mm, dd) format)
    end_date: 
        To Date (in datetime.date(yyyy, mm, dd) format)

    Returns:
    --------- 
    finalPrices:    
        Close and PrevClose Prices of all securities (Pandas DataFrame)
    """
    conn = sqlite3.connect('G:\Shared drives\BackTests\DB\Z\BhavCopy.db')
    

    df['UNIQUECODE'] = df.apply(lambda r: 
                                f'{r.SYMBOL}{r.EXPIRY}{r.OPTIONTYPE}{getStrikeString(r.STRIKE)}' if r.SEGMENT == 'OPTIDX' or r.SEGMENT == 'OPTSTK' 
                                else (f'{r.SYMBOL}{r.EXPIRY}' if r.SEGMENT == 'FUTSTK' or r.SEGMENT == 'FUTIDX' else f'{r.SYMBOL}'), axis=1)

    eqs = df.loc[df['SEGMENT'] == 'EQ', ['SEGMENT', 'SYMBOL', 'UNIQUECODE']]
    futs = df.loc[df['SEGMENT'].isin(['FUTIDX', 'FUTSTK']), ['SEGMENT', 'SYMBOL', 'EXPIRY', 'UNIQUECODE']]
    opts = df.loc[df['SEGMENT'].isin(['OPTIDX', 'OPTSTK']), ['SEGMENT', 'SYMBOL', 'STRIKE','EXPIRY', 'OPTIONTYPE', 'UNIQUECODE']]

    # fetch Data for cash equity
    def get_equity_hist(eqs):
        engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/autodash")
        basesql = "SELECT \"CLOSE\", \"PREVCLOSE\", \"SYMBOL\" FROM public.\"NSECASH\" WHERE \"SERIES\" = 'EQ' AND \"SYMBOL\" in %s AND \"TIMESTAMP\" = date('%s');" %(CommaSeparatedList(list(eqs['SYMBOL'].unique())), end_date)
        eqdf = pd.read_sql(basesql, engine)
        eqdf = eqdf.rename(columns={'SYMBOL':'UNIQUECODE'})
        # print(eqs)
        # print(eqdf)
        merged_df = pd.merge(eqdf, eqs, left_on='UNIQUECODE', right_on='UNIQUECODE')
        merged_df = merged_df.drop_duplicates()
        # print(merged_df)
        return merged_df

    # fetch Data for Futures for each expiry
    def get_futures_hist(conn, futs):
        futsData = pd.DataFrame()
        for expiry in list(futs['EXPIRY'].unique()):
            secNames = list(futs.loc[futs['EXPIRY'].isin([expiry]), 'SYMBOL'].unique())
            temp = GetNSEBhavCopyFutsData(conn, 
                                        secNames=secNames, 
                                        fieldName='Close', expiry=expiry, 
                                        fromDate=start_date, toDate=end_date)
            temp = temp.T
            # print(temp)
            temp['UNIQUECODE'] = temp.index + str(expiry).upper()
            temp.reset_index(inplace=True, drop=True)
            # print(expiry, secNames)
            # print(futs)
            # print(temp)
            merged_df = pd.merge(temp, futs, left_on='UNIQUECODE', right_on='UNIQUECODE')
            merged_df = merged_df.rename(columns={pd.to_datetime(start_date) : 'PREVCLOSE', pd.to_datetime(end_date):'CLOSE'})
            merged_df = merged_df.drop_duplicates()
            # print(merged_df)
            futsData = pd.concat([futsData, merged_df], ignore_index=True)

        return futsData

    # fetch Data for options based on strike and expiry
    def get_options_hist(conn, opts):
        optsData = pd.DataFrame()
        for expiry in list(opts['EXPIRY'].unique()):
            secNames = list(opts.loc[opts['EXPIRY'].isin([expiry]), 'SYMBOL'].unique())
            for sec in secNames:
                strikes = list(opts.loc[(opts['SYMBOL'].isin([sec])) & (opts['EXPIRY'].isin([expiry])), 'STRIKE'].unique())
                strikes = [getStrikeString(x) for x in strikes]
                # print(f'{sec} : {expiry} : {strikes}')
                temp = GetNSEBhavCopyOptsData(conn=conn, secName=sec, fieldName='CLOSE', 
                                            strikes=strikes, expiry=expiry, call_put=['CE', 'PE'], 
                                            fromDate=start_date, toDate=end_date)
                temp = temp.T
                temp['UNIQUECODE'] = temp.index
                temp.reset_index(inplace=True, drop=True)
                # print(temp)
                merged_df = pd.merge(temp, opts, left_on='UNIQUECODE', right_on='UNIQUECODE')
                merged_df = merged_df.rename(columns={pd.to_datetime(start_date) : 'PREVCLOSE', pd.to_datetime(end_date):'CLOSE'})
                merged_df = merged_df.drop_duplicates()
                optsData = pd.concat([optsData, merged_df], ignore_index=True)
        return optsData
    if not eqs.empty:
        equity_prices = get_equity_hist(eqs=eqs)
    else:
        equity_prices = pd.DataFrame()
    if not futs.empty:
        futures_prices = get_futures_hist(conn=conn, futs=futs)
    else:
        futures_prices = pd.DataFrame()
    if not opts.empty:
        options_prices = get_options_hist(conn=conn, opts=opts)
    else:
        options_prices = pd.DataFrame()
    # print(equity_prices)
    # print(futures_prices)
    # print(options_prices)
    finalPrices = pd.concat([equity_prices, futures_prices, options_prices], axis=0, ignore_index=True)
    return finalPrices

def calculate_intraday_pnl(df: pd.DataFrame, price_df: pd.DataFrame):
    """
    Parameters:
    -----------
    df: 
        pandas.DataFrame of Orders fetched from Order Table 
    price_df:
        pandas DataFrame of Prices fetched from BhavCopy for all securities
        
    Retruns:
    -------- 
        pandas.DataFrame with TradedQty, IntraDayPnL, Close and PrevClose price

    """
    # df = df.sort_values(by=['TIME'], ascending=True)
    # price_df = get_prices(df[['SEGMENT', 'SYMBOL', 'STRIKE', 'EXPIRY', 'OPTIONTYPE']], fromDate, toDate)
    #print(price_df)

    ### CREATE UNIQUECODE WRT RESPECT TO SECURITY INFORMATION (SEGMENT, SYMBOL, STRIKE, EXPIRY, OPTIONTYPE) TO MAP CLOSING PRICES
    df['UNIQUECODE'] = df.apply(lambda r: 
                                f'{r.SYMBOL}{r.EXPIRY}{r.OPTIONTYPE}{getStrikeString(r.STRIKE)}' if r.SEGMENT == 'OPTIDX' or r.SEGMENT == 'OPTSTK' 
                                else (f'{r.SYMBOL}{r.EXPIRY}' if r.SEGMENT == 'FUTSTK' or r.SEGMENT == 'FUTIDX' else f'{r.SYMBOL}'), axis=1)
    df['CLOSE'] = df['UNIQUECODE'].map(price_df.set_index('UNIQUECODE')['CLOSE'])
    df['PREVCLOSE'] = df['UNIQUECODE'].map(price_df.set_index('UNIQUECODE')['PREVCLOSE'])
    df.reset_index(drop=True, inplace=True)

    df['INTRADAYPNL'] = df.apply(lambda x: x.QUANTITY*(x.CLOSE - x.PRICE) if x.ORDERTYPE == 'BUY' else -1*x.QUANTITY*(x.CLOSE - x.PRICE), axis=1)

    ### GROUPBY UNIQUEID TO CALCULATE NET INTRADAY PNL AND TRADED QUANTITY
    df.drop(['UNIQUECODE'], axis=1, inplace=True)
    df['UNIQUEID'] = df.apply(lambda r: 
                                f'{r.SEGMENT}{r.SYMBOL}{r.EXPIRY}{r.OPTIONTYPE}{r.STRIKE}{r.STRATEGYID}' if r.SEGMENT == 'OPTIDX' or r.SEGMENT == 'OPTSTK' 
                                else (f'{r.SEGMENT}{r.SYMBOL}{r.EXPIRY}{r.STRATEGYID}' if r.SEGMENT == 'FUTSTK' or r.SEGMENT == 'FUTIDX' else f'{r.SEGMENT}{r.SYMBOL}{r.STRATEGYID}'), axis=1)

    df['TRADEDQTY'] = df.apply(lambda x: x.QUANTITY if x.ORDERTYPE == 'BUY' else -x.QUANTITY, axis=1)
    new_df = df.groupby(['UNIQUEID', 'SEGMENT','SYMBOL', 'STRATEGYID', 'STRIKE', 'EXPIRY', 'OPTIONTYPE', 'CLOSE', 'PREVCLOSE'], dropna=False)[['TRADEDQTY', 'INTRADAYPNL']].sum()
    new_df.reset_index(names=['UNIQUEID', 'SEGMENT','SYMBOL', 'STRATEGYID', 'STRIKE', 'EXPIRY', 'OPTIONTYPE', 'CLOSE', 'PREVCLOSE'], inplace=True)
    new_df = new_df.rename(columns={'INTRADAYPNL' : 'DAILYPNL'})
    new_df.loc[:,'ACTIVEQTY'] = [0]*new_df.shape[0]
    # print(new_df)
    # print(df)
    # df.to_csv('newdf.csv')
    return new_df


def calculate_lastpf_pnl(df, price_df):
    """
    Get PnL for yesterday's portfolio carried till today's closing

    Parameters:
    -----------
    df:
        pandas DataFrame of yersterday's portfolio
    price_df:
        pandas DataFrame of prices
    
    Returns:
    --------
        pandas DataFrame with DayPnL, Close and PrevClose price
    
    """
    ### IN ACTIVEQTY COLUMN FOR TODAYS PORTFOLIO WE WILL ALWAYS STORE ACTIVEQTY TILL RECENT

    df['TRADEDQTY'] -= df['TRADEDQTY'] # FILL 0 FOR TICKERS NOT TRADED TODAY BUT ACTIVE IN PORTFOLIO

    ### DROP ALL ROWS WHERE ACTIVEQTY IS 0
    df.drop(df.index[df['ACTIVEQTY'] == 0], inplace=True)
    ### DROP ALL OPTIONS WHICH EXPIRED
    opt = df.loc[df['SEGMENT'].isin(['OPTIDX', 'OPTSTK'])]
    opt['DATE'] = pd.to_datetime(opt['DATE'], errors='coerce')
    opt['CDATE'] = opt['DATE'].dt.strftime("%d%b%y").str.upper()
    idx = opt.loc[opt['CDATE'] == opt['EXPIRY']]['UNIQUEID'].values
    #print(df.loc[df['UNIQUEID'].isin(idx)])
    df.drop(df.index[df['UNIQUEID'].isin(idx)], inplace=True)


    df['UNIQUECODE'] = df.apply(lambda r: 
                                f'{r.SYMBOL}{r.EXPIRY}{r.OPTIONTYPE}{getStrikeString(r.STRIKE)}' if r.SEGMENT == 'OPTIDX' or r.SEGMENT == 'OPTSTK' 
                                else (f'{r.SYMBOL}{r.EXPIRY}' if r.SEGMENT == 'FUTSTK' or r.SEGMENT == 'FUTIDX' else f'{r.SYMBOL}'), axis=1)
    df['CLOSE'] = df['UNIQUECODE'].map(price_df.set_index('UNIQUECODE')['CLOSE'])
    df['PREVCLOSE'] = df['UNIQUECODE'].map(price_df.set_index('UNIQUECODE')['PREVCLOSE'])
    df.reset_index(drop=True, inplace=True)
    df['DAILYPNL'] = df['ACTIVEQTY'] * (df['CLOSE'] - df['PREVCLOSE'])
    print(df)
    df.drop(['UNIQUECODE'], axis=1, inplace=True)
    return df



def get_portfolio_pnl(intraday, portfolio, date: datetime.date):
    """
    Get total updated portfolio with merged intraday and portfolio pnl

    Parameters:
    -----------
    intraday:
        pandas DataFrame of intraday pnl
    portfolio:
        pandas DataFrame of portfolio pnl
    date:
        datetime.date to update portfolio date
    
    Returns:
    --------
        pandas DataFrame with DayPnL, Close and PrevClose price
    
    """
    if portfolio.empty:
        intraday['ACTIVEQTY'] = intraday['TRADEDQTY']
        return intraday
    tradedqty_map = intraday.set_index('UNIQUEID')['TRADEDQTY'].to_dict()
    pnl_map = intraday.set_index('UNIQUEID')['DAILYPNL'].to_dict()
    portfolio['DAILYPNL'] += portfolio['UNIQUEID'].map(pnl_map).fillna(portfolio['DAILYPNL'])
    portfolio['TRADEDQTY'] = portfolio['UNIQUEID'].map(tradedqty_map).fillna(portfolio['TRADEDQTY'])

    portfolio = pd.concat([portfolio, intraday[~intraday['UNIQUEID'].isin(portfolio['UNIQUEID'])]], ignore_index=True)
    portfolio['DATE'] = [date] * portfolio.shape[0]
    portfolio = portfolio.drop(['ID'], axis=1)
    portfolio['ACTIVEQTY'] += portfolio['TRADEDQTY']
    return portfolio 



def exposure_by_scrip(df: pd.DataFrame):
    '''
    Parameters:
    -----------
    df:
        pandas DataFrame of portfolio
    
    Returns:
    --------
    exposure: 
        dictionary with exposures of that scrip
    '''
    gross_exposure = dict({})
    net_exposure = dict({})
    options = df.loc[df['SEGMENT'].isin(['OPTIDX', 'OPTSTK'])]
    eqt = df.loc[df['SEGMENT'].isin(['EQ'])]
    futs = df.loc[df['SEGMENT'].isin(['FUTSTK', 'FUTIDX'])]
    
    n_eqt = eqt['ACTIVEQTY'].sum() if not eqt.empty else 0 # Total active cash equities in the strategy
    n_fut = futs['ACTIVEQTY'].sum() if not futs.empty else 0 # Total active Futures in the strategy
    n_rem = n_fut+n_eqt # No of future/eqyuity remaining after adjustment for exposure

    # No of quantities adjusted (futures against equities)
    n_adjfeq = min(abs(n_eqt), abs(n_fut)) if (n_fut != 0 and n_eqt != 0) and ((n_fut + n_eqt) < (abs(n_fut) + abs(n_eqt))) else 0

    # Exposure of adjusted futures and equities
    gross_exposure['feq'] = n_adjfeq * futs['CLOSE'].mean() if n_adjfeq != 0 else 0
    
    #Net Exposure for Equity and Futures
    net_exposure['fut'] = n_fut * futs['CLOSE'].mean() if n_fut != 0 else 0
    net_exposure['eq'] = n_eqt * eqt['CLOSE'].mean() if n_eqt != 0 else 0

    # If options in strategy
    if not options.empty:
        long_options = options.loc[options['ACTIVEQTY'] > 0] # Long Options (if activeqty > 0)
        short_options = options.loc[options['ACTIVEQTY'] < 0] # Short Options (if activeqty < 0)
        if not long_options.empty:
            # For Long Options Exposure is simply Quantity * Close (Premium)
            long_options['GROSSEXPOSURE'] = long_options.apply(lambda x: x['ACTIVEQTY'] * x['CLOSE'], axis=1)
            gross_exposure['long_options'] = long_options['GROSSEXPOSURE'].sum()
            net_exposure['long_options'] = gross_exposure['long_options']
        if not short_options.empty:
            short_options['NETEXPOSURE'] = short_options.apply(lambda x: abs(x['ACTIVEQTY'])*x['STRIKE'], axis=1)
            net_exposure['short_options'] = short_options['NETEXPOSURE'].sum()
            #print(short_options)
            ce = short_options.loc[short_options['OPTIONTYPE'].isin(['CE'])]
            ce = ce.sort_values(by=['STRIKE'])
            pe = short_options.loc[short_options['OPTIONTYPE'].isin(['PE'])]
            pe = pe.sort_values(by=['STRIKE'])

            # If there are short options 
            n_ce = ce['ACTIVEQTY'].sum() if not ce.empty else 0 # Number of short Calls 
            n_pe = pe['ACTIVEQTY'].sum() if not pe.empty else 0 # Number of short Puts
            #print(n_ce, n_pe)

            # If short on both call and put with more number of calls than puts
            if (n_ce != 0 and n_pe != 0) and abs(n_ce) > abs(n_pe):
                n_rcepe = abs(n_ce) - abs(n_pe)
                # Put side exposure
                pe['GROSSEXPOSURE'] = pe.apply(lambda x: abs(x['ACTIVEQTY'])*x['STRIKE'], axis=1)
                pe_exp = pe['GROSSEXPOSURE'].sum()
                ce_exp = 0
                ce_adj = 0

                # Call side exposure to adjust with Put by ascending strike 
                for row in ce.iterrows():
                    if ce_adj + abs(row['ACTIVEQTY']) < abs(n_pe):
                        ce_adj += abs(row['ACTIVEQTY'])
                        ce_exp += abs(row['ACTIVEQTY']) * row['STRIKE']
                    elif ce_adj + abs(row['ACTIVEQTY']) > abs(n_pe) and ce_adj < n_pe:
                        ce_exp += row['STRIKE'] * (n_pe - ce_exp)
                gross_exposure['ce_pe'] = max(ce_exp, pe_exp)

                ce['GROSSEXPOSURE'] = ce.apply(lambda x: abs(x['ACTIVEQTY'])*x['STRIKE'], axis=1)
                c_exp = ce['GROSSEXPOSURE'].sum() - ce_exp

                # Adjusting Remaining Futures or equities with net remainig Calls
                if n_rem != 0:
                    f_exp = abs(n_rem) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean())  
                    # If opposite direction (Long Futures/ Equities and Short Calls)
                    if n_rem > 0:
                        # If number of Futures/Equities are more than Calls, use both adjusted and remaining 
                        if abs(n_rem) > abs(n_ce) - abs(ce_adj):
                            gross_exposure['fcp'] = max(c_exp, abs(abs(n_ce)-abs(ce_adj)) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean()))
                            gross_exposure['rem'] = f_exp - (min(c_exp, abs(abs(n_ce)-abs(ce_adj)) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean())))
                        else:
                            # Maximum of Call or Futures/Equities
                            gross_exposure['fcp'] = max(f_exp, c_exp)
                    else:
                        # If in same direction Exposure for both
                        gross_exposure['fcp'] = c_exp
                        gross_exposure['rem'] = f_exp
                else:
                    # If only Calls are remaining then it will be naked Call exposure
                    gross_exposure['naked_ce'] = c_exp

            # If short on both call and put with more number of puts than calls
            elif (n_ce !=0 and n_pe != 0) and abs(n_pe) > abs(n_ce):
                n_rcepe = abs(n_pe) - abs(n_pe)
                # Call side exposure
                ce['GROSSEXPOSURE'] = pe.apply(lambda x: abs(x['ACTIVEQTY'])*x['STRIKE'], axis=1)
                ce_exp = ce['GROSSEXPOSURE'].sum()
                pe_exp = 0
                pe_adj = 0

                # Put side exposure to adjust with Call by ascending strike
                for row in pe.iterrows():
                    if pe_adj + abs(row['ACTIVEQTY']) < abs(n_ce):
                        pe_adj += abs(row['ACTIVEQTY'])
                        pe_exp += abs(row['ACTIVEQTY']) * row['STRIKE']
                    elif pe_adj + abs(row['ACTIVEQTY']) > abs(n_ce) and pe_adj < n_ce:
                        pe_exp += row['STRIKE'] * (n_ce - pe_exp)
                gross_exposure['ce_pe'] = max(pe_exp,ce_exp)

                pe['GROSSEXPOSURE'] = pe.apply(lambda x: abs(x['ACTIVEQTY'])*x['STRIKE'], axis=1)
                p_exp = pe['GROSSEXPOSURE'].sum() - pe_exp
                
                # Adjusting Remaining Futures or equities with net remainig Puts
                if n_rem != 0:
                    f_exp = abs(n_rem) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean())
                    # If opposite direction (Long Futures/ Equities and Short Puts)                    
                    if n_rem < 0:
                        # If number of Futures/Equities are more than Puts, use both adjusted and remaining 
                        if abs(n_rem) > abs(n_pe)-pe_adj:
                            gross_exposure['fcp'] = max(p_exp, abs(abs(n_pe)-abs(pe_adj)) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean()))
                            gross_exposure['rem'] = f_exp - (min(p_exp, abs(abs(n_pe)-abs(pe_adj)) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean())))
                        else:
                            # Maximum of Put or Futures/Equities
                            gross_exposure['fcp'] = max(f_exp, p_exp)
                    else:
                        # If in same direction Exposure for both
                        gross_exposure['fcp'] = p_exp
                        gross_exposure['rem'] = f_exp
                else:
                    # If only Puts are remaining then it will be naked Put exposure
                    gross_exposure['naked_pe'] = p_exp

            # When Call and Put Balance out
            elif (n_ce != 0 and n_pe != 0) and abs(n_ce) == abs(n_pe):
                n_rcepe = 0
                # Call Side exposure
                ce['GROSSEXPOSURE'] = ce.apply(lambda x: abs(x['ACTIVEQTY']) * x['STRIKE'], axis=1)
                ce_exp = ce['GROSSEXPOSURE'].sum()
                #Put Side Exposure
                pe['GROSSEXPOSURE']= pe.apply(lambda x: abs(x['ACTIVEQTY']) * x['STRIKE'], axis=1)
                pe_exp = pe['GROSSEXPOSURE'].sum()
                #Max of Call or Put Side Exposure
                gross_exposure['ce_pe'] = max(ce_exp, pe_exp)
                # Remaiming Future/Equity Exposure
                gross_exposure['rem'] = abs(n_rem)* futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean() if n_rem != 0 else 0
            # If only Puts and No Calls, Adjust Future/Equity with Puts
            elif n_ce == 0 and n_pe != 0:
                f_exp = abs(n_rem) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean()) if n_rem != 0 else 0
                pe['GROSSEXPOSURE'] = pe.apply(lambda x: abs(x['ACTIVEQTY'])*x['STRIKE'], axis=1)
                p_exp = pe['GROSSEXPOSURE'].sum()

                if n_rem < 0:
                    gross_exposure['fcp'] = max(f_exp, p_exp)
                else:
                    gross_exposure['ce_pe'] = p_exp
                    gross_exposure['rem'] = f_exp
            # If only Calls and No Puts, Adjust Future/Equity with Calls    
            elif n_pe == 0 and n_ce != 0:
                f_exp = abs(n_rem) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean()) if n_rem != 0 else 0
                ce['GROSSEXPOSURE'] = ce.apply(lambda x: abs(x['ACTIVEQTY'])*x['STRIKE'], axis=1)
                c_exp = ce['GROSSEXPOSURE'].sum()
                if n_rem > 0:
                    gross_exposure['fcp'] = max(f_exp, c_exp)
                else:
                    gross_exposure['ce_pe'] = c_exp
                    gross_exposure['rem'] = f_exp
            else:
                gross_exposure['ce_pe'] = 0
                gross_exposure['fcp'] = 0
                gross_exposure['rem'] = abs(n_rem) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean())
    else:
        # If There is no Long/Short Option Find exposure for Future/Equity
        if n_rem != 0:
            gross_exposure['rem'] = abs(n_rem) * (futs['CLOSE'].mean() if n_fut != 0 else eqt['CLOSE'].mean())
    exposure = dict({'gross' : 0, 'net' : 0})
    for k,v in gross_exposure.items():
        exposure['gross'] += v 
    for k,v in net_exposure.items():
        exposure['net'] += v
    return exposure


def exposure_by_stategy(df: pd.DataFrame):
    '''
    Parameters:
    -----------
    df:
        pandas DataFrame of portfolio
    
    Returns:
    --------
    exposure_df: 
        pandas DataFrame of exposure of each strategy
    '''
    gross_exposure = dict({})
    net_exposure = dict({})
    strategies = list(df['STRATEGYID'].unique())
    # print(strategies)
    for startegy in strategies:
        gross_exposure[startegy] = 0
        net_exposure[startegy] = 0
        temp = df.loc[df['STRATEGYID'] == startegy]
        scripts = list(temp['SYMBOL'].unique())

        for scrip in scripts:
            expo_dict = exposure_by_scrip(temp.loc[temp['SYMBOL'] == scrip])
            print(startegy,expo_dict)            
            gross_exposure[startegy] += expo_dict['gross']
            net_exposure[startegy] += expo_dict['net']  
    print(gross_exposure, net_exposure)

  

def CalculateGreeks(df, SpotPriceDF):
    df.OptionType = [it.replace('CA', 'CE').replace('PA', 'PE') for it in df.OptionType]
    df.Ticker = df.Scrip + df.Expiry + df.OptionType + df.Strike.apply(getStrikeString)
    df.index = df.Ticker
    print(len(df.Ticker.unique()))
    df['Date'] = df['Date'].astype(str)
    for iField in ['IV', 'Delta', 'Delta2', 'Rho', 'Theta', 'Vega', 'Gamma']:
        df[iField] = len(df.index)*[np.NAN]
        
    for iIndex in df.index:
        if df.loc[iIndex, 'Instrument'] in ['OPTIDX', 'OPTSTK']:
            tempdf = df.loc[iIndex]
            expiryDate = datetime.datetime.strptime(tempdf.loc['Expiry'], '%d%b%y')
            curDate = datetime.datetime.strptime(tempdf.loc['Date'], '%Y-%m-%d')
            futTicker = "".join(tempdf.loc[['Scrip', 'Expiry']])+'XX0'
            daysToExpiry = (expiryDate - curDate).days
            optionType = tempdf.loc['OptionType']
            if futTicker in df.index:
                futPrice = df.loc[futTicker, 'Close']
                intRate = 0
            else:
                try:
                    futPrice = SpotPriceDF.loc[curDate, tempdf.Scrip]
                    intRate = 6 # 6 %is taken Risk free rate
                except:
                    continue
            
            
            if optionType == 'CE':
                c = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], callPrice = tempdf.loc['Close'])
                opt = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], volatility  = c.impliedVolatility)
                df.loc[iIndex, 'Delta'] = opt.callDelta
                df.loc[iIndex, 'Delta2']  = opt.callDelta2
                df.loc[iIndex, 'Rho'] = opt.callRho
                df.loc[iIndex, 'Theta'] = opt.callTheta
            elif optionType == 'PE':
                p = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], putPrice = tempdf.loc['Close'])
                opt = mibian.BS([futPrice, tempdf.loc['Strike'], intRate, daysToExpiry], volatility  = p.impliedVolatility)
                df.loc[iIndex, 'Delta'] = opt.putDelta
                df.loc[iIndex, 'Delta2']  = opt.putDelta2
                df.loc[iIndex, 'Rho'] = opt.putRho
                df.loc[iIndex, 'Theta'] = opt.putTheta
            
            try:
                df.loc[iIndex, 'IV'] = opt.volatility
            except:
                pass
            df.loc[iIndex, 'Vega'] = opt.vega
            df.loc[iIndex, 'Gamma'] = opt.gamma
    return df

def calculate_greeks(df: pd.DataFrame, SpotPriceDF: pd.DataFrame):
    """
    Calls CalculateGreek from Utils for each day NSEFNO BhavCopy
    Parameters:
    -----------
    df:
        Pandas DataFrame (one day NSE FNO BhavCopy)
    SpotPriceDF:
        pandas DataFrame (Historical Spot Price data for Index Futures)
    Returns:
    --------
    greek_df: Pandas DataFrame (Greeks for OPTIONS)
    """
    df.OptionType = [it.replace('CA', 'CE').replace('PA', 'PE') for it in df.OptionType]
    df.Ticker = df.Scrip + df.Expiry + df.OptionType + df.Strike.apply(getStrikeString)
    df.index = df.Ticker
    df['Date'] = df['Date'].astype(str)
    for iField in ['IV', 'Delta', 'Rho', 'Theta', 'Vega', 'Gamma']:
        df[iField] = len(df.index)*[np.nan] ## changed to nan from NAN
        
    for iIndex in df.index:
        if df.loc[iIndex, 'Instrument'] in ['OPTIDX', 'OPTSTK']:
            tempdf = df.loc[iIndex]
            expiryDate = datetime.datetime.strptime(tempdf.loc['Expiry'], '%d%b%y')
            curDate = datetime.datetime.strptime(tempdf.loc['Date'], '%Y-%m-%d')
            futTicker = "".join(tempdf.loc[['Scrip', 'Expiry']])+'XX0'
            daysToExpiry = (expiryDate - curDate).days
            optionType = tempdf.loc['OptionType']
            if futTicker in df.index:
                futPrice = df.loc[futTicker, 'Close']
                intRate = 0
            else:
                """FOR INDEX FUTURES (CLOSE IS NOT AVAILABLE IN BHAVCOPY) """
                try:
                    futPrice = SpotPriceDF.loc[curDate, tempdf.Scrip]
                    intRate = 0.065 # 6 %is taken Risk free rate
                except:
                    continue
                
            price = tempdf.loc['Close']
            S = futPrice
            K = float(tempdf.loc['Strike'])
            t = (daysToExpiry/365)
            r_ = intRate
            flag = 'c' if optionType == 'CE' else 'p'
            try:
                iv = py_vollib.black_scholes_merton.implied_volatility.implied_volatility(price, S, K, t, r_,0,flag)
            except Exception as e:
                try:
                    price = tempdf.loc['SettlePrice']
                    iv = py_vollib.black_scholes_merton.implied_volatility.implied_volatility(price, S, K, t, r_,0,flag)
                    #opt = mibian.BS([S, K, 100*r_, daysToExpiry], callPrice = price) if flag == 'c' else mibian.BS([S, K, 100*r_, daysToExpiry], putPrice = price)
                    #iv = opt.impliedVolatility
                except Exception as p:
                    iv = 1e-2
            delta = py_vollib.black_scholes.greeks.numerical.delta(flag, S, K, t, r_, iv)
            gamma = py_vollib.black_scholes.greeks.numerical.gamma(flag, S, K, t, r_, iv)
            theta = py_vollib.black_scholes.greeks.numerical.theta(flag, S, K, t, r_, iv)
            rho = py_vollib.black_scholes.greeks.numerical.rho(flag, S, K, t, r_, iv)
            vega = py_vollib.black_scholes.greeks.numerical.vega(flag, S, K, t, r_, iv)

            df.loc[iIndex, 'IV'] = iv
            df.loc[iIndex, 'Delta'] = delta if iv > 1e-5 else 0.0
            df.loc[iIndex, 'Rho'] = rho
            df.loc[iIndex, 'Theta'] = theta
            df.loc[iIndex, 'Vega'] = vega
            df.loc[iIndex, 'Gamma'] = gamma
            
    return df