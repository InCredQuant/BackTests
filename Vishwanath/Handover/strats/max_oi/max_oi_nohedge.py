# This is the last updated version of max oi strategy as on aug-2024
# This version should be used until new revision comes

import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime, time, date
import os
import sys
sys.path.insert(1, r'G:\My Drive\workspace\strats')
from strats.commons.framework_base import DataOps, BaseStrategy
from strategies.commons.order_base import Order, Position, OptionType, Segment

def get_opt_strike(df, expiry, current_date):
    try:
        df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        frame = df.loc[df['ExpiryDate'] == expiry]#expiry.strftime('%d%b%y').upper()]
        frame = frame.sort_values(by='DateTime').groupby(['Ticker']).last()
        ce_df = frame.loc[frame['Call_Or_Put'] == 'CE']
        ce_oi = ce_df['Open Interest'].sum()
        pe_df = frame.loc[frame['Call_Or_Put'] == 'PE']
        pe_oi = pe_df['Open Interest'].sum()
        if ce_oi > pe_oi:
            strike = ce_df.loc[ce_df['Open Interest'].idxmax()]['StrikePrice']
            option_type = 'CE'
        else:
            strike = pe_df.loc[pe_df['Open Interest'].idxmax()]['StrikePrice']
            option_type = 'PE'
        return strike, option_type
    except Exception as e:
        print(current_date, e)

class MaxOi(BaseStrategy):

    def init_vars(self):
        self.short_ticker = None
        self.hedge_ticker = None
        self.pos_flag = False
        self.is_pos = False
        self.previous_ticker = None
        self.previous_expiry = None
        self.sl_px = None
        self.output_data = []
        #hedge
        self.previous_hedge = None
        self.premium_percent = 0.15
        self.stoploss_percent = 1.25
        self.min_premium_value = 50



    def get_hedge(self, df, expiry, option_type, premium):
        df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        frame = df.loc[df['ExpiryDate'] == expiry]  # expiry.strftime('%d%b%y').upper()]
        frame = frame.sort_values(by='DateTime').groupby(['Ticker']).last()
        opt_df = frame.loc[frame['Call_Or_Put'] == option_type]
        #hedge_frame = opt_df.iloc[(opt_df['Close']-premium).abs().values.argsort()[:-1]]
        hedge_val = premium*self.premium_percent
        hedge_frame = opt_df[opt_df['Close']<=hedge_val].sort_values(by='Close').tail(1)
        hedge_ticker = hedge_frame.index.values[0]
        hedge_px = hedge_frame['Close'].values[0]
        #print(hedge_ticker, hedge_px)
        return hedge_ticker, hedge_px


    def update(self): # without hedge
        data_obj = DataOps(self.current_data, None)
        data_instance = data_obj.get_data_slice(self.current_date, time(9, 14, 59), time(15, 14, 59))
        data_instance_df = data_instance.to_pandas()
        strike, option_type = get_opt_strike(data_instance_df, self.current_month_expiry__, self.current_date)
        self.short_ticker = self.get_ticker(strike, self.current_month_expiry_, option_type)
        if self.pos_flag:
            # init data
            mid_df = data_obj.get_data_range(self.previous_ticker, self.current_date, time(9, 14, 59), time(15, 14, 59))
            #hedge_df = data_obj.get_data_range(self.hedge_ticker, self.current_date, time(9, 14, 59), time(15, 14, 59))
            # open price impact on sl
            open_df = mid_df.sort('Time').head()[0]
            open_df = open_df.select('Open')
            open_px = open_df.item()  # open high px
            # hedge open px
            #hedge_open_df = hedge_df.sort('Time').head()[0]
            #hedge_open_df = hedge_open_df.select('Open')
            # try:
            #     hedge_open_px = hedge_open_df.item()  # open high px
            # except:
            #     hedge_open_px = 0
            hedge_open_px = None
            if (open_px >= self.sl_px) and self.is_pos:
                #print(f'SL triggered on open {self.current_date}')
                print(f'Exit SL open Date:{self.current_date} SL:{self.sl_px} OpenPx:{open_px}')
                self.is_pos = False
                self.output_data.append([self.current_date, self.previous_ticker, open_px, self.sl_px,self.hedge_ticker, hedge_open_px, 'SL Open'])

            # sl triggered for the day

            high_px = mid_df.select('High').max().item()
            high_px_time = mid_df.filter(pl.col('High') == pl.col('High').max()).select(pl.last('Time')).item()
            hedge_px = None#hedge_df.filter(pl.col('Time') <= high_px_time).sort(by='Time').select(pl.last('Close')).item()
            if (high_px >= self.sl_px) and self.is_pos:
                #print(f'SL triggered {self.current_date}')
                print(f'Exit SL day Date:{self.current_date} SL:{self.sl_px} HighPx:{high_px}')
                self.is_pos = False
                self.output_data.append([self.current_date, self.previous_ticker, self.sl_px, self.sl_px,self.hedge_ticker, hedge_px,'SL Day ('+str(high_px)+' )'])

            # ticker change
            if self.short_ticker != self.previous_ticker:
                if self.is_pos:
                    #print(f'Exit due to ticker change {self.current_date}')
                    print(f'Exit Ticker change Date:{self.current_date} PreviousTicker:{self.previous_ticker} NewTicker:{self.short_ticker}')
                    self.is_pos = False
                    px = data_obj.get_value(self.previous_ticker, self.current_date, time(15, 14, 59))
                    hedge_px = None#hedge_df.sort(by='Time').select(pl.last('Close')).item()
                    self.output_data.append([self.current_date, self.previous_ticker, px, self.sl_px, self.hedge_ticker, hedge_px, 'Ticker Change'])
                self.pos_flag = False

            # 1-day before expiry
            if self.previous_expiry != self.current_month_expiry_:
                if self.is_pos:
                    #print(f'Exit due to expiry change {self.current_date}')
                    print(f'Exit Expiry change Date:{self.current_date} PreviousExpiry:{self.previous_expiry} NewExpiry:{self.current_month_expiry_}')
                    self.is_pos = False
                    px = data_obj.get_value(self.previous_ticker, self.current_date, time(15, 14, 59))
                    hedge_px = None#hedge_df.sort(by='Time').select(pl.last('Close')).item()
                    self.output_data.append([self.current_date, self.previous_ticker, px, self.sl_px, self.hedge_ticker, hedge_px, 'Expiry Change'])
                self.pos_flag = False

            if self.is_pos:
                try:
                    vwap_px = data_obj.get_vwap_ver2(self.previous_ticker, self.current_date)
                except:
                    vwap_px = data_obj.get_value(self.previous_ticker, self.current_date, time(15, 29, 59))

                # try:
                #     hedge_vwap = data_obj.get_vwap_ver2(self.hedge_ticker, self.current_date)
                # except:
                #     hedge_vwap = data_obj.get_value(self.hedge_ticker, self.current_date, time(15, 29, 59))
                hedge_vwap = None
                print(f'Update SL Date:{self.current_date} PreviousSL:{self.sl_px} NewSL:{min(self.sl_px, vwap_px*self.stoploss_percent)}')
                self.sl_px = min(self.sl_px, vwap_px * self.stoploss_percent)
                self.output_data.append([self.current_date, self.previous_ticker, vwap_px, self.sl_px, self.hedge_ticker, hedge_vwap,'SL Update'])
                #print(f'Update sl {self.current_date}')


        if not self.pos_flag:
            short_ticker_px = data_obj.get_value(self.short_ticker, self.current_date, time(15, 14, 59))
            if short_ticker_px >= self.min_premium_value:
                # self.sl_px = short_ticker_px * 1.5
                vwap_px = data_obj.get_vwap_ver2(self.short_ticker, self.current_date)
                self.sl_px = min(short_ticker_px, vwap_px) * self.stoploss_percent
                self.pos_flag = True
                self.is_pos = True
                #print("Entry Taking_pos", self.current_date, self.short_ticker, short_ticker_px, self.sl_px)
                print(f'Entry Date:{self.current_date} ShortTicker:{self.short_ticker} Price:{short_ticker_px} SL:{self.sl_px}')
                self.previous_ticker = self.short_ticker
                self.previous_expiry = self.current_month_expiry_
                self.hedge_ticker, hedge_px = None, None#self.get_hedge(data_instance_df, self.current_month_expiry__, option_type, short_ticker_px)
                self.output_data.append([self.current_date, self.short_ticker, short_ticker_px, self.sl_px, self.hedge_ticker, hedge_px, 'Entry'])
            else:
                print(f'Bypassing Ticker Date:{self.current_date} ShortTicker:{self.short_ticker} BypassingPx:{short_ticker_px}')


    def exit_process(self):
        cols = ['Date', 'Ticker', 'Price','SL','HedgeTicker','HedgePx','Reason']
        output_df = pd.DataFrame(self.output_data, columns=cols)
        output_df.to_excel('./output//'+'bnifty_mxoi_montly'+str(self.stoploss_percent)+'.xlsx')

def main():
    symbol = 'banknifty'
    start = '2024-9-1'
    end = '2024-11-19'
    strategy_obj = MaxOi(symbol, start, end)
    strategy_obj.run()


if __name__ == '__main__':
    main()