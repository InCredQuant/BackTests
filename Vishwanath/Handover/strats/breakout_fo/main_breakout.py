import sys
sys.path.insert(1, r'G:\My Drive\workspace\strats\commons')
from strats.commons.order_base import Order, Position, OptionType, Segment
from strats.commons.trade_register import TradeRegister
from strats.commons.stats import Stats, Filter
from datetime import date, datetime, timedelta
import pandas as pd
from strats.commons.monthly_weekly import get_return_matrix

def entry_order(symbol, pos, qty, px, dt):
    order_obj = Order()
    order_obj.segment = Segment.FT
    order_obj.symbol = symbol
    order_obj.entry_price = px
    order_obj.quantity = qty
    order_obj.entry_date = dt
    order_obj.position = pos
    return order_obj

def exit_order(order_obj, px, dt, trade_reg):
    order_obj.exit_date = dt
    order_obj.exit_price = px
    trade_reg.append_trade(order_obj)

def get_data_bar(df,idx,val=None):
    index_dt = df.iloc[idx].name
    open = df.iloc[idx]['Open']
    high = df.iloc[idx]['High']
    low = df.iloc[idx]['Low']
    close = df.iloc[idx]['Close']
    if not val:
        return (open, high, low, close)
    else:
        return df.iloc[idx][val]

def main():
    # backtest dates

    start_date = datetime(2010,1,1) #datetime(2010, 1, 1)
    end_date = datetime(2024,11,25)
    symbol = 'NIFTY'
    backtest_name = symbol+'_BKT_REVISED'
    qty_map = {'NIFTY':1, 'BANKNIFTY':1} #{'NIFTY':50, 'BANKNIFTY':25}
    df = pd.read_excel('BREAKOUT_INPUT.xlsx', sheet_name=symbol+'_INP', index_col=0)
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d")
    df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
    df = df[['Open','High','Low','Close']]
    sl = 0  # stop loss
    pos = 0
    entry_flag = False
    trade_reg = TradeRegister()
    order_obj = None
    long_at = 0
    short_at = 0
    prev_sl = None
    all_data = []
    for idx in range(2,len(df)):
        current_dt = df.iloc[idx].name
        current_px = df.iloc[idx]['Close']
        if current_dt == datetime(2021,4,9):
            print('Debug')
        # flags
        o2,h2,l2,c2 = get_data_bar(df, idx-2)    # previous two day ohlc
        o1, h1, l1, c1 = get_data_bar(df, idx-1) # previous day ohlc
        o, h, l, c = get_data_bar(df, idx)       # current day ohlc
        tw = o1 > c2                    # trading window
        tcw = o > c1                    # trading close window
        buy_level = o + (c1 - l1)       # buy level
        sell_level = o - (h1 - c1)      # sell level
        if entry_flag:
            if pos == 1: # existing long position
                sl = max(max(long_at, o, c1) * 0.97,prev_sl)
                if (not tcw) and (l < sell_level) and (sell_level > sl):
                    if o < sell_level:
                        exit_order(order_obj, o, current_dt, trade_reg)  # long exit
                        current_px = o
                        # all_data.append((current_dt,o,h, l,c,buy_level,sell_level,current_px, pos, sl))
                        entry_flag = False
                        pos = 0
                        all_data.append((current_dt, pos, o))
                    else:
                        exit_order(order_obj, sell_level, current_dt, trade_reg)  # long exit
                        current_px = sell_level
                        # all_data.append((current_dt,o,h, l,c,buy_level,sell_level,current_px, pos, sl))
                        entry_flag = False
                        pos = 0
                        all_data.append((current_dt, pos, sell_level))
                elif l < sl: # open gaps down/price goes down below sell level
                    if o < sl:
                        exit_order(order_obj, o, current_dt, trade_reg)  # long exit
                        pos = 0
                        all_data.append((current_dt, pos, sl))
                        current_px = o
                    else:
                        exit_order(order_obj, sl, current_dt, trade_reg)  # long exit
                        pos = 0
                        all_data.append((current_dt, pos, sl))
                        current_px = sl
                    # all_data.append((current_dt,o,h, l,c,buy_level,sell_level,current_px, pos, sl))
                    entry_flag = False
                    sl = 0
                else:
                    all_data.append((current_dt, pos, current_px))

            if pos == -1: # existing short position
                sl = min(min(short_at,o,c1) * 1.03, prev_sl)
                if tcw and (h > buy_level) and (buy_level < sl):
                    if o > buy_level:
                        exit_order(order_obj, o, current_dt, trade_reg)  # long exit
                        current_px = o
                        # all_data.append((current_dt,o,h, l,c,buy_level,sell_level,current_px, pos, sl))
                        entry_flag = False
                        pos = 0
                        all_data.append((current_dt, pos, o))
                    else:
                        exit_order(order_obj, buy_level, current_dt, trade_reg)  # long exit
                        current_px = buy_level
                        # all_data.append((current_dt,o,h, l,c,buy_level,sell_level,current_px, pos, sl))
                        entry_flag = False
                        pos = 0
                        all_data.append((current_dt, pos, buy_level))
                    sl = 0
                elif h > sl:
                    if o > sl:
                        exit_order(order_obj,o,current_dt,trade_reg) # short exit
                        current_px = sl
                        pos = 0
                        all_data.append((current_dt, pos, o))
                        # all_data.append((current_dt,o,h, l,c,buy_level,sell_level,current_px, pos, sl))
                    else:
                        exit_order(order_obj, sl, current_dt, trade_reg)  # short exit
                        current_px = sl
                        pos = 0
                        all_data.append((current_dt, pos, sl))
                    entry_flag = False
                    sl = 0
                else:
                    all_data.append((current_dt, pos, current_px))
            else:
                all_data.append((current_dt, pos, current_px))

        # entry criteria for new positions
        if not entry_flag:
            if tw & (h > buy_level):
                order_obj = entry_order(symbol, Position.LONG, qty_map[symbol], buy_level, current_dt)
                entry_flag = True
                pos = 1
                long_at = buy_level
                print('>>Long Entry on {} at {}'.format(current_dt, buy_level))
                sl = long_at * 0.97
                all_data.append((current_dt, pos, long_at))
            elif (not tw) & (l < sell_level):
                order_obj = entry_order(symbol, Position.SHORT, qty_map[symbol], sell_level, current_dt)
                entry_flag = True
                pos = -1
                short_at = sell_level
                print('>>Short Entry on {} at {}'.format(current_dt, sell_level))
                sl = short_at * 1.03
                all_data.append((current_dt, pos, short_at))
            else:
                all_data.append((current_dt, pos, current_px))
        prev_sl = sl


    outpath = './output//'
    trades = trade_reg.get_trade_register()
    trades.to_excel(outpath+backtest_name+'_TRADES.xlsx')

    # daily_df = pd.DataFrame(all_data, columns=['Date','Open','High','Low','Close','BL','SL','Px','Pos','Sl'])
    daily_df = pd.DataFrame(all_data, columns=['Date', 'POS', 'PX'])
    daily_df.to_excel(outpath+backtest_name+"_DAILY.xlsx")

    stats_obj = Stats(trades)
    stats_df = stats_obj.create_stats(filter_by=Filter.POSITION)
    stats_df.to_excel(outpath+backtest_name + "_STATS.xlsx")


if __name__ == '__main__':
    main()

