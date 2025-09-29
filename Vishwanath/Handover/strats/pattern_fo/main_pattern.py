import sys
sys.path.insert(1, r'G:\My Drive\workspace\strats\commons')
from strats.commons.order_base import Order, Position, OptionType, Segment
from strats.commons.trade_register import TradeRegister
from strats.commons.stats import Stats, Filter
from datetime import date, datetime, timedelta
import pandas as pd



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

def main():
    symbol = 'NIFTY'
    start_date = datetime(2010,1,1)
    end_date = datetime(2024,11,25)
    df = pd.read_excel('PATTERN_INPUT.xlsx', sheet_name=symbol, index_col=0)
    
    df.dropna(inplace=True)
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d")
    df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
    trade_reg = TradeRegister()
    entry_flag = False
    pos = 0
    order_obj = None
    backtest_name = symbol+'_PATTERN'
    qty_map = {'NIFTY':1, 'BANKNIFTY':1}#{'NIFTY':50, 'BANKNIFTY':25}
    all_data = []
    current_px = 0
    for idx in range(1, len(df)):
        current_px = df.iloc[idx]['Close']
        current_dt = df.iloc[idx].name

        #print(df.iloc[idx].name)
        if entry_flag:
            if (pos == 1) & (df.iloc[idx]['Low'] <= df.iloc[idx]['S']): # long exit
                exit_order(order_obj, df.iloc[idx]['S'], df.iloc[idx].name, trade_reg)
                entry_flag = False
                pos = 0
                order_obj = None
                current_px = df.iloc[idx]['S']
                print('Long exit on {}'.format(df.iloc[idx].name))
            if (pos == -1) & (df.iloc[idx]['High'] >= df.iloc[idx]['B']): # short exit
                exit_order(order_obj, df.iloc[idx]['B'], df.iloc[idx].name, trade_reg)
                entry_flag = False
                pos = 0
                order_obj = None
                current_px = df.iloc[idx]['B']
                print('Short exit on {}'.format(df.iloc[idx].name))

        if not entry_flag:
            if df.iloc[idx]['TW'] == 1: # trading window
                if (pos == 0) & (df.iloc[idx]['High'] > df.iloc[idx]['B']): # buy triggered
                    order_obj = entry_order(symbol,Position.LONG,qty_map[symbol],df.iloc[idx]['B'],df.iloc[idx].name)
                    entry_flag = True
                    pos = 1
                    current_px = df.iloc[idx]['B']
                    print('Long on {}'.format(df.iloc[idx].name))
                if (pos == 0) & (df.iloc[idx]['Low'] < df.iloc[idx]['S']):
                    order_obj = entry_order(symbol, Position.SHORT, qty_map[symbol], df.iloc[idx]['S'], df.iloc[idx].name)
                    entry_flag = True
                    pos = -1
                    current_px = df.iloc[idx]['S']
                    print('Short on {}'.format(df.iloc[idx].name))

        all_data.append((current_dt,current_px,pos))

    writer = pd.ExcelWriter('./output//'+symbol+'_ALL_STATS.xlsx')

    trades = trade_reg.get_trade_register()
    trades['RETURN'] = trades['PNL']/(trades['ENTRY_PRICE']*trades['QUANTITY'])
    #trades.to_excel(backtest_name+"_TRADES.xlsx")
    trades.to_excel(writer, sheet_name='TRADES')

    daily_df = pd.DataFrame(all_data, columns=['Date','Px','Pos'])
    #daily_df.to_excel(backtest_name+"_DAILY.xlsx")
    daily_df.to_excel(writer, sheet_name='DAILY')

    stats_obj = Stats(trades)
    stats_df = stats_obj.create_stats()
    stats_df.to_excel(writer, sheet_name='STATS')

    #writer.save()
    writer.close()


if __name__ == '__main__':
    main()