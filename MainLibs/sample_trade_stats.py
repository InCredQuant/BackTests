from order_base import Order, Position, OptionType, Segment
from trade_register import TradeRegister
from datetime import datetime
from stats import Stats, Filter

def main():
    from random import randint, choice
    trade_reg = TradeRegister()
    for i in range(100):
        order_obj = Order()
        order_obj.segment = Segment.OP
        order_obj.order_id = randint(1000,2000)
        order_obj.group_id = randint(100,150)
        order_obj.symbol = choice(['NIFTY','BANKNIFTY'])
        order_obj.position = choice([Position.LONG, Position.SHORT])
        order_obj.entry_date = datetime(2022,1,1)
        order_obj.entry_price = randint(100,200)
        order_obj.exit_date = datetime(2022,1,27)
        order_obj.exit_price = randint(100,200)
        order_obj.quantity = 50
        order_obj.expiry_date = datetime(2022,1,27)
        order_obj.option_type = choice([OptionType.CE, OptionType.PE])
        order_obj.strike_price = randint(170,175)*100
        trade_reg.append_trade(order_obj)
        print('Order ID:{} created.'.format(order_obj.order_id))
    df = trade_reg.get_trade_register()
    print(df)
    df.to_excel('test_trade_reg.xlsx')

    stats_obj = Stats(df)
    #df = stats_obj.create_stats()
    test_df = stats_obj.create_stats(filter_trades=True, filter_by=('POSITION','LONG'))
    print(test_df)


if __name__ == '__main__':
    main()

