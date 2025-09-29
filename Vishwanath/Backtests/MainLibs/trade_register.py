import pandas as pd
import numpy as np
from order_base import Order


class TradeRegister:

    def __init__(self):
        self._trades = None
        self._order_instance = None
        self._create_register()

    def _create_register(self):
        self._cols = [
            'STRATEGY_ID',
            'SEGMENT',
            'ORDER_ID',
            'GROUP_ID',
            'SYMBOL',
            'POSITION',
            'ENTRY_DATE',
            'ENTRY_PRICE',
            'EXIT_DATE',
            'EXIT_PRICE',
            'QUANTITY',
            'EXPIRY_DATE',
            'OPTION_TYPE',
            'STRIKE_PRICE',
        ]
        self._trade_register = []

    def _format_trade(self):
        # format order object before appending to trade registry
        # process order instance attributes to generate dictionary as per requirement
        order = {v[0][1:].upper(): v[1] for v in self._order_instance.items()}
        self._trade_register.append(order)

    def _create_pnl(self):
        self._trades['PNL'] = np.where(self._trades['POSITION'] == 'LONG',
                                        (self._trades['EXIT_PRICE'] - self._trades['ENTRY_PRICE']) * self._trades['QUANTITY'],
                                        (self._trades['ENTRY_PRICE'] - self._trades['EXIT_PRICE']) * self._trades['QUANTITY'])
        self._trades['RETURN'] = np.where(self._trades['POSITION'] == 'LONG',
                                        self._trades['EXIT_PRICE'] / self._trades['ENTRY_PRICE'] - 1,
                                        self._trades['ENTRY_PRICE'] / self._trades['EXIT_PRICE'] - 1)
        self._trades['RETURN'] = round((self._trades['RETURN'] * 100),2)

    def append_trade(self, order_instance):
        if not isinstance(order_instance, Order):
            raise ValueError('Invalid order instance value for trade register.')
        self._order_instance = vars(order_instance)
        self._format_trade()

    def get_trade_register(self):
        self._trades = pd.DataFrame(self._trade_register, columns=self._cols)
        self._create_pnl()
        return self._trades


def main():
    # testing
    order_obj = Order()
    order_obj.symbol = "NIFTY"
    trade_reg = TradeRegister()
    trade_reg.append_trade(order_obj)
    df = trade_reg.get_trade_register()
    print(df)


if __name__ == '__main__':
    main()
