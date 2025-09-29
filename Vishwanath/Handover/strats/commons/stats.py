import pandas as pd
from enum import Enum


class Filter(Enum):
    STRATEGY_ID = 1
    SEGMENT = 2
    SYMBOL = 3
    POSITION = 4


class Stats():
    """
    Generate strategy statistics.
    """

    def __init__(self, trade_register):
        self.trade_register = trade_register
        self._cw = 0  # consecutive wins
        self._cl = 0  # consecutive loss

    def _get_filter_trades(self, all_filter_frames, filter_by):
        # filters trades based on input criteria
        if not isinstance(filter_by, Filter):
            raise ValueError('Invalid filter value for strategy statistics.')
        filter_name = filter_by.name
        filter_trades = self.trade_register.copy()
        if filter_name not in filter_trades.columns:
            raise ValueError('Filter key does not exist in the trade register column.')
        unique_vals = filter_trades[filter_name].unique()
        unique_vals.sort()
        for value in unique_vals:
            all_filter_frames[value] = filter_trades[filter_trades[filter_name] == value]

    def create_stats(self, filter_trades=False, filter_by=None):
        # trade register specific statistics
        all_filter_frames = {'TOTAL': self.trade_register.copy()}
        if filter_by:
            self._get_filter_trades(all_filter_frames,
                                    filter_by)  # Returns dict containing key as unique filter column values and respective filtered dataframe
        stats_cont = {}  # stats container
        for value in all_filter_frames:
            stats_cont[value] = self._fill_stats(all_filter_frames[value], value)
            if len(all_filter_frames) == 2:  # if after filtering, only one unique value remains
                break
        trade_stats_frame = pd.concat([stats_cont[v] for v in stats_cont], axis=1)
        return trade_stats_frame

    def _consec_win(self, value):
        # consecutive win streak
        if value > 0:
            self._cw += 1
        else:
            self._cw = 0
        return self._cw

    def _consec_loss(self, value):
        # consecutive loss streak
        if value < 0:
            self._cl += 1
        else:
            self._cl = 0
        return self._cl

    def _fill_stats(self, filter_trades_register, filter_by):
        trade_stats = {}
        self._cw = 0
        self._cl = 0
        trade_stats['Largest Win Trade'] = filter_trades_register['PNL'].max()
        trade_stats['Largest Win Trade(%)'] = filter_trades_register['RETURN'].max()
        trade_stats['Largest Loss Trade'] = filter_trades_register['PNL'].min()
        trade_stats['Largest Loss Trade(%)'] = filter_trades_register['RETURN'].min()
        trade_stats['Average Trade Return'] = filter_trades_register['RETURN'].mean()
        trade_stats['Median Trade Return'] = filter_trades_register['RETURN'].median()
        trade_stats['Trade Return Stdev'] = filter_trades_register['RETURN'].std()
        trade_stats['Winning Trades'] = filter_trades_register[filter_trades_register['PNL'] > 0]['PNL'].count()
        trade_stats['Losing Trades'] = filter_trades_register[filter_trades_register['PNL'] < 0]['PNL'].count()
        trade_stats['Win Loss Ratio'] = trade_stats['Winning Trades']/trade_stats['Losing Trades']
        trade_stats['Total Trades'] = trade_stats['Winning Trades'] + trade_stats['Losing Trades']
        trade_stats['Consec Win'] = filter_trades_register['PNL'].apply(self._consec_win).max()
        trade_stats['Consec Loss'] = filter_trades_register['PNL'].apply(self._consec_loss).max()
        trade_stats['Win(%)'] = (trade_stats['Winning Trades'] / trade_stats['Total Trades']) * 100
        trade_stats['Loss(%)'] = 100 - trade_stats['Win(%)']
        trade_stats['Average Win'] = filter_trades_register[filter_trades_register['PNL'] > 0]['PNL'].mean()
        trade_stats['Average Win(%)'] = filter_trades_register[filter_trades_register['RETURN'] > 0]['RETURN'].mean()
        trade_stats['Average Loss'] = filter_trades_register[filter_trades_register['PNL'] < 0]['PNL'].mean()
        trade_stats['Average Loss(%)'] = filter_trades_register[filter_trades_register['RETURN'] < 0]['RETURN'].mean()
        trade_stats['Gross Profit'] = filter_trades_register[filter_trades_register['PNL'] > 0]['PNL'].sum()
        trade_stats['Gross Profit(%)'] = filter_trades_register[filter_trades_register['RETURN'] > 0]['RETURN'].sum()
        trade_stats['Gross Loss'] = filter_trades_register[filter_trades_register['PNL'] < 0]['PNL'].sum()
        trade_stats['Gross Loss(%)'] = filter_trades_register[filter_trades_register['RETURN'] < 0]['RETURN'].sum()
        trade_stats['Gross Net'] = trade_stats['Gross Profit'] - abs(trade_stats['Gross Loss'])
        trade_stats['Gross Net(%)'] = trade_stats['Gross Profit(%)'] - abs(trade_stats['Gross Loss(%)'])
        trade_stats['Profit Factor'] = trade_stats['Gross Profit'] / abs(trade_stats['Gross Loss'])
        trade_stats['Payoff Ratio'] = trade_stats['Average Win'] / abs(trade_stats['Average Loss'])
        # final stats frame
        trade_stats_frame = pd.DataFrame().from_dict(trade_stats, orient='index')
        trade_stats_frame.index.name = 'Statistics'
        trade_stats_frame.columns = [filter_by]
        #trade_stats_frame[filter_by] = round(trade_stats_frame[filter_by], 2)
        return trade_stats_frame
