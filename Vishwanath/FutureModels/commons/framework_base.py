
import os
import numpy as np
import polars as pl
from datetime import date, time, datetime, timedelta
import sys
sys.path.insert(1,r'G:\My Drive\workspace\strats\commons')
from abc import ABC, abstractmethod
from tqdm import tqdm


class DataOps:

    def __init__(self, data, spot_data):
        self.data = data
        self.spot_data = spot_data
        self.data = self.data.with_columns(pl.col('Time').str.strptime(pl.Time))

    def get_tranformed_data(self):
        return self.data

    def get_spot_close(self, dt_value):
        px = self.spot_data.filter(pl.col('date') == dt_value).select('close').item()
        return px


    def get_value(self, ticker: str, date_value: date, time_value: time, col_name: str = 'Close'):
        ticker_filter = pl.col('Ticker') == ticker
        date_filter = pl.col('Date') == date_value
        time_filter = pl.col('Time') == time_value
        px_frame = self.data.filter(ticker_filter & date_filter & time_filter).select(col_name)
        if px_frame.is_empty():
            px_frame = self.get_data_range(ticker, date_value, time(9, 15, 0), time_value)#.tail()[-1]  # .select(col_name)
            px_frame = px_frame.sort('Time').tail()[-1]
            px_frame = px_frame.select(col_name)
        return px_frame.item()

    def get_vwap(self, ticker: str, date_value: date):
        """ This function provides average of ohlc for last 30 min of market close """
        frame = self.get_data_range(ticker, date_value, time(15,0,59), time(15,29,59))
        df = frame.to_pandas()
        val = np.nanmean(df[['Open','High','Low', 'Close']])
        return round(val,2)

    def get_vwap_ver2(self, ticker: str, date_value: date):
        """ This function provides average of ohlc for last 30 min of market close """
        frame = self.get_data_range(ticker, date_value, time(15,0,59), time(15,29,59))
        df = frame.to_pandas()
        df['AvgPx'] = df[['Open', 'High','Low','Close']].mean(axis=1)
        if not df.empty:
            val = (df['AvgPx']*df['Volume']).sum()/df['Volume'].sum()
        else:
            val = self.get_value(ticker, date_value, time(15, 29, 59))
        return round(val,2)


    def get_data_expiry(self,date_value: date, start: time, end: time, expiry):
        expiry_filter = pl.col('ExpiryDate') == expiry
        date_filter = pl.col('Date') == date_value
        time_range_filter = pl.col('Time').is_between(start, end)
        data_instance = self.data.filter(date_filter & time_range_filter & expiry_filter)
        return data_instance

    def get_data_slice(self,date_value: date, start: time, end: time):
        date_filter = pl.col('Date') == date_value
        time_range_filter = pl.col('Time').is_between(start, end)
        data_instance = self.data.filter(date_filter & time_range_filter)
        return data_instance

    def get_data_range(self, ticker: str, date_value: date, start: time, end: time):
        ticker_filter = pl.col('Ticker') == ticker
        date_filter = pl.col('Date') == date_value
        time_range_filter = pl.col('Time').is_between(start, end)
        data_instance = self.data.filter(ticker_filter & date_filter & time_range_filter)
        return data_instance


    def get_max_min(self, ticker: str, date_value: date, start: time, end: time, col_name: str, value_type='max'):
        data_instance = self.get_data_range(ticker, date_value, start, end)
        if value_type == 'max':
            value = data_instance.select(col_name).max().item()
        if value_type == 'min':
            value = data_instance.select(col_name).min().item()
        value_time = data_instance.filter(pl.col(col_name) == value).select('Time').item()
        return value, value_time


class BaseStrategy(ABC):

    def __init__(self, symbol, start, end):
        self.symbol = symbol
        self.start = datetime.strptime(start,'%Y-%m-%d').date()
        self.end = datetime.strptime(end,'%Y-%m-%d').date()
        self.current_date = None
        self.current_data = None
        self.base_path = r'C:\data\\'+self.symbol.upper() + r'_FUT_OPT_DATA\\' ## set path #r'C:\data\t\\
        self.all_files = os.listdir(self.base_path)
        self.origin_dates = self.get_backtest_dates(date(2016, 6, 2), self.end)
        self.backtest_dates = [dt for dt in self.origin_dates if dt >= self.start]#self.get_backtest_dates(self.start, self.end)
        self.file_key = None
        self.datetime_expiry = None
        self.current_week_expiry = None
        self.current_week_expiry_ =None
        self.next_week_expiry = None
        self.current_month_expiry = None
        self.next_month_expiry = None
        self.data = None
        self.current_file_dates = None
        self.weekly_expiry = self.get_expiry_dates(symbol.upper(), 'weekly')
        self.monthly_expiry = self.get_expiry_dates(symbol.upper(), 'monthly')
        self.exclude_dates = [date(2017,10,19), date(2018,11,7), date(2021,11,4), date(2022,10,24)] # diwali dates
        self.monthly_exp_map = {'31JAN24':'25JAN24'}

    # internal functions
    def get_backtest_dates(self, start, end):
        all_dates = []
        for f in tqdm(os.listdir(self.base_path)):
            #print('=>',self.base_path+f)
            data = pl.read_parquet(self.base_path + f)
            dts = sorted(data.select('Date').unique().to_series().to_list())
            all_dates.extend(dts)
        all_dates = [datetime.strptime(dt,"%d/%m/%Y").date() if isinstance(dt,str) else dt for dt in all_dates]
        all_dates = [dt.date() if isinstance(dt, datetime) else dt for dt in all_dates]
        all_dates = [dt for dt in all_dates if ((dt >= start) & (dt <= end))]
        return all_dates

    def get_expiry_dates(self, symbol, how='weekly'):
        base_path = r'C:\data\EXPIRY_DATES_NEW.csv'
        data = pl.read_csv(base_path)
        if how == 'weekly':
            exp_dates = data.filter((pl.col('SYMBOL') == symbol) & (pl.col('WEEKLY') == 1)).select(
                'DATE').to_series().to_list()
        elif how == 'monthly':
            exp_dates = data.filter((pl.col('SYMBOL') == symbol) & (pl.col('MONTHLY') == 1) & (pl.col('INSTRUMENT') == 'FUTIDX')).select(
                'DATE').to_series().to_list()
        #exp_dates = [datetime.strptime(dt, '%Y-%m-%d') for dt in exp_dates]
        exp_dates = [datetime.strptime(dt, '%d-%m-%Y') for dt in exp_dates]
        return exp_dates

    def get_ticker(self, strike, expiry, option_type):
        return self.symbol.upper() + expiry + str(strike) + option_type + '.NFO'

    def _data_mapper(self):
        file_name = [k for k in self.all_files if self.file_key in k][0]
        self.data = pl.read_parquet(self.base_path + file_name)
        self.current_file_dates = self.data.select('Date').unique().sort(by='Date').to_series().to_list()

    def _create_file_key(self):
        y = self.current_date.year
        m = self.current_date.month
        if m >= 10:
            return str(y) + '-' + str(m)
        return str(y) + '-0' + str(m)

    def _get_expiry(self):
        sorted_weekly_expiry = sorted(set([exp for exp in self.weekly_expiry if exp.date() >= self.current_date]))
        self.current_week_expiry = sorted_weekly_expiry[0].strftime('%d%b%y').upper()
        self.next_week_expiry = sorted_weekly_expiry[1].strftime('%d%b%y').upper()
        sorted_weekly_expiry_ = sorted(set([exp for exp in self.weekly_expiry if exp.date() > (self.current_date + timedelta(1))])) # variant
        self.current_week_expiry_ = sorted_weekly_expiry_[0].strftime('%d%b%y').upper()
        sorted_weekly_expiry__ = sorted(set([exp for exp in self.weekly_expiry if exp.date() > self.current_date]))  # variant
        self.current_week_expiry__ = sorted_weekly_expiry__[0].strftime('%d%b%y').upper()

        sorted_monthly_expiry = sorted(set([exp for exp in self.monthly_expiry if exp.date() >= self.current_date]))
        self.current_month_expiry = sorted_monthly_expiry[0].strftime('%d%b%y').upper()
        self.next_month_expiry = sorted_monthly_expiry[1].strftime('%d%b%y').upper()
        sorted_monthly_expiry_ = sorted(set([exp for exp in self.monthly_expiry if exp.date() > (self.current_date + timedelta(1))]))  # variant
        self.current_month_expiry_ = sorted_monthly_expiry_[0].strftime('%d%b%y').upper()
        sorted_monthly_expiry__ = sorted(set([exp for exp in self.monthly_expiry if exp.date() > self.current_date]))  # variant
        self.current_month_expiry__ = sorted_monthly_expiry__[0].strftime('%d%b%y').upper()

    def iter_vars(self):
        k = self._create_file_key()
        if k != self.file_key:
            self.file_key = k
            self._data_mapper()
        self.current_data = self.data.filter(pl.col('Date') == self.current_date)
        self._get_expiry()

    def run(self):
        self.init_vars()
        for dt in self.backtest_dates:
            try:
                self.current_date = dt
                self.iter_vars()
                if not self.current_data.is_empty():
                    if self.current_date in self.exclude_dates:
                        continue
                    self.update()
            except Exception as e:
                print(e, self.current_date)
        self.exit_process()

    #-------------------------------------------------------

    @abstractmethod
    def init_vars(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def exit_process(self):
        pass


# class Sample(BaseStrategy):
#
#     def init_vars(self):
#         pass
#
#     def update(self):
#         print(self.current_date, self.current_week_expiry, self.next_week_expiry, self.current_month_expiry, self.next_month_expiry)
#
#     def exit_process(self):
#         pass
#
#
# def main():
#     test_obj = Sample('nifty','2019-3-1','2024-1-25')
#     test_obj.run()


# main()