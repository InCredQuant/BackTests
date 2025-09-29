import pandas as pd
from datetime import datetime

def calculate_straddle_prices(input_path, output_path):
    df = pd.read_feather(input_path)
    df['time'] = df['datetime'].dt.time
    min_expiry_per_datetime = df.groupby('datetime')['expiry'].transform('min')
    currentWeekDf = df[df['expiry'] == min_expiry_per_datetime].copy()
    currentWeekDf = currentWeekDf.sort_values(['datetime', 'strike_price'])
    currentWeekDf['strike_spot_diff'] = abs(currentWeekDf['strike_price'] - currentWeekDf['spot'])
    atm_strikes = (currentWeekDf.groupby('datetime')['strike_spot_diff']
                .idxmin()
                .map(lambda idx: currentWeekDf.loc[idx, 'strike_price']))
    currentWeekDf['atm_strike'] = currentWeekDf['datetime'].map(atm_strikes)
    atm_only_df = currentWeekDf[currentWeekDf['strike_price'] == currentWeekDf['atm_strike']]
    straddlePrice = atm_only_df[atm_only_df['time']>=datetime.strptime('09:30:00','%H:%M:%S').time()]
    straddlePrice['straddle_price'] = straddlePrice.groupby(['datetime'])['close'].transform('sum')
    straddlePriceDf = straddlePrice.drop_duplicates(subset='datetime')[['datetime','straddle_price']]
    straddlePriceDf.to_excel(output_path, index=False)

if __name__ == "__main__":
    calculate_straddle_prices("combined_data_BN.feather","q1_final.xlsx")