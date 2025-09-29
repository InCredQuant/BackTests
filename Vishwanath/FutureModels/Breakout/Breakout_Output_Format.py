import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from pathlib import Path

def drop_duplicates_preserve_nonzero(df):
    df_result = df.groupby('Date', as_index=False).apply(
        lambda group: pd.Series({
            'Pos': next((val for val in group['Pos'] if val != 0), 0),
            'Returns': next((val for val in group['Returns'] if val != 0), 0),
        })
    ).reset_index(drop=True)
    return df_result

def calculate_returns(df):
    df.columns = df.columns.str.capitalize()
    df['Date'] = pd.to_datetime(df['Date'])
    prev_pos = df['Pos'].shift(1).fillna(0)
    prev_px = df['Px'].shift(1)
    current_px = df['Px']
    conditions = [prev_pos == 1, prev_pos == -1]
    choices = [(current_px / prev_px) - 1, (prev_px / current_px) - 1]
    df['Returns'] = np.select(conditions, choices, default=0)
    return df

def calculate_metrics(df):
    df['NAV'] = 100 * (1 + df['Returns']).cumprod()
    df['Running_Max_NAV'] = df['NAV'].cummax()
    df['Drawdown'] = (df['NAV'] / df['Running_Max_NAV']) - 1
    max_drawdown = df['Drawdown'].min()
    result_df = df[['Date', 'Pos', 'Returns', 'NAV', 'Drawdown']]
    result_df['Max_Drawdown'] = max_drawdown
    return result_df

folder = Path(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Breakout\output")
outputfolder = Path(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Breakout\output\FinalOutput")
for file in folder.glob('*DAILY.xlsx'):
    df = pd.read_excel(file)
    retsdf = calculate_returns(df.copy())
    result = drop_duplicates_preserve_nonzero(retsdf.copy())
    result_df = calculate_metrics(result.copy())
    output_file = outputfolder / f"{file.stem}_OUTPUT.xlsx"
    result_df.to_excel(output_file)

print("Output files generated successfully.")
print("Going to sleep. Bye!")
