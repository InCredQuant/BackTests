import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from pathlib import Path

def calculate_returns(df):
    df['Date'] = pd.to_datetime(df['Date'])
    prev_pos = df['Pos'].shift(1).fillna(0)
    prev_px = df['Px'].shift(1)
    current_px = df['Px']
    conditions = [prev_pos == 1, prev_pos == -1]
    choices = [(current_px / prev_px) - 1, (prev_px / current_px) - 1]
    df['Returns'] = np.select(conditions, choices, default=0)
    df['NAV'] = 100 * (1 + df['Returns']).cumprod()
    df['Running_Max_NAV'] = df['NAV'].cummax()
    df['Drawdown'] = (df['NAV'] / df['Running_Max_NAV']) - 1
    max_drawdown = df['Drawdown'].min()
    result_df = df[['Date', 'Px', 'Pos', 'Returns', 'NAV', 'Drawdown']]
    result_df['Max_Drawdown'] = max_drawdown
    return result_df

folder = Path(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Pattern\output")
outputfolder = Path(rf"C:\Vishwanath\PythonCodes\Strategy\FutureModels\Pattern\output\FinalOutput")
for file in folder.glob('*.xlsx'):
    df = pd.read_excel(file, sheet_name='DAILY')
    result_df = calculate_returns(df)
    output_file = outputfolder / f"{file.stem}_OUTPUT.xlsx"
    result_df.to_excel(output_file)

print("Output files generated successfully.")
print("Going to sleep. Bye!")

