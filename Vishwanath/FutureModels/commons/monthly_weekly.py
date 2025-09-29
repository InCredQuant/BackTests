#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 21-10-2022 12:17
# @Author : Ankur

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# for the output plot
# fig, ax = plt.subplots(figsize = (12, 7))


def get_return_matrix(df, freq='M', vertical=False):
    '''
    Parameters
    ----------
    df - Dataframe containing NAV
    freq - 'M' (Monthly), 'W' (Weekly)

    Returns
    -------
    df - Containing respective return matrix(Monthly/Weekly)
    '''
    mn_map = {1:'JAN',2:'FEB', 3:'MAR', 4:'APR', 5:'MAY', 6:'JUN', 7:'JUL', 8:'AUG', 9:'SEP',10:'OCT',11:'NOV',12:'DEC'}
    mn_cols = [mn_map[v] for v in mn_map.keys()]
    df.columns = ['PX']
    resample_df = None
    if freq == 'Y':
        resample_df = df.resample('Y').last()
    elif freq == 'M':
        resample_df = df.resample('M').last()
    elif freq == 'W':
        resample_df = df.resample('W-Fri').last()
    resample_df['Return'] = round(resample_df['PX'].pct_change() * 100, 2)
    if vertical:
        return resample_df
    resample_df['Year'] = resample_df.index.year
    resample_df['Month'] = resample_df.index.month
    resample_df['Month'] = resample_df['Month'].apply(lambda x:mn_map[x])
    if freq == 'Y':
        pivot = pd.pivot_table(resample_df, columns=['Year'])
    elif freq == 'M':
        pivot = pd.pivot_table(resample_df, index=['Year'], columns=['Month'], aggfunc={'Return':np.sum}, fill_value=0)
        pivot = pivot.droplevel(0, axis=1) # drop multilevel index for column
        pivot = pivot.reindex(columns=mn_cols) # sort columns
    # Generate a custom diverging colormap
    # #------------------------------------------------------------------------------------------------------
    # cmap = sns.diverging_palette(133, 10, as_cmap=True)
    # with sns.axes_style("white"):
    #     ax = sns.heatmap(pivot, annot=True, fmt='.2f', cmap=cmap, vmin=-0.99, vmax=.99, center=0.00,
    #                      square=True, linewidths=.5, annot_kws={"size": 8}, cbar_kws={"shrink": .5})
    # #sns.heatmap(pivot, annot=True)
    # plt.show()
    # # ------------------------------------------------------------------------------------------------------
    return pivot


def main():
    db_path = r'G:\My Drive\resources\data\NIFTY.xlsx'
    df = pd.read_excel(db_path, index_col=0)
    df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
    returns = get_return_matrix(df)
    print(returns)


if __name__ == '__main__':
    main()