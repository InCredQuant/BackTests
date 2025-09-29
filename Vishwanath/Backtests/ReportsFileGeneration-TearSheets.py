# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 11:42:46 2023

@author: virendra.kumar_incre
"""

import pandas
import numpy
import quantstats
import datetime
#import matplotlib
#import matplotlib.pyplot as plt
#from IPython.display import display
#import seaborn
#import tkinter
from tkinter import filedialog

import sys
sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
from GetData import *

import warnings
warnings.filterwarnings("ignore")
startDate = datetime.datetime(2011, 12, 30)

conn = GetConn('PriceData')
indexData = GetDataForFutTickersFromBloomDB(conn, ['NZ1 INDEX'], 'PX_LAST', startDate-datetime.timedelta(1))
indexData = pandas.DataFrame(indexData.loc[startDate:, :])
print('Done')

try:
    datafiles = filedialog.askopenfilenames(initialdir ='G:/Shared drives/QuantFunds/Liquid1/LiveModels/ModelFiles', title = 'Please Select NAV Files!', filetypes=(('Excel File', '*.xlsx'), ('Excel File', '*.xls')))
except:
    print('Directory Access Issue!')
print('Started!')
FullReport = pandas.DataFrame()
AllDataNAV = pandas.DataFrame()
for navFile in datafiles:
    navData = pandas.read_excel(navFile, sheet_name = 'NAV', header = 0, index_col = 0)
    navData = navData.loc[startDate:]
    name = navFile.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
    if 'banknifty' in name.lower():
        indexRet = indexData.loc[navData.index, 'AF1 INDEX']
        indexRet.columns = ['BankNifty Fut']
    else:
        indexRet = indexData.loc[navData.index, 'NZ1 INDEX']
        indexRet.columns = ['Nifty Fut']   
    quantstats.reports.html(navData.loc[:, 'NAV'], benchmark= indexRet.pct_change(), rf = 0.065, title =  name, download_filename= name+'.html')
    report = quantstats.reports.metrics(navData.loc[:, 'NAV'].pct_change(), benchmark= indexRet.pct_change(), rf = 0.065, display = False, mode = 'full')
    tempReport = pandas.DataFrame(report.loc[:, 'Strategy'])
    tempReport.columns = [name]
    navData.columns =[name]
    AllDataNAV = pandas.concat([AllDataNAV, navData], axis = 1)
    FullReport = pandas.concat([FullReport, tempReport], axis = 1)
AllDataNAV = pandas.concat([AllDataNAV, indexData], axis = 1)
print('Completed!')