# -*- coding: utf-8 -*-
"""
Created on Mon Sep  5 13:26:50 2022

@author: Viren@Incred
Purpose: Doing all types of Testing
"""
'''
import sqlite3
import os

dbpath = 'Z:/LiveDB'

conn = sqlite3.connect(os.path.join(dbpath, 'MIS.db'))
curs = conn.cursor()
conn.close()




import numpy
import pandas
import matplotlib.pyplot as plt


RN = []
numpy.random.seed(10000)
def simulate_path(s0, mu, sigma, timehorizon, timesteps, n_sims):
    S0 = s0             # initial spot price
    r = mu              # rf in risk neutral network
    T = timehorizon     # time horizon
    t = timesteps       # no of time steps
    n = n_sims          # no of simulations
    dt = T/t            # length of time interval
    
    S = numpy.zeros((t, n))
    S[0] = s0
    RN = numpy.random.standard_normal(S.shape)
    for i in range(0, t-1):
        S[i+1] = S[i]*(1 + r*dt + sigma*numpy.sqrt(dt)*RN[i])
    return S

#pandas.DataFrame(S, index = numpy.around(numpy.arange(0, T, dt), 2), columns = range(1, n+1))

price_path = pandas.DataFrame(simulate_path(100,0.05,0.2,1,252,100000))
plt.plot(price_path.iloc[:,:100])
plt.xlabel('time steps')
plt.xlim(0,252)
plt.ylabel('index levels')
plt.title('Monte Carlo Simulated Asset Prices');

s0 = 100
mu = 0.05
sigma = 0.20
timehorizon = 1
timesteps = 252
n_sims = 100


futtemp = mydata.CloseFuts.pct_change(fill_method = None)
stocktemp = mydata.CloseStocks.pct_change(fill_method = None)
temp =  futtemp  - stocktemp
temp1 = temp[temp>0.05]
temp2 = temp[temp<-0.05]
temp  = temp1.combine_first(temp2)

DataDF = []
for cName in temp.columns:
    localtemp = temp.loc[:, cName].dropna()
    if len(localtemp) >0:
        for ind in localtemp.index:
            DataDF.append([cName, futDictInv[cName], ind, mydata.CloseFuts.loc[ind, cName], localtemp.loc[ind]])
    
DataDF = pandas.DataFrame(DataDF, columns = ['Ticker', 'FutTicker', 'Date', 'Close', '%Dev'])

def sum(a, b, c):
    if c:
        return a+b+c
    else:
        return a+b
    
    
grpDict = {}
gp = temp.groupby('Decided')
for grp in gp.groups.keys():
    dtemp = gp.get_group(grp)
    grpDict[grp] = list(dtemp['Ticker'].values)

import json
with open('C:/Users/virendra.kumar_incre/Desktop/tempfile3.txt', 'w') as f:
    f.write(str(list(temp['Ticker'])))
    
    
params = dict(
    pfast=10,  # period for the fast moving average
    pslow=30   # period for the slow moving average
    )

def check_even(inum):
    if inum % 2 == 0:
        return True
    return False
    
def check(num):
    switch = {1: 4, 
              2: 3,
              3: 5,
              4: 8
              }
    return switch.get(num, check_even)
print(check(2))


def one():
    return 'Jan'

def Two():
    return 'Feb'

def Three():
    return 'Mar'


def numb_to_month(n):
    switcher = {1: one, 2: Two, 3: Three}
    func = switcher.get(n, lambda: 'Invalid Month')
    return func()


import pandas
import numpy
import random
import quantstats
import datetime
import matplotlib
import matplotlib.pyplot as plt
from IPython.display import display
import seaborn
import tkinter
from tkinter import filedialog

import warnings
warnings.filterwarnings("ignore")

plt.style.use('ggplot')
matplotlib.rcParams['figure.figsize'] = [20.0, 8.0]
matplotlib.rcParams['font.size'] = 14
matplotlib.rcParams['lines.linewidth'] = 2.0

root = tkinter.Tk()
root.lift()
root.withdraw()
try:
    datafile = filedialog.askopenfilename(parent = root, initialdir = 'Z:/BacktestsResults/', title = 'Please Select NAV File!', filetypes=(('Excel File', '*.xlsx'), ('Excel File', '*.xls')))
except:
    datafile = filedialog.askopenfilename(parent = root, initialdir = 'G:/Shared drives/BackTests/BacktestsResults/', title = 'Please Select NAV File!', filetypes=(('Excel File', '*.xlsx'), ('Excel File', '*.xls')))

stratDate = datetime.datetime(2013, 1, 1)

navData = pandas.read_excel(datafile, sheet_name = 'NAV', header = 0, index_col = 0)
navData = navData.loc[stratDate:]

priceData = pandas.read_excel(datafile, sheet_name = 'Prices', header = 0, index_col = 0)
indicesList = ['NZ1 INDEX', 'NIFTY INDEX', 'BSE100 INDEX', 'BSE200 INDEX', 'BSE500 INDEX', 'NSE500 INDEX']
for index in indicesList:
    try:
        indexData = priceData.loc[:, index]
        break
    except:
        continue
    
indexData = indexData.loc[stratDate:]
print(datafile.split('/')[-1].replace('.xlsx', ''))

quantstats.reports.full(navData, benchmark= indexData)
quantstats.reports.html(navData, benchmark= indexData)


from IPython.display import Javascript
from nbconvert import HTMLExporter

def save_notebook():
    display(
        Javascript("IPython.notebook.save_notebook()"),
        include=['application/javascript']
    )

def output_HTML(read_file, output_file):
    import codecs
    import nbformat
    exporter = HTMLExporter()
    # read_file is '.ipynb', output_file is '.html'
    output_notebook = nbformat.read(read_file, as_version=4)
    output, resources = exporter.from_notebook_node(output_notebook)
    codecs.open(output_file, 'w', encoding='utf-8').write(output)


import time

save_notebook()
time.sleep(3)
current_file = 'C:/Users/virendra.kumar_incre/Desktop/Work/Analyze-NAV-Stats.ipynb'
output_file = 'output_file.html'
output_HTML(current_file, output_file)

'''


import os
import pandas as _pd
import datetime as _dt
import numpy as _np


path = 'Z:/BacktestsResults/DailyTradingSignals_Indices/'
filenames = os.listdir(path)

fulldata = _pd.DataFrame()
for filename in filenames:
    if '_TradesRegister' not in filename and '.xlsx' in filename:
        indata = _pd.read_excel(path+filename, sheet_name = 'NAV', index_col = 0)
        indata.columns = [filename.replace('.xlsx', '')]
        fulldata = _pd.concat([fulldata, indata], axis = 1)
        
ll = []
startDate = datetime.datetime(2021, 1, 1)
for i in range(1000):
    ll.append(startDate + datetime.timedelta(i))


import sqlite3
import pandas
import pickle
indexfile = 'G:/Shared drives/BackTests/OptionsBacktest/workspace/intraday_data_process/futures_intraday/BANKNIFTY-II.pkl'

file = open(indexfile,  'rb')
indexData = pickle.load(file)
file.close()

indexData.columns = [i.replace(' ', '') for i in indexData.columns]



conn = sqlite3.connect('Z:/LiveDB/PriceData.db')
curs = conn.cursor()

indexData.to_sql('ScripM', conn, schema = None, if_exists = 'append', index = False)#flavor = 'sqlite',
conn.commit()
conn.close()



 def MD12_Seasoning(self, tickersList, sl = -0.05, target = 0.15):
     # Defining Strategy
     if hasattr(self, 'Strategy'):
         #for ticker in tickersList:
         self.Strategy.loc[self.CurrentTime, tickersList] = 'Seasoning'
     Price = self.Close.loc[self.CurrentTime, tickersList]
     Long = set(Price.dropna().index)
     
     curTime = self.CurrentTime.date()
     # For better implementation, make the Active days list for that month andchek for the entry & Exit points via that List
     A_ = [24, 25, 26, 27, 28, 29, 30, 31, 1, 2, 3] # These are active Days
     B_ = range(4, 15)
     H_ = [5, 6]# Sat/Sun weekdays
     HolidayList = ['26-01-2022', '01-03-2022', '18-03-2022', '14-04-2022', '15-04-2022', '03-05-2022', '09-08-2022', '15-08-2022', '31-08-2022', '05-10-2022', '24-10-2022', '26-10-2022', '08-11-2022', '26-01-2023', '07-03-2023', '30-03-2023', '04-04-2023', '07-04-2023', '14-04-2023', '01-05-2023', '28-06-2023', '15-08-2023', '19-09-2023', '02-10-2023', '24-10-2023', '14-11-2023', '27-11-2023', '25-12-2023']
     HolidayList = [datetime.datetime.strptime(i, '%d-%m-%Y').date() for i in HolidayList]
     #condition1 = curTime.day in A_ or  ((curTime.day+1) in A_ and curTime.weekday()+1 in H_) or ((curTime.day+2) in A_ and curTime.weekday()+2 in H_ and curTime.weekday()+1 in H_)# Checking for if 24 is on weekend, then detect it in advance
     #condition2 = ((curTime.day+1) in B_ and curTime.weekday()+1 in H_) or ((curTime.day+2) in B_ and curTime.weekday()+2 in H_ and curTime.weekday()+1 in H_)# if 4 is on Weekend, then exit before that on friday itself
     #check 3 May 2022, 24 Oct 2022
     # Current is active or (Cur +1 is active & Cur +1 in weekend or Holidays)
    
     condition1 = curTime.day in A_ or  ((curTime.day+1) in A_ and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)) or ((curTime.day+2) in A_ and ((curTime.weekday()+2 in H_ or curTime + datetime.timedelta(2) in HolidayList) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList))) or ((curTime.day+3) in A_ and ((curTime.weekday()+3 in H_ or curTime + datetime.timedelta(3) in HolidayList) and (curTime.weekday()+2 in H_ or curTime + datetime.timedelta(2) in HolidayList) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)))# Checking for if 24 is on weekend, then detect it in advance
     condition2 = ((curTime.day+1) in B_ and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)) or ((curTime.day+2) in B_ and (( curTime.weekday()+2 in H_  or curTime + datetime.timedelta(2) in HolidayList)) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList)) or ((curTime.day+3) in B_ and (( curTime.weekday()+3 in H_  or curTime + datetime.timedelta(3) in HolidayList)) and (curTime.weekday()+2 in H_ or curTime + datetime.timedelta(2) in HolidayList) and (curTime.weekday()+1 in H_ or curTime + datetime.timedelta(1) in HolidayList))
     # if 4 is on Weekend, then exit before that on friday itself
     if condition1 and not condition2:
         self.PositionWOSL.loc[self.CurrentTime, Long] = 1
     #if self.CurrentTime.date() in [datetime.date(2023, 2, 1), datetime.date(2023, 2, 2), datetime.date(2023, 2, 3)]:
     #    self.PositionWOSL.loc[self.CurrentTime, Long] = 0            
     for iTicker in tickersList:
         self.StopLoss(iTicker, sl)# -3% StopLoss
         self.Target(iTicker, target)# +10% Target


# Assignmnet Job: We have to create List of All the Holidays (get data from NSE/BSE website from 2010 to 2021. 2022 & 2023 we already have)
# Write simple code so that this code can assign value 1 to the self.PositionWOSL Dataframe for that time. by default this PositionWOSL dataframe values are zero only.
# As discussed entry (value 1) and Exit( value 0) would be assigned if the entry or exit dates are eitgher weekend or Holiday list.

# Suggestion: Made list of active days of the month, store that in a global variable, so that every day you don't have to recalculate it for that particular month
# CLose Price DataFrame is stored in Exfel file.


import psycopg2

conn = psycopg2.connect(
    host='locahost',
    port= '5432',
    database='Quant',
    user='Viren',
    password='viren123')

cur = conn.cursor()
query = 'SELECT * FROM trade.stocks ORDER BY "Ticker" DESC, "Date" DESC, "Time" DESC LIMIT 100;'
cur.execute(query)

data = pandas.DataFrame(cur.fetchall(), columns = [it[0] for it in cur.description])

cur.close()
conn.close()


import mibian
# [Underlying Price, Strike, Interest Rate, Days to Expiry], Price
c = mibian.BS([17828, 18200, 10, 14], callPrice = 22.0)
c.impliedVolatility
opt = mibian.BS([17828, 18200, 10, 14], volatility  = c.impliedVolatility)

c1 = mibian.BS([17828, 18200, 10, 14], callPrice = 22.0, putPrice = 335.15)
c1.impliedVolatility
opt1 = mibian.BS([17828, 18200, 10, 14], volatility  = c1.impliedVolatility)

c2 = mibian.BS([17828, 18200, 10, 14], putPrice = 335.15)
c2.impliedVolatility
opt2 = mibian.BS([17828, 18200, 10, 14], volatility  = c2.impliedVolatility)


allROws = []
allROws.append([opt.callDelta, opt.callDelta2, opt.callRho, opt.callTheta, opt.exerciceProbability, opt.gamma, opt.impliedVolatility, opt.putCallParity, opt.putDelta, opt.putDelta2, opt.putRho, opt.putTheta, opt.vega, opt.volatility])
allROws.append([opt1.callDelta, opt1.callDelta2, opt1.callRho, opt1.callTheta, opt1.exerciceProbability, opt1.gamma, opt1.impliedVolatility, opt1.putCallParity, opt1.putDelta, opt1.putDelta2, opt1.putRho, opt1.putTheta, opt1.vega, opt1.volatility])
allROws.append([opt2.callDelta, opt2.callDelta2, opt2.callRho, opt2.callTheta, opt2.exerciceProbability, opt2.gamma, opt2.impliedVolatility, opt2.putCallParity, opt2.putDelta, opt2.putDelta2, opt2.putRho, opt2.putTheta, opt2.vega, opt2.volatility])

import pandas
df = pandas.DataFrame(allROws)
df.columns = ['callDelta', 'callDelta2', 'callRho', 'callTheta', 'exerciceProbability', 'gamma', 'impliedVolatility', 'putCallParity', 'putDelta', 'putDelta2', 'putRho', 'putTheta', 'vega', 'volatility']

import gc
gc.collect()

import pandas
import numpy
import random
import quantstats
import datetime
import matplotlib
import matplotlib.pyplot as plt
from IPython.display import display
import seaborn
import tkinter
from tkinter import filedialog
import sys
sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
from GetData import *

import warnings
warnings.filterwarnings("ignore")

plt.style.use('ggplot')
matplotlib.rcParams['figure.figsize'] = [20.0, 8.0]
matplotlib.rcParams['font.size'] = 14
matplotlib.rcParams['lines.linewidth'] = 2.0

root = tkinter.Tk()
root.lift()
try:
    datafile = filedialog.askopenfilename(parent = root, initialdir ='G:\Shared drives\BackTests\BackTestsResults\DailyTradingSignals_Indices', title = 'Please Select NAV File!', filetypes=(('Excel File', '*.xlsx'), ('Excel File', '*.xls')))
    #datafile = filedialog.askopenfilename(parent = root, initialdir = 'Z:/BacktestsResults/', title = 'Please Select NAV File!', filetypes=(('Excel File', '*.xlsx'), ('Excel File', '*.xls')))
except:
    datafile = filedialog.askopenfilename(parent = root, initialdir = 'G:/Shared drives/BackTests/BacktestsResults/', title = 'Please Select NAV File!', filetypes=(('Excel File', '*.xlsx'), ('Excel File', '*.xls')))

stratDate = datetime.datetime(2012, 12, 31)#datetime.datetime(2014, 6, 26)

navData = pandas.read_excel(datafile, sheet_name = 'NAV', header = 0, index_col = 0)
navData = navData.loc[stratDate:]

conn = GetConn('PriceData')
indexData = GetDataForIndicesFromBloomDB(conn, ['NIFTY INDEX'], 'PX_LAST')    
indexData = indexData.loc[stratDate:]
print(datafile.split('/')[-1].replace('.xlsx', ''))

quantstats.reports.full(pandas.Series([it[0] for it in navData.values], index = navData.index), benchmark= pandas.Series([it[0] for it in indexData.values], index = indexData.index))

quantstats.reports.html(pandas.Series([it[0] for it in navData.values], index = navData.index), benchmark= pandas.Series([it[0] for it in indexData.values], index = indexData.index), output = 'C:/Users/virendra.kumar_incre/Downloads/temp.html')

try:
    quantstats.reports.html(pandas.Series([it[0] for it in navData.values]), 'SPY')
except:
    pdb.set_trace()
    
    

navData = pandas.read_excel('G:/Shared drives/QuantFunds/Liquid1/GenericReports/ModelsWeights.xlsx', sheet_name = 'BasketsNAV', header = 0, usecols = [1, 7, 8, 9, 10], index_col = 0)



import pandas
import numpy
import quantstats
import datetime
#import matplotlib
#import matplotlib.pyplot as plt
#from IPython.display import display
#import seaborn
#import tkinter

import sys
sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
from GetData import *

import warnings
warnings.filterwarnings("ignore")
startDate = datetime.datetime(2011, 12, 30)

try:
    conn = GetConn('PriceData')
except:
    pass




print('Done')

from tkinter import filedialog
import pandas

try:
    datafiles = filedialog.askopenfilenames(parent = root, initialdir ='G:/Shared drives/QuantFunds/Liquid1/LiveModels/ModelFiles', title = 'Please Select NAV Files!', filetypes=(('Excel File', '*.xlsx'), ('Excel File', '*.xls')))
except:
    print('Directory Access Issue!')
print('Started!')

if 'conn' in locals():
    indexData = GetDataForFutTickersFromBloomDB(conn, ['NZ1 INDEX'], 'PX_LAST', startDate-datetime.timedelta(1))
    indexData = pandas.DataFrame(indexData.loc[startDate:, :])
else:
    indexData = pandas.DataFrame()

FullReport = pandas.DataFrame()
AllDataNAV = pandas.DataFrame()
for navFile in datafiles:
    navData = pandas.read_excel(navFile, sheet_name = 'NAV', header = 0, index_col = 0)
    navData = navData.loc[startDate:]
    name = navFile.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
    if len(indexData) ==0:
        indexData = pandas.read_excel(navFile, sheet_name = 'Prices', header = 0, index_col = 0)
        indexData = indexData.loc[:, 'NIFTY INDEX']
        indexData.columns = ['NZ1 INDEX']
        
    if 'banknifty' in name.lower():
        indexRet = indexData.loc[navData.index, 'AF1 INDEX']
        indexRet.columns = ['BankNifty Fut']
    else:
        indexRet = indexData.loc[navData.index, 'NZ1 INDEX']
        indexRet.columns = ['Nifty Fut']   
    quantstats.reports.html(navData.loc[:, 'NAV'], benchmark= indexRet.pct_change(), rf = 0.065, title =  name, download_filename=name+'.html')
    report = quantstats.reports.metrics(navData.loc[:, 'NAV'].pct_change(), benchmark= indexRet.pct_change(), rf = 0.065, display = False, mode = 'full')
    tempReport = pandas.DataFrame(report.loc[:, 'Strategy'])
    tempReport.columns = [name]
    navData.columns =[name]
    AllDataNAV = pandas.concat([AllDataNAV, navData], axis = 1)
    FullReport = pandas.concat([FullReport, tempReport], axis = 1)
AllDataNAV = pandas.concat([AllDataNAV, indexData], axis = 1)
print('Completed!')



import datetime
#import sqlite3
import pandas
from GetData import GetConn, QueryFutTickers
from GetData import *#GetComponentsForIndexForDateRange, GetDataForIndicesFromBloomDB, GetDataForFutTickersFromBloomDB, GetConn, QueryFutTickers
import MyTechnicalLib
#import time
#from FactorsDef import *
import pickle
import time
import warnings
warnings.filterwarnings("ignore")

t1 = time.time()
priceconn = GetConn('PriceData')
analystconn = GetConn('AnalystData')


DataStartDate = datetime.date(2001, 12, 25)#(2022, 12, 29)#2009
#DataStartDate = datetime.date(2020, 1, 1)

allstocks = []

indexList = ['SENSEX INDEX' , 'NIFTY INDEX' , 'NSE500 INDEX' , 'BSE100 INDEX' , 'BSE200 INDEX' , 'BSE500 INDEX' , 'NSEBANK INDEX' , 'NSEMCAP INDEX' , 'NSE100 INDEX' , 'NSEIT INDEX' , 'NIFTYJR INDEX' , 'NSEFMCG INDEX' , 'NSEINFR INDEX' , 'NSENRG INDEX' , 'NSEPHRM INDEX' , 'NSEPSBK INDEX' , 'NSEPSE INDEX' , 'NSE200 INDEX' , 'NSEAUTO INDEX' , 'NSEMET INDEX' , 'NSESMCP INDEX' , 'NSECMD INDEX' , 'NSEFIN INDEX' , 'NSECON INDEX', 'SPBSAIP INDEX']
for ind in indexList:
    indexcomponents = GetComponentsForIndexForDateRange(priceconn, DataStartDate, datetime.datetime.today(), ind)# 'BSE100 INDEX'
    [allstocks.extend(i) for i in indexcomponents.values()]
    
allstocks = set(allstocks)

ll = []
[[ll.append([k, it]) for it in v] for k, v in model.MODELSELECTOR.items()]
pandas.DataFrame(ll).to_clipboard()

###########################################
import pandas
import numpy
import quantstats
import datetime

from GetData import *
import warnings
warnings.filterwarnings("ignore")
startDate = datetime.datetime(2002, 10, 1)#datetime.datetime(2011, 12, 30)

try:
    conn = GetConn('PriceData')
except:
    pass

if 'conn' in locals():
    indexData = GetDataForIndicesFromBloomDB(conn, ['BSE100 INDEX', 'BSE200 INDEX', 'BSE500 INDEX', 'NSESMCP INDEX', 'NSEMCAP INDEX'], 'PX_LAST', startDate-datetime.timedelta(1))
    #indexData = GetDataForFutTickersFromBloomDB(conn, ['NZ1 INDEX'], 'PX_LAST', startDate-datetime.timedelta(1))
    indexData = pandas.DataFrame(indexData.loc[startDate:, :])
    indexData = indexData.sort_index()
else:
    indexData = pandas.DataFrame()

print('Done')

name = navFile.split('/')[-1].replace('.xlsx', '').replace('.xls', '').split('_')
strategy = navFile.split('/')[-1].replace('.xlsx', '').replace('.xls', '').split('_')[0]
indexName = navFile.split('/')[-1].replace('.xlsx', '').replace('.xls', '').split('_')[1]



allData = []
for ind in mydata.IndexInclusionFactor.index[2:-1]:
    nextInd = mydata.IndexInclusionFactor.loc[ind:].index[1]
    indConst = mydata.IndexInclusionFactor.loc[ind].dropna().index
    indConst = set.intersection(set(indConst), set(mydata.Close.columns))
    rets = numpy.divide(mydata.Close.loc[:nextInd, indConst].iloc[-2], mydata.Close.loc[:ind, indConst].iloc[-1]).dropna()-1
    indexPrice = mydata.indexprice.loc[:datetime.date(2023, 12, 28), 'BSE500 INDEX'].dropna()
    indexret = (indexPrice.loc[:nextInd].iloc[-1]/indexPrice.loc[:ind].iloc[-1])-1
    rets = rets.sort_values()
    top30Avg = rets.iloc[-30:].mean()
    bottom30Avg = rets.iloc[:30].mean()
    top50Avg = rets.iloc[-50:].mean()
    bottom50Avg = rets.iloc[:50].mean()
    top100Avg = rets.iloc[-100:].mean()
    bottom100Avg = rets.iloc[:100].mean()
    allAvg = rets.mean()
    allMedian = rets.median()
    underPerf = rets[rets<indexret]
    allData.append([nextInd, top30Avg, bottom30Avg, top50Avg, bottom50Avg, top100Avg, bottom100Avg, allAvg, allMedian, indexret, len(underPerf), len(rets)])

dtemp = pandas.DataFrame(allData)
dtemp.columns = ['Date', 'Top30', 'Bottom30', 'Top50', 'Bottom50' , 'Top100', 'Bottom100', '500Avg', '500Median', 'BSE500Index', 'UnderPerf', 'Available']
dtemp.index = dtemp.Date
del dtemp['Date']


import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()
# file_path = filedialog.askopenfilename()
file_path = filedialog.askdirectory()

new_file = input("Name file\n")
open_file = open(f"{file_path}\%s.py" % new_file, 'w')



import pandas
import numpy
import quantstats
import datetime
#import matplotlib
#import matplotlib.pyplot as plt
#from IPython.display import display
#import seaborn
#import tkinter

import sys
sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
from GetData import *

import warnings
warnings.filterwarnings("ignore")
startDate = datetime.datetime(2002, 10, 1)#datetime.datetime(2011, 12, 30)

try:
    conn = GetConn('PriceData')
except:
    pass

if 'conn' in locals():
    indexData = GetDataForIndicesFromBloomDB(conn, ['NIFTY INDEX', 'NSEBANK INDEX', 'BSE100 INDEX', 'BSE200 INDEX', 'BSE500 INDEX', 'NSESMCP INDEX', 'NSEMCAP INDEX'], 'PX_LAST', startDate-datetime.timedelta(1))
    #indexData = GetDataForFutTickersFromBloomDB(conn, ['NZ1 INDEX'], 'PX_LAST', startDate-datetime.timedelta(1))
    indexData = pandas.DataFrame(indexData.loc[startDate:, :])
    indexData = indexData.sort_index()
    print('Fetched')
    
    
import pandas
filename = 'G:/Shared drives/BackTests/Spot Data 1min/BANKNIFTY_synthetic_futures_2022.parquet'
indata = pandas.read_parquet(filename)


# Read NAV data from Folder Files

# Select one or more folders
import os
import tkinter
import pandas
from tkinter import filedialog
root = tkinter.Tk()

root.title("Select One or Multiple Folders!")

root.withdraw()
root.update()

def select_folders():
    folders = []
    while True:
        folder = filedialog.askdirectory()
        if folder:
            folders.append(folder)
        else:
            break
    return folders

selectedFolders = select_folders()

ignoreWords = ['.jpg', 'tradesregister']
intradayWords = ['hourly', 'half', 'mins']
FullData = pandas.DataFrame()
for iDir in selectedFolders:
    allFiles = os.listdir(iDir)
    for iFile in allFiles:
        if sum([True if it in iFile.lower() else False for it in ignoreWords]) ==0:
            iNAV = pandas.read_excel(os.path.join(iDir, iFile), sheet_name = 'NAV', index_col = 0)
            iNAV.columns = ['_'.join(iFile.split('_')[:-1])]
            if sum([True if it in iFile.lower() else False for it in intradayWords])>0:
                iNAV = iNAV.resample('d', convention = 'end').last().dropna()
            FullData = pandas.concat([FullData, iNAV], axis = 1)


###
from intraday_db_postgres import DataBaseConnect
import datetime
import pandas

db_obj = DataBaseConnect(f'postgresql+psycopg2://{"postgres"}:{"postgres"}@{"192.168.44.4"}:{"5432"}/{"data"}')
db_obj.connect()

expiryDates = db_obj.getExpiryDates()

expiryDates = [it for it in expiryDates if it >= datetime.date(2024, 3, 28)]
expiryDates = [it for it in expiryDates if it <= datetime.date(2024, 6, 13)]
expiryDates.append(datetime.date(2024, 5, 23))
expiryDates.append(datetime.date(2024, 6, 6))
expiryDates.append(datetime.date(2024, 6, 13))

expiryDates.sort()

closeprices = pandas.DataFrame()
tickers = ['NIFTY25APR24XX0', 'NIFTY30MAY24XX0', 'NIFTY27JUN24XX0']
fromDate = datetime.date(2024, 3, 28)


callPrice = db_obj.GetNSEBhavCopyDatabyTicker(calls, 'Close')
callPrice.diff().loc[fromDate:].to_clipboard()

putPrice = db_obj.GetNSEBhavCopyDatabyTicker(puts, 'Close')
putPrice.diff().loc[fromDate:].to_clipboard()


callDelta = db_obj.getNSEBhavCopyGreeks(calls, 'Delta')
callDelta.loc[fromDate:].to_clipboard()

putDelta = db_obj.getNSEBhavCopyGreeks(puts, 'Delta')
putDelta.loc[fromDate:].to_clipboard()


######################
indata = pandas.read_clipboard()
indata = indata.dropna()
indata.index = indata.DATE

indata['Signal'] = indata.ACTIVEQTY
indata.Signal[indata.Signal<0] = -1
indata.Signal[indata.Signal>0] = 1

gg = indata.groupby('ID')

for grp gg.groups.keys():
    itemp = gg.get_group(grp)
    itemp.sort_index()
    itemp1 = itemp.shift(1)
    
    dtemp = itemp[~((itemp.ACTIVEQTY == 0) & (itemp1.ACTIVEQTY == 0))]
    
    


import yfinance as yf
yf.pdr_override()
stock=input("Enter a stock symbol: ")
print(stock)


from scipy import stats

def StaticBeta(Close, indexprice):
    stockRets = Close.pct_change()
    indexRet = indexprice.pct_change()
    
    # Convert index to Series if it's a DataFrame
    if isinstance(indexRet, pd.DataFrame):
        indexRet = indexRet.iloc[:, 0]
    
    betas = {}    
    for stock in stockRets.columns:
        stockData = stockRets[stock].dropna()
        commonDates = stockData.index.intersection(indexRet.index)
        slope, intercept, r_value, p_value, std_err = stats.linregress(indexRet.loc[commonDates], stockData.loc[commonDates])        
        betas[stock] = slope   
    return pd.Series(betas)



 n = len(mu)    
    def objective(w):
        portfolio_return = np.sum(w * mu) + (1 - np.sum(w)) * rf
        portfolio_risk = lambda_risk * 0.5 * np.dot(w.T, np.dot(sigma, w))
        return -(portfolio_return - portfolio_risk)
    
    # Constraints
    constraints = [
        # Leverage constraint: sum of absolute positions <= max_leverage
        {'type': 'ineq', 'fun': lambda w: max_lever - np.sum(np.abs(w))},
        
        # Margin requirement constraint
        {'type': 'ineq', 'fun': lambda w: np.sum(np.maximum(w, 0)) - margin_req * np.sum(np.abs(w))}]
    
    # Position bounds
    bounds = [(min_pos, max_pos) for _ in range(n)]
    
    # Initial guess: equal weights
    w0 = np.array([1/n] * n)
    
    # Optimize with constraints
    result = sco.minimize( objective, w0, method='SLSQP', bounds=bounds, constraints=constraints )


from GetData import BacktestData
#import EqualWeight
#importlib.reload(EqualWeight)
#from EqualWeight import EqualWeight

temp = pandas.read_clipboard()
temp.index = temp.DateTime
del temp['DateTime']
temp.index = pandas.to_datetime(temp.index)
temp.resample('d').last().dropna().to_clipboard()



from pydantic_ai import Agent

agent = Agent(  
    'google-gla:gemini-1.5-flash',
    system_prompt='Be concise, reply with one sentence.',  
)

result = agent.run_sync('Where does "hello world" come from?')  
print(result.output)