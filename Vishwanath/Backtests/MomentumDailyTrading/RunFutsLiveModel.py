# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 15:29:08 2022

@author: bbg.quant_incredalts
"""


import time
t1  = time.time()
runfile('G:/Shared drives/BackTests/pycode/MainLibs/BloombergPriceUpdaterDB.py', wdir='G:/Shared drives/BackTests/pycode/MainLibs')
runfile('G:/Shared drives/BackTests/pycode/MomentumDailyTrading/BuildData-DailyTradingSignals-Futs.py', wdir='G:/Shared drives/BackTests/pycode/MomentumDailyTrading')
runfile('G:/Shared drives/BackTests/pycode/MomentumDailyTrading/DailyTradingSignals_Futs.py', wdir='G:/Shared drives/BackTests/pycode/MomentumDailyTrading')

t2 = time.time()
print('RunTime-', round((t2-t1)/60, 1), ' Mins!')