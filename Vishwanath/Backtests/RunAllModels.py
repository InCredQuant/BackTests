# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 10:46:22 2023

@author: Viren@Incred
Use: To Run all the Models
"""
import time
import numpy
#import gc

#gc.enable()

# Daily Momentum model
t1 = time.time()
runfile('G:/Shared drives/BackTests/pycode/MainLibs/BloombergPriceUpdaterDB.py', wdir='G:/Shared drives/BackTests/pycode/MainLibs')
runfile('G:/Shared drives/BackTests/pycode/MomentumDailyTrading/BuildData-DailyTradingSignals-Futs.py', wdir='G:/Shared drives/BackTests/pycode/MomentumDailyTrading')
time.sleep(1)
#gc.collect()
runfile('G:/Shared drives/BackTests/pycode/MomentumDailyTrading/DailyTradingSignals_Futs_Dynamic_LiveModel.py', wdir='G:/Shared drives/BackTests/pycode/MomentumDailyTrading')
#gc.collect()

'''
#Monthly Momentum Long Short Models
runfile('G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort/DataBuilderSimpleMomentumLongShort.py', wdir='G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort')
runfile('G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort/6MonthsPriceMomLongShort.py', wdir='G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort')


runfile('G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort/1MonthPriceReversalLongShort.py', wdir='G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort')
runfile('G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort/6MonthsPriceVolatilityLongShort.py', wdir='G:/Shared drives/BackTests/pycode/SimpleMomentumLongShort')

# Monthly Model- Sector Neutral
runfile('G:/Shared drives/BackTests/pycode/SectorialReversalLongShort/DataBuilderSectorialReversalLongShort.py', wdir='G:/Shared drives/BackTests/pycode/SectorialReversalLongShort')
runfile('G:/Shared drives/BackTests/pycode/SectorialReversalLongShort/SectorialReversalLongShort.py', wdir='G:/Shared drives/BackTests/pycode/SectorialReversalLongShort')

t2 = time.time()
print('Completed in: ', numpy.round(t2-t1, 2), 'Sec')
'''