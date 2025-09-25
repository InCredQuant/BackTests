import numpy
import datetime
import time
import pandas
import math
import copy
import pdb
import re
import os
import matplotlib.pyplot as plt
import scipy.optimize as sco
from scipy import stats
from scipy import linalg

def NameDecorator(Class):
    oldInit=Class.__init__
    def newinit(self,*arg,**kwarg):
       #self.Name=str(Class)[9:]
       self.Parameters=[]
       self.ParameterValues=[]
       #s=str(Class)[9:]
       s=str(Class)
       s=s[s.find('.')+1:]
       for name,value in kwarg.items():
           self.Parameters.append(name)
           self.ParameterValues.append(value)
           s=s+'_'+name+'_'+str(value)
       self.Name=s
       oldInit(self,*arg,**kwarg)
    Class.__init__=newinit
    return Class

class BackTester():
    ### initialise data and empty matrices to track statistics
    def __init__(self, data):
        self.BackTestData = data
        #expects Close attribute in data.BackTestData
        self.Close = copy.deepcopy(data.Close)
        self.indexprice = copy.deepcopy(data.indexprice)
        self.symbols=self.Close.columns#BackTestData
        self.FromTime= self.Close.index[0]#BackTestData
        if hasattr(self.BackTestData, 'ExpiryDates'):
            self.ExpiryDates = pandas.DatetimeIndex(self.BackTestData.ExpiryDates)
        self.CurrentTime = self.FromTime
        self.EndTime = self.Close.index[-1]#BackTestData
        self.LastTime=self.CurrentTime
        self.TransactionCostRate=0.0003#3 bps by default Transaction Cost
        self.CapitalAllocation=pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.MTM=pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.TradeLog=pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.TradeLogQty=pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.Position=pandas.DataFrame(numpy.zeros_like(self.Close),index=self.Close.index,columns=self.Close.columns)
        self.TempCapitalAllocation=[]
        self.TempQuantityAllocation=[]
        self.Dates=self.BackTestData.Close.index
        #self.Iterator=self.BackTestData.Close.iterrows()
        self.Counter=-1
        self.NAV=pandas.DataFrame(columns=['NAV'],index=self.Close.index).fillna(100)
        self.TransactionCost=pandas.DataFrame(columns=['TransactionCost'],index=self.Close.index).fillna(0)
        if hasattr(self, 'TradedValue'):
            self.StartingNAV = self.TradedValue
        else:
            self.StartingNAV = 100
        self.NAV['NAV'][self.CurrentTime] = self.StartingNAV        
        self.AllOrders = []
        self.OptionTickerRegEx = r'(?P<symbol>[A-Z&]+(\-[A-Z&]+)?)(?P<expiry_date>\d+[A-Z]+\d+)(?P<option_type>[A-Z]+)(?P<strike>\d+(\.\d+)?)'# being used for Options backtesting
        self.PairTrades = False# Being used for Market Neutral (Long Short) or Pair trades, where Exposure is counted one side from the both sides positions

    ## main loop
    def run(self):
        #Called once only in beginning
        self.basicdatainitialize()
        if self.CurrentTime > self.FromTime:
            self.CurrentTime = max(self.Dates[self.Dates < self.CurrentTime])
            self.Counter = list(self.Dates).index(self.CurrentTime)
            self.StartCounter = self.Counter
            self.FromTime = self.CurrentTime
            
        while self.CurrentTime < self.EndTime :
            self.moveonesteptime()
        
        self.ResultFrameWithIndex()
        return self

    ## increment of loop(time ++)

    def GetAllUpdateDates(self, days):
        self.UpdateDates = []
        self.UpdateDates.append(min(self.Dates[self.Dates >= self.CurrentTime]))
        temptime = self.CurrentTime + datetime.timedelta(days)            
        for i in range(len(self.Dates)):
            if self.Dates[i] >= temptime and self.Dates[i-1] <= temptime:# and temptime <=  datetime.datetime.today().date():
                self.UpdateDates.append(self.Dates[i])
                temptime = self.Dates[i] + datetime.timedelta(days)
                #temptime = temptime + datetime.timedelta(days)
            
    def GetAllRebalanceTimes(self, timeDiff): # Required if intraday nbacktests have to be run
        self.RebalanceTimes = []
        self.IndexTimes = self.indexprice.loc[self.CurrentTime - datetime.timedelta(minutes = 1000*timeDiff):, :].index       
        self.RebalanceTimes.append(min(self.IndexTimes[self.IndexTimes >= self.CurrentTime]))
        
        temptime = self.CurrentTime + datetime.timedelta(minutes = timeDiff)            
        for i in range(len(self.IndexTimes)):
            if self.IndexTimes[i] >= temptime and self.IndexTimes[i-1] <= temptime:# and temptime <=  datetime.datetime.today().date():
                self.RebalanceTimes.append(self.IndexTimes[i])
                temptime = self.IndexTimes[i] + datetime.timedelta(minutes = timeDiff)
                #temptime = temptime + datetime.timedelta(days)
        
        temptime = self.RebalanceTimes[0] - datetime.timedelta(minutes = timeDiff)
        N = len(self.IndexTimes)
        for i in range(N-1, 0, -1):            
            if self.IndexTimes[i-1] <= temptime and self.IndexTimes[i] >= temptime:# and temptime <=  datetime.datetime.today().date():
                self.RebalanceTimes.insert(0, self.IndexTimes[i-1])
                temptime = self.IndexTimes[i-1] - datetime.timedelta(minutes = timeDiff)
            
    def GetCurrentCapitalAllocation(self, myarg):
        if type(myarg) is str:
            myarg = pandas.to_datetime(myarg)        
        usedate = max(self.Dates[self.Dates <= self.CurrentTime])
        currentallocation = self.CapitalAllocation.ix[usedate][self.CapitalAllocation.ix[myarg] != 0]
        return currentallocation
    
    def GetLatestIndexComponents(self):
        return self.BackTestData.indexcomponents[max([i for i in self.BackTestData.indexcomponents.keys() if i < self.CurrentTime])]
        

    def moveonesteptime(self):        
        self.Counter +=1
        self.LastTime=self.CurrentTime
        self.CurrentTime=self.Dates[self.Counter]
        #self.CurrentTime=self.Iterator.next()[0]
        if hasattr(self, 'StrategyType') and self.StrategyType == 'OP':
            self.updatePnLandNAVhandlerOptions()
        else:
            self.updatePnLandNAVhandler()
        self.declarecurrentvariables()
        #self.StopLossHandler()
        #pdb.set_trace()
        if self.detectupdatedate():
            self.UpdateSpecificStats()
            self.updateCapAllocation()
        
    def ResultFrameWithIndex(self):
        if hasattr(self.BackTestData,'indexprice'):
            tempframe = pandas.concat([self.NAV, self.indexprice.fillna(method = 'pad')], axis = 1)
            for i in range(len(self.NAV)-1):
                if self.NAV.iloc[i+1].values != self.StartingNAV:
                    self.PlotResult = tempframe.loc[self.NAV[i:].index[0]:]
                    break
            self.PlotResult = self.PlotResult.ffill()
            if hasattr(self,'PlotResult'):
                self.PlotResult = self.PlotResult / self.PlotResult.iloc[0]            
#            self.orderhandler()

    ## calculate nav, transaction cost, mtm etc
    def updatePnLandNAVhandler(self):
        #pdb.set_trace()
        if len(self.TempCapitalAllocation)!=0:
             # carry forward last position by default
            self.Position.loc[self.CurrentTime] = self.Position.loc[self.LastTime]
            # get capital allocation of last loop
            self.YestDayEnd = copy.deepcopy(self.CapitalAllocation.loc[self.LastTime])
            # temp capital allocation save current capital allocation while yestdayend is capital allocation at last time but querried at present
            # so yestdayend carries captial allocation post trade while tempcap alloc carries capital allocations before any trading
            # their difference amounts to traded value
            self.TradeLog.loc[self.LastTime,:]= numpy.subtract(self.YestDayEnd,self.TempCapitalAllocation)
            # today before trade is change is allocated capital due to price change. present value of allocated capital due to effect of market activity
            LongCurrentPriceRatio = numpy.divide(self.Close.loc[self.CurrentTime],self.Close.loc[self.LastTime])
            CurrentPriceRatio = numpy.divide(self.Close.loc[self.LastTime], self.Close.loc[self.CurrentTime])
            LongCurrentPriceRatio = LongCurrentPriceRatio.where(~numpy.isnan(LongCurrentPriceRatio), other = 1)#, inplace =True)#CurrentPriceRatio.where(~numpy.isnan(CurrentPriceRatio), other = 1)#
            CurrentPriceRatio = CurrentPriceRatio.where(~numpy.isnan(CurrentPriceRatio), other = 1)#, inplace = True)
            
            CurrentPriceRatio = CurrentPriceRatio.where(self.Position.loc[self.LastTime] ==-1, LongCurrentPriceRatio)#, inplace = True)
            
            self.TodayBeforeTrade = numpy.multiply(self.CapitalAllocation.loc[self.LastTime],CurrentPriceRatio)
            # update capital allocation for today
            #if self.CurrentTime == datetime.datetime(2013, 6, 30):
            #    pdb.set_trace()
            self.CapitalAllocation.loc[self.CurrentTime,:]=(self.TodayBeforeTrade)
            # mtm due to last days capital allocation
            self.MTM.loc[self.CurrentTime,:]=numpy.subtract(numpy.abs(self.TodayBeforeTrade),numpy.abs(self.YestDayEnd))
            self.TransactionCost.loc[self.CurrentTime,'TransactionCost']=numpy.sum((numpy.abs(self.TradeLog.loc[self.LastTime])))*(self.TransactionCostRate)
            self.NAV.loc[self.CurrentTime,'NAV']=self.NAV.loc[self.LastTime,'NAV'] + numpy.sum(self.MTM.loc[self.CurrentTime]) -self.TransactionCost.loc[self.CurrentTime,'TransactionCost']
        self.TempCapitalAllocation = copy.deepcopy(self.CapitalAllocation.loc[self.CurrentTime])
#        self.drawdownstoplosshandler()


    def savebacktestresult(self, filepath, fullData = True):
        t1 = time.time()
        #pdb.set_trace()
        
        writer = pandas.ExcelWriter(filepath, engine = 'xlsxwriter')
        if fullData:
            self.BackTestData.Close.to_excel(writer,'Prices')#, engine = 'xlsxwriter')
            self.MTM.to_excel(writer,'MTM')#, engine = 'xlsxwriter')
            self.TradeLog.to_excel(writer,'TradeLog')#, engine = 'xlsxwriter')
            self.TransactionCost.to_excel(writer,'TransactionCost')#, engine = 'xlsxwriter')
            self.CapitalAllocation.to_excel(writer,'Capital Allocation')#, engine = 'xlsxwriter')
            if hasattr(self, 'Quantity'):
                self.Quantity.to_excel(writer,'Quantity')#, engine = 'xlsxwriter')
                self.Exposure.to_excel(writer,'Exposure')
            
        self.NAV.to_excel(writer,'NAV')#, engine = 'xlsxwriter')
        self.Position.to_excel(writer,'Position')#, engine = 'xlsxwriter')        
        self.Churn = pandas.DataFrame(self.TradeLog[self.TradeLog >0].sum(axis = 1), columns = ['NAV'])/self.NAV
        self.Churn.columns = ['Churn']
        self.Weight = numpy.divide(self.CapitalAllocation, self.NAV)
        latestPos = self.Weight.iloc[-1]#.transpose()
        latestPos = latestPos[latestPos !=0]
        latestPos = latestPos.sort_values()
        latestPos.to_excel(writer, 'LastPosition')
        if hasattr(self, 'Weights'):
            self.Weights.to_excel(writer,'Weights')#, engine = 'xlsxwriter')
        if hasattr(self, 'PositionWOSL'):
            #pdb.set_trace()
            self.PositionWOSL.to_excel(writer,'PositionWOSL')
        
        if hasattr(self, 'FactorRanks'):
            self.FactorRanks.to_excel(writer,'FactorRanks')
            
        if hasattr(self, 'TradedValue') and hasattr(self.BackTestData, 'LotSize'):
            lotSize = self.BackTestData.LotSize.iloc[-2:, :]
            Price = self.Close.iloc[-2:, :]# Ideally this price should be Next Month Fut price during expiry date
            RecentPositionValue = self.Weight.iloc[-2:, :]*self.TradedValue
            LotValue = numpy.multiply(lotSize, Price)
            self.Lots = numpy.round(numpy.divide(RecentPositionValue, LotValue), 0)
            #self.LotsR = numpy.round(self.Lots, 0)
            ActualValue = numpy.multiply(numpy.multiply(self.Lots, Price), lotSize)
            NetPosition = ActualValue[ActualValue.columns[~ActualValue.columns.isin(['Cash', self.IndexName])]].sum(axis = 1)            
            self.Lots.loc[:, self.IndexName] = numpy.round(numpy.divide(-NetPosition, LotValue.loc[:, self.IndexName]), 0)
            Chg = pandas.DataFrame(self.Lots.diff().iloc[-1])
            Chg.columns = ['Trades']
            Chg['LotSize'] = lotSize.iloc[-1]
            Chg['Transaction'] = numpy.multiply(Chg['Trades'], Chg['LotSize'])
            self.Lots = self.Lots.transpose()
            self.Lots = pandas.concat([self.Lots, Chg], axis = 1)
            self.Lots['Ticker'] = self.Lots.index            
            self.Lots = self.Lots.loc[self.Lots.index.isin(self.modelTable.index), :]
            self.Lots['Symbol'] = [self.modelTable.loc[i, 'NSE'] for i in self.Lots.index]
            self.Lots['Buy/Sell'] = [1 if i> 0 else i for i in self.Lots['Transaction']]
            self.Lots['Buy/Sell'] = [2 if i< 0 else i for i in self.Lots['Buy/Sell']]
            self.Lots['Quantity'] = self.Lots['Transaction'].abs()
            self.Lots['Price'] = Price.iloc[-1]
            self.Lots['Instrument Name'] = ['FUTIDX' if i == 'NIFTY' else 'FUTSTK' for i in self.Lots['Symbol']]
            self.Lots.to_excel(writer, 'Transactions')
        #self.Churn.to_excel(writer,'Churn')
        try:
          writer.save()
        except:
          writer.close()
        t2 = time.time()
        print('File Saved in:', round((t2-t1)/60, 1), 'Mins', sep = ' ')

    def SavePlotsData(self, dirPath, backtestName, fullData = True):
        if not os.path.exists(os.path.join(dirPath,'Files/')):
            os.mkdir(os.path.join(dirPath,'Files/'))
        
        backtestName = backtestName.replace(', ', '_').replace(' ', '_')
        self.savebacktestresult(os.path.join(dirPath,'Files/'+ backtestName+'.xlsx'), fullData = fullData)        
        navData = self.PlotResult.dropna()
        navData = navData.rename(columns=lambda x: x.replace('NAV', backtestName))
        
        yrsDiff = (navData.index[-1] - navData.index[0]).days/365.0
        cagrRet = (navData.iloc[-1])**(1/yrsDiff) -1
        AverageChurn = int(100*self.Churn.resample('a').sum().mean())
        titlename = 'CAGR-'+ str(["%.1f" % i for i in (cagrRet.values*100)]) + ',Ann.Churn-'+str(AverageChurn)+'%, ' +backtestName
        
        if not os.path.exists(os.path.join(dirPath,'Figures/')):
            os.mkdir(os.path.join(dirPath,'Figures/'))
            
        navData.plot(title = titlename, figsize =(18,6))
        plt.savefig(os.path.join(dirPath,'Figures/'+backtestName+'_NAV.jpg')) 
        
        temp = navData.pct_change(252)
        temp = 100*(temp[temp.columns[0]] - temp[temp.columns[1]]).dropna()
        temp = pandas.DataFrame(temp)
        temp.columns = ['Rolling 1Yr Returns ' + titlename]
        temp['X-Axis'] = 0
        temp.plot(title = titlename, figsize =(18,6))
        plt.savefig(os.path.join(dirPath,'Figures/'+backtestName+'_RR.jpg'))
        
        tmp = navData.resample('a').last()
        tmp = tmp.pct_change()
        tmp.index = tmp.index.year
        tmp.plot(kind = 'bar', title = titlename, figsize =(18,6))
        plt.savefig(os.path.join(dirPath,'Figures/'+backtestName+'_Y_Plot.jpg'))
        
        tmp2 = navData.resample('q').last()
        tmp2 = tmp2.pct_change()
        tmp2.index = [i.strftime('%b-%y') for i in tmp2.index]
        tmp2.plot(kind = 'bar', title = titlename, figsize =(18,6))
        plt.savefig(os.path.join(dirPath,'Figures/'+backtestName+'_Q_Plot.jpg'))

    def calculateBeta(self, window=252):
        # Calculate daily returns for stocks and index
        stockRets = self.Close.pct_change()
        indexRet = self.indexprice.pct_change()
    
        # Initialize a DataFrame to store rolling beta values
        self.Beta = pandas.DataFrame(index=self.Close.index, columns=self.Close.columns)
    
        # Calculate rolling beta for each stock
        for stock in self.Close.columns:
            # Covariance between stock returns and index returns
            cov = stockRets[stock].rolling(window).cov(indexRet)
            # Variance of index returns
            var = indexRet.rolling(window).var()
            self.Beta[stock] = cov / var

    def StaticBeta(self, Close, indexprice):
      stockRets = Close.pct_change()
      indexRet = indexprice.pct_change()
      
      # Convert index to Series if it's a DataFrame
      if isinstance(indexRet, pandas.DataFrame):
          indexRet = indexRet.iloc[:, 0]
      
      betas = {}    
      for stock in stockRets.columns:
          stockData = stockRets[stock].dropna()
          commonDates = stockData.index.intersection(indexRet.index)
          slope, intercept, r_value, p_value, std_err = stats.linregress(indexRet.loc[commonDates], stockData.loc[commonDates])        
          betas[stock] = slope   
      return pandas.Series(betas)
  
    def MeanVarianceOptimization(self, mu, sigma, rf, lambda_risk):
      """
      Calculate optimal portfolio weights using mean-variance optimization (it is without any Constraints or bounds)    
      Parameters:
      -----------
      mu : numpy.array
          Expected returns vector
      sigma : numpy.array
          Covariance matrix
      rf : float
          Risk-free rate
      lambda_risk : float
          Risk aversion parameter
          
      Returns:
      --------
      numpy.array
          Optimal portfolio weights
      """
      n = len(mu)    
      def objective(w):
          portfolio_return = numpy.sum(w * mu) + (1 - numpy.sum(w)) * rf
          portfolio_risk = lambda_risk * 0.5 * numpy.dot(w.T, numpy.dot(sigma, w))
          return -(portfolio_return - portfolio_risk)
          
      # Initial guess: equal weights
      w0 = numpy.array([1/n] * n)    
      # Optimize
      result = sco.minimize(objective, w0, method='SLSQP')  # SLSQP, is used for Quadratic Optimization 
      return result.x

    def ConstrainedMeanVarianceOptimizer(self, mu, sigma, rf, lambda_risk, margin_req=0.5, max_lever=1.5, min_pos=-0.1, max_pos=0.3):
      """
      Calculate optimal portfolio weights using mean-variance optimization with constraints      
      Parameters:
      -----------
      mu : numpy.array
          Expected returns vector
      sigma : numpy.array
          Covariance matrix
      rf : float
          Risk-free rate
      lambda_risk : float
          Risk aversion parameter
      margin_req : float, optional (default=0.5)
          Margin requirement as a fraction of position value
      max_lever : float, optional (default=1.5)
          Maximum allowed leverage
      min_pos : float, optional (default=-0.1)
          Minimum allowed position size (-0.1 = 10% short)
      max_pos : float, optional (default=0.1)
          Maximum allowed position size (0.1 = 10% long)
      
      Returns:
      --------
      numpy.array
          Optimal portfolio weights
      """
      n = len(mu)    
      def objective(w):
          portfolio_return = numpy.sum(w * mu) + (1 - numpy.sum(w)) * rf
          portfolio_risk = lambda_risk * 0.5 * numpy.dot(w.T, numpy.dot(sigma, w))
          return -(portfolio_return - portfolio_risk)      
      # Constraints
      constraints = [
          # Leverage constraint: sum of absolute positions <= max_leverage
          {'type': 'ineq', 'fun': lambda w: max_lever - numpy.sum(numpy.abs(w))},          
          # Margin requirement constraint
          {'type': 'ineq', 'fun': lambda w: numpy.sum(numpy.maximum(w, 0)) - margin_req * numpy.sum(numpy.abs(w))}
      ]
      
      # Position bounds
      bounds = [(min_pos, max_pos) for _ in range(n)]      
      # Initial guess: equal weights
      w0 = numpy.array([1/n] * n)      
      # Optimize with constraints
      result = sco.minimize( objective, w0, method='SLSQP', bounds=bounds, constraints=constraints )      
      if not result.success:
          print(f"Warning: Optimization did not converge. Status: {result.message}")
      return result.x
  
    def SharpeRatioOptimization(self, mu, sigma, rf):
      """
      Calculate weights for maximum Sharpe ratio portfolio, without Any constrainst  or bounds
      """
      n = len(mu)    
      def sharpe_ratio(w):
          port_ret = numpy.sum(w * mu) + (1 - numpy.sum(w)) * rf
          port_vol = numpy.sqrt(numpy.dot(w.T, numpy.dot(sigma, w)))
          return -(port_ret - rf) / port_vol  # Negative for maximization        
      result = sco.minimize(sharpe_ratio, numpy.array([1/n]*n), method='SLSQP') 
      return result.x

    def ConstrainedSharpeRatioOptimization(self, mu, sigma, rf, margin_req=0.5, max_lever=1.5, min_pos=-0.1, max_pos=0.3):
      """
      Calculate weights for maximum Sharpe ratio portfolio, without constraints and bounds
      """
      n = len(mu)    
      def sharpe_ratio(w):
          port_ret = numpy.sum(w*mu) + (1 - numpy.sum(w))*rf
          port_vol = numpy.sqrt(numpy.maximum(numpy.dot(w.T, numpy.dot(sigma, w)), 1e-5))#numpy.sqrt(numpy.dot(w.T, numpy.dot(sigma, w)))
          return -(port_ret - rf) / port_vol  # Negative for maximization      
      # Constraints
      constraints = [
          # Leverage constraint: sum of absolute positions <= max_leverage
          {'type': 'ineq', 'fun': lambda w: max_lever - numpy.sum(numpy.abs(w))},          
          # Margin requirement constraint
          {'type': 'ineq', 'fun': lambda w: numpy.sum(numpy.maximum(w, 0)) - margin_req * numpy.sum(numpy.abs(w))}
      ]      
      # Position bounds
      bounds = [(min_pos, max_pos) for _ in range(n)]      
      # Initial guess: equal weights
      w0 = numpy.array([1/n] * n)      
      # Optimize with constraints
      result = sco.minimize( sharpe_ratio, w0, method='SLSQP', bounds=bounds, constraints=constraints )      
      if not result.success:
          print(f"Warning: Optimization did not converge. Status: {result.message}")
      return result.x

    def BlackLittermanOptimizer(self, prior_weights, sigma, rf, lambda_risk, views, view_confidences, tau=0.025, margin_req=0.5, max_lever=1.5, min_pos=-0.1, max_pos=0.3):
      """
      Implements Black-Litterman model with constrained mean-variance optimization      
      Parameters:
      -----------
      prior_weights : numpy.array
          Prior Weights of the Assets
      sigma : numpy.array
          Historical covariance matrix
      rf : float
          Risk-free rate
      lambda_risk : float
          Risk aversion parameter
      views : numpy.array
          Matrix P of investor views (k x n), where k is number of views
          Each row represents a view: weights summing to 0 for relative views
          and 1 for absolute views
      view_confidences : numpy.array
          Vector of confidence levels (omega) for each view
      tau : float
          Uncertainty parameter (typically 0.025-0.05)#by default 0.025
      margin_req : float
          Margin requirement as fraction of position value
      max_lever : float
          Maximum allowed leverage
      min_pos, max_pos : float
          Position size limits
          
      Returns:
      --------
      numpy.array
          Optimal portfolio weights based on Black-Litterman views
      dict
          Additional metrics including implied returns, posterior returns,
          and uncertainty measures
      """
      n = len(prior_weights)
      # 1. Calculate Prior returns or Market returns also if taken initial index weights
      pi = lambda_risk * numpy.dot(sigma, prior_weights)
      
      # 2. Incorporate views using Black-Litterman
      omega = numpy.diag(view_confidences)
      tau_sigma = tau * sigma
      
      # Calculate posterior parameters
      temp = linalg.inv(numpy.dot(numpy.dot(views, tau_sigma), views.T) + omega)
      ts_v = numpy.dot(tau_sigma, views.T)
      #post_sigma = tau_sigma - numpy.dot(numpy.dot(ts_v, temp), ts_v.T)
      post_returns = pi + numpy.dot(numpy.dot(ts_v, temp), views.dot(prior_weights) - numpy.dot(views, pi))
      
      # 3. Use posterior parameters in constrained optimization
      def objective(w):
          port_return = numpy.sum(w * post_returns) + (1 - numpy.sum(w)) * rf
          port_risk = lambda_risk * 0.5 * numpy.dot(w.T, numpy.dot(sigma, w))
          return -(port_return - port_risk)
      
      constraints = [ {'type': 'ineq', 'fun': lambda w: max_lever - numpy.sum(numpy.abs(w))},
          {'type': 'ineq', 'fun': lambda w: numpy.sum(numpy.maximum(w, 0)) - margin_req * numpy.sum(numpy.abs(w))} ]
      
      bounds = [(min_pos, max_pos) for _ in range(n)]   
      #w0 = numpy.array([1/n] * n)#Starting weights
      # Optimize
      result = sco.minimize( objective, prior_weights, method='SLSQP', bounds=bounds, constraints=constraints )
      return result.x

    def CalculateStats(self, navData, rf):
      statsDF = pandas.DataFrame()
      yrsDiff = (navData.index[-1] - navData.index[0]).days / 365.25
      statsDF['CAGR'] = 100*((navData.iloc[-1]/navData.iloc[0])**(1/yrsDiff) -1)
      statsDF['Ann. Vol.'] = 100*(navData.pct_change().std()*numpy.sqrt(252))
      statsDF['Max DD'] = 100*(((navData/navData.expanding().max()) -1).min())
      statsDF['Sharpe'] = (statsDF['CAGR'] - 100*rf)/statsDF['Ann. Vol.']
      statsDF['Beta'] = self.StaticBeta(navData, self.indexprice)
      return statsDF
  
    def basicdatainitialize(self):
        #within self.BackTestData
        pass

    def StopLossHandler(self):
        #within self.BackTestData
        pass

    def declarecurrentvariables(self):
        pass


    def detectupdatedate(self):
        pass
        #should return a bool val

    def UpdateSpecificStats(self):
        pass

    def updateCapAllocation(self):
        pass
