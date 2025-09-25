import pandas
import copy
import numpy
import pandas as pd
import numpy as np
import numba as nb
import datetime
import scipy.stats
#from talibdecoratorNew import talibonDataFrame
from scipy.stats import linregress
from sklearn.linear_model import LinearRegression

def Stochastics(Data, Days):
    def StochasticsSinglePeriod(arr):
        return 100*(arr[-1]-numpy.min(arr))/(numpy.max(arr) - numpy.min(arr))
    return Data.rolling(window = Days, min_periods = Days).apply(StochasticsSinglePeriod)
#pandas.rolling_apply(Data,Days,StochasticsSinglePeriod,min_periods = Days)

def WithRespectToHighLow(Data,Days):
    CloseMean = Data.Close.rolling(window = Days, min_periods = Days).apply(numpy.mean)#pandas.rolling_mean(Data.Close,Days,min_periods = Days)
    HighMean = Data.High.rolling(window = Days, min_periods = Days).apply(numpy.mean)#pandas.rolling_mean(Data.High,Days,min_periods = Days)
    LowMean = Data.Low.rolling(window = Days, min_periods = Days).apply(numpy.mean)#pandas.rolling_mean(Data.Low,Days,min_periods = Days)
    return (HighMean-CloseMean)/(HighMean-LowMean)

def MovingAverage(Data,Days, ShiftDays=0):
    return (Data.rolling(window = Days, min_periods = Days).apply(numpy.mean)).shift(ShiftDays)
#(pandas.rolling_mean(Data,Days,min_periods = Days)).shift(ShiftDays)

def GetEMA(Data, Days , ShiftDays=0):
    return (Data.ewm(span = Days).mean()).shift(ShiftDays)
#(pandas.ewma(Data, span=Days)).shift(ShiftDays)

def GetWMA(Data, Days, ShiftDays=0):
    weights = numpy.array(range(1, Days+1))
    weights = weights/weights.sum()
    return Data.rolling(Days).apply(lambda x: numpy.sum(weights*x)).shift(ShiftDays)

def MovingStd(Data,Days):
    return Data.rolling(window = Days, min_oeriods = Days).apply(numpy.std)
#pandas.rolling_std(Data,Days,min_periods = Days)



def BollingerBands(Data,Days,NsigmaUp,NsigmaDown):
    Middle = MovingAverage(Data,Days)
    var = MovingStd(Data,Days)
    Up = Middle + NsigmaUp * var
    Down = Middle - NsigmaDown * var
    return (Up,Middle,Down)

def GetMFI(Data,Days):
    TypicalPrice = (Data.Close + Data.High + Data. Low)/3
    Sign = numpy.sign(TypicalPrice.diff())
    TypPriceSign = TypicalPrice * Sign
    MoneyFlow = TypPriceSign * Data.Volume
    def MFITemp(arr):
        PositiveFlow = numpy.sum(arr[arr >0])
        NegativeFlow = numpy.abs(numpy.sum(arr[arr <0]))
        Ratio = PositiveFlow / NegativeFlow
        Index = 100 - (100/(1+Ratio))
        return Index
    return MoneyFlow.rolling(window = Days, min_periods = Days).apply(MFITemp)
#pandas.rolling_apply(MoneyFlow, Days, MFITemp ,min_periods = Days)

def ADX(Data,Days):
    c1 = Data.Close.shift(1)
    l1 = Data.Low.shift(1)
    h1 = Data.High.shift(1)
    m1 = numpy.abs(Data.High - Data.Low)
    m2 = numpy.abs(Data.High - c1)
    m3 = numpy.abs(Data.Low - c1)
    TrueRange = numpy.maximum(m1,numpy.maximum(m2,m3))
    #pdb.set_trace()
    DiffinLow = l1 - Data.Low
    DiffinHigh = Data.High - h1
    PDM =  ((DiffinHigh>DiffinLow) * (DiffinHigh>0)) * DiffinHigh
    MDM =  ((DiffinHigh<DiffinLow) * (DiffinLow>0)) * DiffinLow
    SmoothTR = MovingAverage(TrueRange , Days)
    SmoothPDM = MovingAverage(PDM , Days)
    SmoothMDM = MovingAverage(MDM , Days)
    PDI = 100*SmoothPDM / SmoothTR
    MDI = 100*SmoothMDM / SmoothTR
    DX = numpy.abs(PDI - MDI) / (PDI + MDI)
    ADX = 100*MovingAverage(DX, Days)
    return (PDI, MDI, ADX)


def PSAR(Data, Acc, AccMax):
    def PSARonArray(high, low, close, Acc ,AccMax):
            PSAR = numpy.zeros_like(close)
            #pdb.set_trace()
            DirectionPresent = numpy.sign(close[1]-close[0])
            if DirectionPresent==1:
                PredictedSARPresent = min(low[1],low[0])
                ExtremePointPresent = max(high[1],high[0])
                AccPresent = Acc
                temp = PredictedSARPresent + (ExtremePointPresent - PredictedSARPresent)*AccPresent
                PredictedSARFuture = min(min(low[1],low[0]), temp)
            else:
                PredictedSARPresent = max(high[1],high[0])
                ExtremePointPresent = min(low[1],low[0])
                AccPresent = Acc
                temp = PredictedSARPresent + (ExtremePointPresent - PredictedSARPresent)*AccPresent
                PredictedSARFuture = max(max(high[1],high[0]), temp)
            PSAR[1] = PredictedSARFuture
            for i in range(2,len(close)):
                PredictedSARPresent = PredictedSARFuture
                DirectionPast = DirectionPresent
                ExtremePointPast = ExtremePointPresent
                AccPast = AccPresent
                if DirectionPast>0:
                    if low[i] < PredictedSARPresent:
                        DirectionPresent = -1
                    else:
                        DirectionPresent = 1
                else:
                    if high[i] > PredictedSARPresent:
                        DirectionPresent = 1
                    else:
                        DirectionPresent = -1
                if DirectionPresent == DirectionPast:
                     if DirectionPresent == 1:
                         ExtremePointPresent = max(high[i], ExtremePointPast)
                     else:
                         ExtremePointPresent = min(low[i], ExtremePointPast)
                else:
                     if DirectionPresent == 1:
                         ExtremePointPresent = high[i]
                     else:
                         ExtremePointPresent = low[i]
                if DirectionPresent == DirectionPast and not(ExtremePointPresent == ExtremePointPast):
                    AccPresent = min(AccPresent + Acc, AccMax)
                else:
                    if DirectionPresent == DirectionPast:
                        AccPresent = AccPast
                    else:
                        AccPresent = Acc
                if not(DirectionPresent == DirectionPast):
                     PredictedSARFuture = ExtremePointPast
                else:
                     if DirectionPresent == 1:
                         temp = PredictedSARPresent + (ExtremePointPresent - PredictedSARPresent)*AccPresent
                         PredictedSARFuture = min(min(low[i],low[i-1]), temp)
                     else:
                         temp = PredictedSARPresent + (ExtremePointPresent - PredictedSARPresent)*AccPresent
                         PredictedSARFuture = max(max(high[i],high[i-1]), temp)
                PSAR[i] = PredictedSARFuture
            return PSAR
    PSARfunc = talibonDataFrame(PSARonArray)
    (value,) = PSARfunc(Data,['High','Low','Close'], Acc, AccMax)
    return value
#su/sd is average or sum ? difference in close prices or returns? different in mAtlab and on net
def CMO(Data, Days):
    def CMOtemp(arr):
        su=numpy.average(arr[arr>0])
        sd=-numpy.average(arr[arr<0])
        return 100*(su-sd)/(su+sd)
    return (Data.Close.pct_change()).rolling(window = Days, min_periods = Days).apply(CMOtemp)
#pandas.rolling_apply(Data.Close.pct_change(), Days, CMOtemp,  min_periods = Days )

def CCI(Data, Days, Parameter = 0.15):
    TP = (Data.Close + Data.High +Data.Low) / 3
    def CCItemp(arr, parameter):
        #pdb.set_trace()
        ma = numpy.average(arr)
        meandev = numpy.average(numpy.abs(arr - ma))
        return (arr[-1] - ma)/(Parameter*meandev)
    return TP.rolling(window = Days, min_peiods = Days).apply(lambda x:CCItemp(x,Parameter))
#pandas.rolling_apply(TP, Days, lambda x:CCItemp(x,Parameter), min_periods = Days)

def Aroon(Data, Days):
    def AroonTemp(arr):
        return 100*(arr.argmax()-arr.argmin())/Days
    return Data.Close.rolling(window = Days, min_periods = Days).apply(AroonTemp)
#pandas.rolling_apply(Data.Close, Days, AroonTemp, min_periods = Days)

def MACD(Data, DaysSlow = 26, DaysFast = 12, SignalPeriod = 9):
    FastEma = GetEMA(Data, DaysFast)
    SlowEma = GetEMA(Data, DaysSlow)
    MACD = FastEma - SlowEma
    Signal = GetEMA(MACD, SignalPeriod )
    return (MACD, Signal)

def GetDynamicEMA(data,factor):
    result=pandas.DataFrame(numpy.zeros_like(data), index = data.index, columns = data.columns, dtype=float)
    data = data.fillna(method = 'bfill')
    factor = factor.fillna(0)
    for i in range(len(data)):
        if i == 0:
            result.ix[0] = data.ix[0]
        else:
            result.iloc[i]=(factor.iloc[i].values * data.iloc[i].values) + (1.0-factor.iloc[i].values)*result.iloc[i-1].values
    return result

def GetPVT(Data):
    ClosePctChange = Data.Close.pct_change()
    temp = ClosePctChange * Data.Volume
    temp.iloc[0] = Data.Volume.iloc[0]
#    return temp  + temp.shift(1).fillna(0)
    return temp.cumsum()

def GetROC(Data, Days):
    return 100*(Data.Close - Data.Close.shift(Days))/Data.Close.shift(Days)

def GetMovingVolatility(data,days):
    ret=data.pct_change()+1
    ret=ret.fillna(1)
    ret=numpy.log(ret)
    ret=ret.fillna(0)
#    return pandas.rolling_std(ret, 20, min_periods = 20)*sqrt(252)
    
    temp=pandas.Series(ret.index,index=ret.index)
    temp=temp.diff()
    temp[0] = 0
#    mymultiplier = 365 / temp.median().days
    if pandas.__version__ == '0.13.1':
        mymultiplier = 365 / (int(temp.median().values/(86400*10**9)))
    else:
        mymultiplier = 365 / temp.median().days
        
    return ret.rolling(window = days, min_periods = days).apply(numpy.std)*numpy.sqrt(mymultiplier)
#pandas.rolling_std(ret, days, min_periods = days)*numpy.sqrt(mymultiplier)
    
#    if type(temp.ix[-1]) == numpy.timedelta64:
#        temp = temp.apply(lambda x:float(x)/(10**9))
#    elif type(temp.ix[-1]) == datetime.timedelta:
#        temp=temp.apply(lambda x:x.total_seconds())
#    p = pandas.Series.median(temp,days)
#    if p/(24*3600.0) >= 1:
#        p = p / (24*3600)
#    else:
#        p = p/ (6.25*3600)
#    temp=numpy.sqrt(252/p)
#    std=pandas.rolling_apply(ret,days,lambda x:numpy.std(x,axis=0,ddof=1),min_periods=days)
#    return std.apply(lambda x:x*temp)

# def GetRSI(Data, Days):
#     def RSITemp(arr):
#         gain = numpy.average(arr[arr>0])
#         loss = -numpy.average(arr[arr<0])
#         RS= gain / loss
#         if not(numpy.isnan(gain) or numpy.isnan(loss)) or ((numpy.isnan(gain) and numpy.isnan(loss))):# checks for if both are not or nan, if yes
#             rsi = 100 - (100/(1+RS))
#         elif numpy.isnan(gain):
#             rsi = 0
#         elif numpy.isnan(loss):
#             rsi = 100
#         return rsi
#     return (Data.diff()).rolling(window = Days, min_periods = Days).apply(RSITemp)
# #pandas.rolling_apply(Data.diff(), Days, RSITemp, min_periods = Days)




#--------------------------------------------------------------
@nb.jit(fastmath=True, nopython=True)
def calc_rsi( array, deltas, avg_gain, avg_loss, n ):
    # Use Wilder smoothing method
    up   = lambda x:  x if x > 0 else 0
    down = lambda x: -x if x < 0 else 0
    i = n+1
    if avg_loss != 0:
        array[n] = 100 - (100 / (1 + (avg_gain / avg_loss)))
    else:
        array[n] = 100
    for d in deltas[n+1:]:
        avg_gain = ((avg_gain * (n-1)) + up(d)) / n
        avg_loss = ((avg_loss * (n-1)) + down(d)) / n
        if avg_loss != 0:
            rs = avg_gain / avg_loss
            array[i] = 100 - (100 / (1 + rs))
        else:
            array[i] = 100
        i += 1
    return array

def GetRSI(data, n = 14 ):
    all_frames = []
    col_names = data.columns
    for col in col_names:
        array = data[col].dropna()
        if len(array)==0:
            output = pd.Series(numpy.nan, index=array.index)
            output.name = col
            all_frames.append(output)
            continue
        #array = array.dropna()#(inplace=True)
        temp_idx = array.index
        deltas = np.append([0],np.diff(array))
        avg_gain =  np.sum(deltas[1:n+1].clip(min=0)) / n
        avg_loss = -np.sum(deltas[1:n+1].clip(max=0)) / n
        array = np.empty(deltas.shape[0])
        array.fill(np.nan)
        rsi_val = calc_rsi(array, deltas, avg_gain, avg_loss, n)
        output = pd.Series(rsi_val, index=temp_idx)
        output.name = col
        all_frames.append(output)
    final_output = pd.concat(all_frames, axis=1, ignore_index=True)
    final_output.columns = col_names
    return final_output

import talib
def GetRSI_talib(data, n = 14 ):
    all_frames = []
    col_names = data.columns
    for col in col_names:
        array = data[col].dropna()
        if len(array)==0:
            output = pd.Series(numpy.nan, index=array.index)
            output.name = col
            all_frames.append(output)
            continue
        #array = array.dropna()#(inplace=True)
        temp_idx = array.index
        #deltas = np.append([0],np.diff(array))
        #avg_gain =  np.sum(deltas[1:n+1].clip(min=0)) / n
        #avg_loss = -np.sum(deltas[1:n+1].clip(max=0)) / n
        #array = np.empty(deltas.shape[0])
        #array.fill(np.nan)
        rsi_val = talib.RSI(array, n)
        output = pd.Series(rsi_val, index=temp_idx)
        output.name = col
        all_frames.append(output)
    final_output = pd.concat(all_frames, axis=1, ignore_index=True)
    final_output.columns = col_names
    return final_output


def SwingIndex(Data, Parameter):
    def SwingIndexTemp(Open,High,Low,Close, Parameter):
        Index=numpy.zeros_like(Open)
        Index[0]=Close[0]
        for i in range(1,len(Close)):
            Numerator = 50*( Close[i] - Close[i-1] + 0.5*( Close[i] - Open[i]) + 0.25*(Close[i-1] - Open[i-1]))*numpy.max([numpy.abs(High[i] - Close[i-1]),numpy.abs(Low[i] - Close[i-1])])
            temp=list(numpy.abs([High[i] - Close[i-1], Low[i]-Close[i-1], High[i]-Low[i]]))
            loc=temp.index(numpy.max(temp))
            if loc==0:
                R=numpy.abs(High[i]-Close[i-1])-0.5*numpy.abs(Low[i]-Close[i-1]) +0.25*numpy.abs(Close[i-1]-Open[i-1])
            elif loc==1:
                R=numpy.abs(Low[i]-Close[i-1])-0.5*numpy.abs(High[i]-Close[i-1]) +0.25*numpy.abs(Close[i-1]-Open[i-1])
            else:
                R=numpy.abs(High[i]-Low[i])+0.25*numpy.abs(Close[i-1]-Open[i-1])
            Index[i]= Numerator / (R*Parameter*Close[i])
            if numpy.isnan(Index[i]):
                Index[i]=0
        return numpy.cumsum(Index)
    SwingFunc = talibonDataFrame(SwingIndexTemp)
    (res,) = SwingFunc(Data, ['Open','High','Low','Close'], Parameter)
    return res

def ADL(Data):
    #pdb.set_trace()
    MFfactor = ((Data.Close -  Data.Low) - (Data.High - Data.Close))/(Data.High - Data.Low)
    MF = MFfactor * Data.Volume
    return MF + MF.shift(1).fillna(0)

def WilliamR(Data, Days):
    def WilliamRTemp(arr):
        return 100*(arr[-1]-numpy.max(arr))/(numpy.max(arr) - numpy.min(arr))
    return Data.rolling(window = Days, min_periods = Days).apply(WilliamRTemp)
#pandas.rolling_apply(Data,Days,WilliamRTemp,min_periods = Days)

def OBV(Data):
    Sign = numpy.sign(Data.Close.diff().fillna(0))
    temp = Sign * Data.Volume
    return temp.cumsum()

def BalanceOfPower(Data, Days):
    return MovingAverage((Data.Close -Data.Open)/(Data.High - Data.Low),Days)

def RMI(Data, MomentumDays = 1, LookBackDays = 14):
    #pdb.set_trace()
    Temp = Data - Data.shift(MomentumDays)
    Temp = Temp.fillna(0)
    def RMITemp(arr):
        gain = numpy.average(arr[arr>0])
        loss = -numpy.average(arr[arr<0])
        RM= gain / loss
        return 100 - (100/(1+RM))
    return Temp.rolling(window = LookBackDays, min_periods = LookBackDays).apply(RMITemp)
#pandas.rolling_apply(Temp, LookBackDays, RMITemp, min_periods = LookBackDays)

def DisparityIndex(Data, DMADays, StochDays):
    dma = MovingAverage(Data, DMADays)
    ratio = Data / dma
    return Stochastics(ratio, StochDays)

def GetSuperTrend(Data,Days,Multiplier):
    c1 = Data.Close.shift(1)
    m1 = numpy.abs(Data.High - Data.Low)
    m2 = numpy.abs(Data.High - c1)
    m3 = numpy.abs(Data.Low - c1)
    TrueRange = numpy.maximum(m1,numpy.maximum(m2,m3))
    ATR = MovingAverage(TrueRange, Days)
    ATRRange = ATR * Multiplier
    MeanPrice = (Data.High +Data.Low)/2
    sign = numpy.sign((Data.Close - Data.Close.shift(1)).fillna(0))
    Trend = MeanPrice + sign*ATRRange
    return Trend

def GetRSI_StdAdjusted(Data, Days):
     def RSI_StdAdjustedTemp(arr):
        gain = numpy.average(arr[arr>0])/numpy.std(arr[arr>0])
        loss = -numpy.average(arr[arr<0])/numpy.std(arr[arr<0])
        RS= gain / loss
        return 100 - (100/(1+RS))
     return (Data.diff()).rolling(window = Days, min_periods = Days).apply(RSI_StdAdjustedTemp)
#pandas.rolling_apply(Data.diff(), Days, RSI_StdAdjustedTemp, min_periods = Days)

def RCI(Data,Days):
    temp = Data.Close.apply(lambda x:x/Data.BenchMark[Data.BenchMark.columns[0]])
    return GetRSI_StdAdjusted(temp, Days)

def GetMovingSlope(Data, Days):
    def temp(data):
        y=data
#        rank = y.argsort().argsort()
#        y = y[(rank<(len(rank)-3))]
#        rank = y.argsort().argsort()
#        y = y[(rank>(3-1))]
        x=range(len(y))
        return (linregress(x,y))[0]/(linregress(x,y))[4]
    return Data.rolling(window = Days, min_periods = Days).apply(temp)
#pandas.rolling_apply(Data, Days, temp,  min_periods = Days)

def GetRS(Data, Days):
    def RSTemp(arr):
        gain = numpy.average(arr[arr>0])
        loss = -numpy.average(arr[arr<0])
        RS= gain / loss
        return RS
    return (Data.diff()).rolling(window = Days, min_periods = Days).apply(RSTemp)
#pandas.rolling_apply(Data.diff(), Days, RSTemp, min_periods = Days)

def RegressionCrossOverSignal(Data, LSDays = 5, LRDays = 50):
    def LS(data):
        y = data
        x = range(len(y))
        return (linregress(x,y))[0]
    def LR(data):
        y = data
        x = numpy.array(range(len(y))).reshape(-1, 1)
        model = LinearRegression()
        model.fit(x, y)
        return model.predict(numpy.array(x[-1]).reshape(-1,1))[0]
    ls = Data.rolling(window = LSDays, min_periods = LSDays).apply(LS)
    return (ls, ls.rolling(window = LRDays, min_periods = LRDays).apply(LR))
    
    
def BollingerBandsNew(Data,Days):
    Middle = MovingAverage(Data,Days)
    var = MovingStd(Data,Days)
    t = GetMovingSlope(Data, 5*Days)
    nsigma = .1 + (1-numpy.abs(t/(10)))*0.9
    nsigma[nsigma<0]=.1
    Up = Middle + nsigma * var
    Down = Middle - nsigma * var
    return (Up,Middle,Down)

def GetTrueRange(Data):
    c1 = Data.Close.shift(1)
    m1 = numpy.abs(Data.High - Data.Low)
    m2 = numpy.abs(Data.High - c1)
    m3 = numpy.abs(Data.Low - c1)
    return numpy.maximum(m1,numpy.maximum(m2,m3))

def UltimateOscillator(Data,Day1=7,Day2=14,Day3=28):
    BP = Data.Close - numpy.minimum(Data.Low,Data.Close.shift(1).fillna(0))
    TR = GetTrueRange(Data)
    Avg1 = MovingAverage(BP,Day1)/MovingAverage(TR, Day1)
    Avg2 = MovingAverage(BP,Day2)/MovingAverage(TR, Day2)
    Avg3 = MovingAverage(BP,Day3)/MovingAverage(TR, Day3)
    return (4*Avg1 + 2*Avg2 + Avg3)/(4 + 2 + 1)

def VortexOscillator(Data, Days):
    TR = MovingAverage(GetTrueRange(Data),Days)
    PVM = MovingAverage(numpy.abs(Data.High - Data.Low.shift(1).fillna(0)),Days)
    NVM = MovingAverage(numpy.abs(Data.Low - Data.High.shift(1).fillna(0)),Days)
    PVI = PVM / TR
    NVI = NVM / TR
    return (NVI,PVI)

def EaseOfMovement(Data,Days):
    temp = (Data.High + Data.Low) / 2
    temp = temp.diff()
    EMV = Data.Volume/((Data.High - Data.Low))
    return MovingAverage(temp/EMV, Days)

def GetATR(Data,Days):
    '''Calclates the Average True Range, assuming Exponential weight as of 2/(1+Days) '''
    c = GetTrueRange(Data)
    #n = 2*Days - 1
    return c.ewm(span = Days).mean()

def BollingerBandsNew2(Data,Days):
    Middle = MovingAverage(Data,Days)
    var = MovingStd(Data,Days)
    Up1 = Middle + .5 * var
    Down1 = Middle - .5 * var
    Up2 = Middle + 2.5 * var
    Down2 = Middle - 2.5 * var
    return (Up2,Up1,Middle,Down1,Down2)

def BollingerBandsNew2withATR(Data,Days):
    Middle = MovingAverage(Data.Close,Days)
    var = GetATR(Data,Days)
    Up1 = Middle + .5 * var
    Down1 = Middle - .5 * var
    Up2 = Middle + 2.5 * var
    Down2 = Middle - 2.5 * var
    return (Up2,Up1,Middle,Down1,Down2)

def MckGinley(Data,N):
    def temp(data,N):
        result=numpy.zeros_like(data,dtype=float)
        error=numpy.zeros_like(data,dtype=float)
        if numpy.isnan(data[0]):
            result[0]=0
            result[1]=0
        else:
            result[0]=data[0]
            result[1]=data[1]
        error[1] = data[1] - result[0]
        for i in range(2,len(data)):
            error[i] = data[i] - result[i-1]
            result[i]=result[i-1] + (error[i])/(N*(data[i]/result[i-1])**4) + (error[i]-error[i-1])/N**2
        return result
    func = talibonDataFrame(temp)
    return func(Data,['Close'],N)[0]

def VHF(Data,n):
    p = Data.diff()
    pabs = (p.abs()).rolling(window = n, min_periods = n).sum()#pandas.rolling_sum(p.abs(),n,min_periods = n)
    hcp = Data.rolling(window = n, min_periods = n).max()#pandas.rolling_max(Data,n,min_periods = n)
    lcp = Data.rolling(window = n, min_periods = n).min()#pandas.rolling_min(Data,n,min_periods = n)
    return (hcp-lcp)/(pabs)

def CHOP(Data,n):
    p = GetATR(Data,1).rolling(window = n, min_periods = n).sum()#pandas.rolling_sum(GetATR(Data,1),n,min_periods = n)
    den = (Data.High).rolling(window = n, min_periods = n).max() - (Data.Low).rolling(window = n, min_periods = n).min()
    #den = pandas.pandas.rolling_max(Data.High,n,min_periods = n) - pandas.pandas.rolling_min(Data.Low,n,min_periods = n)
    return 100*(numpy.log(p/den)/numpy.log(n))

def RAVI(Data,n1,n2):
    return 100*(MovingAverage(Data,n1)-MovingAverage(Data,n2))/MovingAverage(Data,n2)

def choppiness(data,days):
    return data.rolling(window = days, min_periods = days).apply(lambda x:numpy.abs(x[-1] - x[0])/numpy.sum(numpy.abs(numpy.diff(x))))
#pandas.rolling_apply(data,days,lambda x:numpy.abs(x[-1] - x[0])/numpy.sum(numpy.abs(numpy.diff(x))))
    
def choppinessMaxMin(data,days):
    return data.rolling(window = days, min_periods = days).apply(lambda x:numpy.abs(numpy.max(x) - numpy.min(x))/numpy.sum(numpy.abs(numpy.diff(x))))
#pandas.rolling_apply(data,days,lambda x:numpy.abs(numpy.max(x) - numpy.min(x))/numpy.sum(numpy.abs(numpy.diff(x))))

def PointAndFig(data,rev=10,ppb=.0050):
#    data = [100,101,102,101,99,98,97,98,99,100,99,100,101,102,101,100,101,99,100,99]
    start = data[0]
#    target = (start - ppb*rev , start + ppb*rev)
    target = (start - ppb*rev*start , start + ppb*rev*start)
    lastmode = 0
    pnf = []
    for x in data:
        if x>=target[1] or x<=target[0]:
            mode = 1 if x>=target[1] else -1
#            target = (x - ppb*rev, x + ppb) if x>=target[1] else (x - ppb, x + ppb*rev)
            target = (x - ppb*rev*x, x + ppb*x) if x>=target[1] else (x - ppb*x, x + ppb*rev*x)
            if mode != lastmode:
                    pnf.append(mode*rev)
            else:
                pnf[-1] += mode
            lastmode = mode
    pnf[0] = numpy.sign(pnf[0])*(numpy.abs(pnf[0])+1)
    return pnf




def hurst(ts):
    """Returns the Hurst Exponent of the time series vector ts"""
    # Create the range of lag values
    lags = range(2, 100)
    #    ts = ts.pct_change()
    # Calculate the array of the variances of the lagged differences
    tau = [numpy.sqrt(numpy.std(numpy.subtract(ts[lag:], ts[:-lag]))).values for lag in lags]
    # Use a linear fit to estimate the Hurst Exponent
    poly = numpy.polyfit(numpy.log(lags), numpy.log(tau), 1)
    return poly[0]*2.0 

def MovingHurst(data, period = 30):
    Result = pandas.DataFrame(numpy.nan*numpy.ones(data.shape), columns = data.columns, index = data.index)
    indices = [i for i in range(len(data.index)) if i >= period]
    for i in indices:
        Result.ix[i] = hurst(data.ix[i - period:i])
    return Result
        
    
def FactorZscore(indata, period = 12):
    def zscore(numarr):
        return (numarr[-1] - numpy.nanmean(numarr))/numpy.nanstd(numarr)
    return pandas.rolling_apply(indata, period, zscore)
    
def zscore(numarr):
    return (numarr - numpy.nanmean(numarr))/numpy.nanstd(numarr)


def serial_corr(wave, lag=1):
    n = len(wave)
    y1 = wave.ys[lag:]
    y2 = wave.ys[:n-lag]
    corr = corrcoef(y1, y2)
    return corr

def autocorr(wave):
    lags = range(len(wave.ys)//2)
    corrs = [serial_corr(wave, lag) for lag in lags]
    return lags, corrs

def CompareGrowth(d1, d2):
    """d1 is recent and d2 is previous growth"""
    return (d1 - d2)/ numpy.abs(d2)

def Momersion(data, period = 21):
    """Input as Daily Data"""
    ret = data.diff(1)
    ret[ret < 0] = -1
    ret[ret > 0] = 1
    temp = ret.rolling(window = 2, min_periods = 2).apply(numpy.product)#pandas.rolling_apply(ret, 2, numpy.product)
    def Subfactor(d1):
        mc = numpy.abs(d1[d1 == -1].sum(axis = 0))
        mrc = numpy.abs(d1[d1 == 1].sum(axis = 0))
        return 100*mc/(mc + mrc)
    return temp.rolling(window = period, min_periods = period).apply(Subfactor)
#pandas.rolling_apply(temp, period, Subfactor)    

def Max_DrawDown(ser):
    max2here = ser.expanding().max()
    dd2here = ser - max2here
    return dd2here.min()

def MeanDrawDowm(ser):
    max2here = ser.expanding().max()#pandas.expanding_max(ser)
    dd2here = ser - max2here
    return dd2here.mean()
    
def ExtremeEventsReturns(ClosePrice, OpenPrice, mcap, pct_cutoff = 0.10):
    res = {}
    for ticker in ClosePrice.columns:
        closepctchange = ClosePrice[ticker].pct_change()
        closechange = ClosePrice[ticker][numpy.abs(closepctchange) > pct_cutoff]
        openpctchange = (OpenPrice[ticker] - ClosePrice[ticker].shift(1))/ClosePrice[ticker].shift(1)
        openchange = OpenPrice[ticker][numpy.absolute(openpctchange)> pct_cutoff]
        for date in list(set.union(set(closechange.index), set(openchange.index))):
            onedayret = ClosePrice[ticker].shift(-1).pct_change().loc[date]
            onewkret = ClosePrice[ticker].shift(-5).pct_change(5).loc[date]
            onemonthret = ClosePrice[ticker].shift(-21).pct_change(21).loc[date]
            threemonthret = ClosePrice[ticker].shift(-63).pct_change(63).loc[date]
            mktcap = 100 - scipy.stats.percentileofscore(mcap.loc[date], mcap[ticker].loc[date])
            res[ticker, date.date()] = [closepctchange.loc[date], openpctchange.loc[date], mktcap, onedayret, onewkret, onemonthret, threemonthret]
    if len(res) >0:
        resframe = pandas.DataFrame(res).transpose()
        resframe.columns = ['%Change_Close', '%Change_Open', 'MCAP_Percentile', 'OneDay_Return', 'OneWeek_Return', 'OneMonth_Return', 'ThreeMonths_Return']
        return resframe
    else:
        return pandas.DataFrame()

def SimilarityVector(mat1, mat2):
    mat2[mat2>0] = 1
    res = {}
    for ind in mat1.index:
        res[ind] = 1.0*numpy.dot(mat1.loc[ind], mat2.loc[ind])/numpy.sqrt(numpy.dot(mat1.loc[ind], mat1.loc[ind])*numpy.dot(mat2.loc[ind], mat2.loc[ind]))
    return pandas.DataFrame(res,index = ['SimilarityScore'] ).transpose()

def Skewness(ser):
    return scipy.stats.skew(ser)

def RetSTD(mydata):
    rets = mydata.CloseDaily.pct_change()
    result = {}
    for ind in rets.index:
        stocks = mydata.IndexInclusionFactor.ix[ind - datetime.timedelta(40):ind][-1:].transpose().dropna()
        if len(stocks) >0:
            result[ind] = rets.loc[ind].loc[stocks.index].dropna().std()
    return pandas.DataFrame(result, index = ['RetSTD']).transpose()

def HEIKIN(O, H, L, C, oldO, oldC):
    HA_Close = (O + H + L + C)/4
    HA_Open = (oldO + oldC)/2
    elements = numpy.array([H, L, HA_Open, HA_Close])
    HA_High = elements.max(0)
    HA_Low = elements.min(0)
    out = numpy.array([HA_Close, HA_Open, HA_High, HA_Low])  
    return out

def IndexComponents(mydata, freq = 'MS'):
    Close = mydata.CloseDaily.resample(freq, convention = 'end').last()
    Result = pandas.DataFrame(numpy.nan*numpy.ones(Close.shape), columns = Close.columns, index = Close.index)
    for eachtime in Close.index:   
        correspondingdateinindexcomp = [i for i in mydata.indexcomponents.keys() if i < eachtime]
        if (len(correspondingdateinindexcomp) > 0):
            for item in mydata.indexcomponents[max(correspondingdateinindexcomp)]:
                Result.loc[eachtime, item] = 1
    return Result

def IndexComponents2(data, freq = 'MS'):# this is being used for second level object ie mydata.index.Close Level Data Points
    if not hasattr(data, 'CloseDaily'):
        Close = data.Close.resample(freq, convention = 'end').last()
    else:
        Close = data.CloseDaily.resample(freq, convention = 'end').last()
    Result = pandas.DataFrame(numpy.nan*numpy.ones(Close.shape), columns = Close.columns, index = Close.index)
    for eachtime in Close.index:   
        correspondingdateinindexcomp = [i for i in data.indexcomponents.keys() if i < eachtime]
        if (len(correspondingdateinindexcomp) > 0):
            for item in data.indexcomponents[max(correspondingdateinindexcomp)]:
                Result.loc[eachtime, item] = 1
    return Result
