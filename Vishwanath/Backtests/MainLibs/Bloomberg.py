
"""
Created on 9Sep2022
@author: Viren@Incred
Purpose: To fetch the Price & Analyst Data from Bloomberg
"""

import blpapi
import pandas
import sqlite3
import datetime
import numpy
import re
import calendar
from GetData import *
from dateutil.relativedelta import relativedelta
#import pdb

#dbpath = 'G:/Shared drives/BackTests/DB'# Dir path for Data Bases
#TOP500TICKERS = []
priceconn = GetConn('PriceData')#sqlite3.connect(dbpath +'/PriceData.db')
analystconn = GetConn('AnalystData')#sqlite3.connect(dbpath + '/AnalystData.db')

##########################

def MonthLastDate(inDate):
    return datetime.date(inDate.year, inDate.month, calendar.monthrange(inDate.year, inDate.month)[1])
    
    
def InsertIndexComponents(indate, comp, ind, priceconn = priceconn):
    #conn = sqlite3.connect(dbpath +'/PriceData.db')
    #conn = sqlite3.connect(bloompricedataconn)
    #ind = ind.replace(' INDEX', '')
    #TOP500TICKERS.extend(comp)
    comp = str(comp).replace('[', '').replace(']', '').replace('\'', '')    
    curs = priceconn.cursor()
    curs.execute("insert into Components (Date, Components, IndexName) values(?, ?, ?);", (indate, comp, ind))
    priceconn.commit()
    #conn.close()

def indexcomponentsparse(msg):
    MyStr = msg.toString()
    #if MyStr[:100].find('ReferenceDataResponse') == -1:
    #    return 
    basicpattern = re.compile('Index Member = "[^"]+"')
    mylist = re.findall(basicpattern, MyStr)
    mylist = [i.replace('Index Member = ','').replace('"','') for i in mylist]
    return mylist


def RequestIndexData(startdate, ind):
    todaydate = datetime.datetime.today().date()    
    #enddate = todaydate.strftime('%Y%m%d')            
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost('localhost')
    sessionOptions.setServerPort(8194)
    
    print("Connecting...")
    session = blpapi.Session(sessionOptions)    
    if not session.start():
        print("Failed to start session.")
    
    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata"    )
        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")   
        request = refDataService.createRequest("ReferenceDataRequest")
        request.append("securities", ind)
        request.append("fields", "Indx_mweight_hist")    
        # add overrides
        overrides = request.getElement("overrides")
        override1 = overrides.appendElement()
        override1.setElement("fieldId", "END_DATE_OVERRIDE")  
        curdate = startdate#MonthLastDate(startdate)
#        FinalData = []
        while(True):
            curdatestr = curdate.strftime('%Y%m%d')
            print(curdatestr, ind)
            override1.setElement("value", curdatestr)
            session.sendRequest(request)        
            while(True):
                ev = session.nextEvent()
                if ev.eventType() == blpapi.Event.RESPONSE:
                    allmessages = [i for i in ev]
                    for msg in allmessages:
                        InsertIndexComponents(curdate, indexcomponentsparse(msg), ind)
#                        FinalData.append((curdate,indexcomponentsparse(msg)))
                    # Response completly received, so we could exit
                    break
            curdate = curdate + relativedelta(months = 1)
            if curdate >= todaydate:
                break
    finally:
        # Stop the session
        session.stop()
        
###################

def InsertBloombergPriceData(datalist = [], priceconn = priceconn):
    curs = priceconn.cursor()
    curs.execute('create table if not exists StockPriceData(Date DATE, Ticker TEXT, PX_LAST REAL, PX_OPEN REAL, PX_LOW REAL, PX_HIGH REAL, MCAP REAL, TURNOVER REAL, INDIA_TOTAL_DAILY_VOLUME REAL, INDIA_TOTAL_DELIVERY_PCT REAL);')
    for i in range(int(len(datalist)/2)):
        security = datalist[2*i]
        date = datalist[2*i +1]['date']
        try:
            mcap = datalist[2*i +1]['CUR_MKT_CAP']
        except:
            mcap = numpy.nan
        try:
            turnover = datalist[2*i +1]['TURNOVER']
        except:
            turnover = numpy.nan    
        try:
            pxlast = datalist[2*i +1]['PX_LAST']
        except:
            pxlast = numpy.nan
        try:
            pxopen = datalist[2*i +1]['PX_OPEN']
        except:
            pxopen = numpy.nan
        try:
            pxlow = datalist[2*i +1]['PX_LOW']
        except:
            pxlow =  numpy.nan
        try:
            pxhigh = datalist[2*i +1]['PX_HIGH']
        except:
            pxhigh = numpy.nan
        try:
            dailyvolume = datalist[2*i +1]['INDIA_TOTAL_DAILY_VOLUME']
        except:
            dailyvolume =  numpy.nan
        try:
            delpct = datalist[2*i +1]['INDIA_TOTAL_DELIVERY_PCT']
        except:
            delpct =  numpy.nan
        try:
            curs.execute('insert into StockPriceData values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (date, security, pxlast, pxopen, pxlow, pxhigh, mcap, turnover, dailyvolume, delpct))
        except:
            pass               
    priceconn.commit()
#    print len(datalist)/2, ' Inserted'
    return


def InsertBloombergPriceDataFut(datalist = [], priceconn = priceconn):
    curs = priceconn.cursor()
    curs.execute('create table if not exists FutPriceData(Date DATE, Ticker TEXT, PX_LAST REAL, PX_OPEN REAL, PX_LOW REAL, PX_HIGH REAL, VWAP_TURNOVER REAL, PX_VOLUME REAL, OPEN_INT REAL);')
    for i in range(int(len(datalist)/2)):
        security = datalist[2*i]
        date = datalist[2*i +1]['date']
        try:
            turnover = datalist[2*i +1]['VWAP_TURNOVER']
        except:
            turnover = numpy.nan    
        try:
            pxlast = datalist[2*i +1]['PX_LAST']
        except:
            pxlast = numpy.nan
        try:
            pxopen = datalist[2*i +1]['PX_OPEN']
        except:
            pxopen = numpy.nan
        try:
            pxlow = datalist[2*i +1]['PX_LOW']
        except:
            pxlow =  numpy.nan
        try:
            pxhigh = datalist[2*i +1]['PX_HIGH']
        except:
            pxhigh = numpy.nan
        try:
            dailyvolume = datalist[2*i +1]['PX_VOLUME']
        except:
            dailyvolume =  numpy.nan
        try:
            openInt = datalist[2*i +1]['OPEN_INT']
        except:
            openInt =  numpy.nan
        try:
            curs.execute('insert into FutPriceData values(?, ?, ?, ?, ?, ?, ?, ?, ?)', (date, security, pxlast, pxopen, pxlow, pxhigh, turnover, dailyvolume, openInt))
        except:
            pass               
    priceconn.commit()
#    print len(datalist)/2, ' Inserted'
    return

def InsertBloombergIndexData(datalist = [], priceconn = priceconn):
    curs = priceconn.cursor()
    dup = 0
    curs.execute('create table if not exists IndexPriceData(Date DATE, Ticker TEXT, PX_LAST REAL, PX_OPEN REAL, PX_LOW REAL, PX_HIGH REAL, MCAP REAL, TURNOVER REAL);')
    for i in range(int(len(datalist)/2)):
        security = datalist[2*i]
        date = datalist[2*i +1]['date']
        try:
            mcap = datalist[2*i +1]['CUR_MKT_CAP']
        except:
            mcap = numpy.nan
        try:
            turnover = datalist[2*i +1]['TURNOVER']
        except:
            turnover = numpy.nan    
        try:
            pxlast = datalist[2*i +1]['PX_LAST']
        except:
            pxlast = numpy.nan
        try:
            pxopen = datalist[2*i +1]['PX_OPEN']
        except:
            pxopen = numpy.nan
        try:
            pxlow = datalist[2*i +1]['PX_LOW']
        except:
            pxlow =  numpy.nan
        try:
            pxhigh = datalist[2*i +1]['PX_HIGH']
        except:
            pxhigh = numpy.nan
        try:
            curs.execute('insert into IndexPriceData values(?, ?, ?, ?, ?, ?, ?, ?)', (date, security, pxlast, pxopen, pxlow, pxhigh, mcap, turnover))
        except:
            dup +=1
#    print  int(len(datalist)/2), ' Inserted'
    priceconn.commit()
    return


def messageparser(msg):
    #MyStr = msg.toString()
    #if MyStr[:100].find('HistoricalDataResponse') == -1:
    #    return ''
    security_data = msg.getElement('securityData')
    name = [i for i in security_data.getElement('security').values()][0]
    field_data = security_data.getElement('fieldData')
    FinalData = []
    for i in range(field_data.numValues()):
        fields = field_data.getValue(i)
        mystr = fields.toString()
        mylist = mystr.replace('\n','')[14:-1].split('    ')
        temp = {}
        for item in mylist:
            tt = item.replace(' ','').split('=')
            temp[tt[0]] = tt[1]
        FinalData.append(temp)        
    return (name, FinalData)

def AnalystData(datalist = [], analystconn = analystconn):
    curs = analystconn.cursor()
    curs.execute('create table if not exists AnalystRatingData(Date DATE, Ticker TEXT, TargetPrice REAL, AnalystRating REAL, AnalystRecommendation REAL);')
    for i in range(int(len(datalist)/2)):
        security = datalist[2*i]
        date = datalist[2*i +1]['date']
        try:
            rating = datalist[2*i +1]['BEST_ANALYST_RATING']
        except:
            rating = numpy.nan
        try:
            price = datalist[2*i +1]['BEST_TARGET_PRICE']
        except:
            price = numpy.nan
        try:
            reco = datalist[2*i +1]['TOT_ANALYST_REC']
        except:
            reco = numpy.nan
        try:
            curs.execute('insert into AnalystRatingData values(?, ?, ?, ?, ?)', (date, security, price, rating, reco))
        except:
            pass
    analystconn.commit()
    return

def Insert_BEST_EPSData(datalist = [], analystconn = analystconn):
    curs = analystconn.cursor()
    if len(datalist) > 0:
        k1 = curs.execute('create table if not exists EPSData(Date DATE, Ticker TEXT, BEST_EPS REAL, BEST_EPS_LO REAL, BEST_EPS_HI REAL, BEST_EPS_STDDEV REAL);')

        for i in range(int(len(datalist)/2)):
            security = datalist[2*i]
            date = datalist[2*i +1]['date']
            try:
                eps = datalist[2*i +1]['BEST_EPS']
            except:
                eps = numpy.nan
            try:
                low = datalist[2*i +1]['BEST_EPS_LO']
            except:
                low = numpy.nan
            try:
                high = datalist[2*i +1]['BEST_EPS_HI']
            except:
                high = numpy.nan
            try:
                std = datalist[2*i +1]['BEST_EPS_STDDEV']
            except:
                std = numpy.nan
            try:
                k2 = curs.execute('insert into EPSData values(?, ?, ?, ?, ?, ?)', (date, security, eps, low, high, std))
            except:
                pass
        analystconn.commit()
    return 


#input as data list and output a data frame, used for getting one day price day to get the corporate actions, if any
def GetBloombergPriceDataDF(datalist = []):
    newdatalist = []
    for i in range(int(len(datalist)/2)):
        security = datalist[2*i]
        try:
            pxlast = datalist[2*i +1]['PX_LAST']
        except:
            pxlast = numpy.nan
        newdatalist.append({'Ticker':security,'PX_LAST' : pxlast})
    dataF = pandas.DataFrame(newdatalist)
    dataF.index = dataF.Ticker
    del dataF['Ticker']
    return dataF


#input as data list and output a data frame, used for getting one day price day to get the corporate actions, if any
def BloombergPriceDataMultiDF(datalist = [], MyFields = []):
    newdatalist = []
    MyFields.append('date')
    for i in range(int(len(datalist)/2)):        
        security = datalist[2*i]
        tempList = {}
        tempList['Ticker'] = security
        for field in MyFields:
            try:
                fieldData = datalist[2*i +1][field]
            except:
                fieldData = numpy.nan
            tempList[field] = fieldData
        newdatalist.append(tempList)
    dataF = pandas.DataFrame(newdatalist)
    #dataF.index = dataF.Ticker
    #del dataF['Ticker']
    return dataF


def BloomRequestData(tickerslist,datatype = 'stockdata',sectype = 'not index',action = 'insert', startdate = "20050101", enddate = datetime.date.today().strftime('%Y%m%d')):
    """ action = 'insert' or 'return data frame' """
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost('localhost')
    sessionOptions.setServerPort(8194)
    print("Connecting..." )
    session = blpapi.Session(sessionOptions)    
    if not session.start():
        print("Failed to start session.")
    if datatype == 'stockdata':
        MyFields = ['PX_LAST', 'PX_HIGH', 'PX_LOW', 'PX_OPEN', 'TURNOVER', 'CUR_MKT_CAP', 'INDIA_TOTAL_DAILY_VOLUME', 'INDIA_TOTAL_DELIVERY_PCT']
    elif datatype == 'analystdata':
        MyFields = ['BEST_ANALYST_RATING', 'BEST_TARGET_PRICE', 'TOT_ANALYST_REC']
    elif datatype == 'EPSData':
#        MyFields = ['BEST_EPS', 'BEST_EPS_LO', 'BEST_EPS_HI', 'BEST_EPS_STDDEV', 'BEST_LTG_EPS']
        MyFields = ['BEST_EPS', 'BEST_EPS_LO', 'BEST_EPS_HI', 'BEST_EPS_STDDEV']
    elif datatype == 'FutData':
        MyFields = ['PX_LAST', 'PX_HIGH', 'PX_LOW', 'PX_OPEN', 'VWAP_TURNOVER', 'PX_VOLUME', 'OPEN_INT']
    elif datatype == 'FutLiveData':
        MyFields = ['LAST_PRICE', 'PX_HIGH', 'PX_LOW', 'PX_OPEN', 'FUT_CONT_SIZE']


    finaldata = []
    returndata = pandas.DataFrame()
    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
        refDataService = session.getService("//blp/refdata")
        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        for item in tickerslist:
            request.getElement("securities").appendValue(item)
        #request.getElement("securities").appendValue("CNX100 Index")
        for item in MyFields:
            request.getElement("fields").appendValue(item)
        request.set("periodicityAdjustment", "ACTUAL")
        #request.set("periodicitySelection", "MONTHLY")
        request.set("startDate", startdate)
        request.set("endDate", enddate)
        request.set("adjustmentFollowDPDF", True)
        
        if datatype == 'EPSData':
            overrides = request.getElement("overrides")
            override1 = overrides.appendElement()
            override1.setElement("fieldId", "BEST_FPERIOD_OVERRIDE")
            override1.setElement("value", "BF") # 12 months Blended Forward
        print("Sending Request:")
        session.sendRequest(request)
        counter = 0
        while(True):
            counter = counter + 1
            ev = session.nextEvent()
            for msg in ev:
                if counter>=4:
                    values = messageparser(msg)
                    security_data = msg.getElement('securityData')
                    name = [i for i in security_data.getElement('security').values()][0]
                    tempdata = [i for i in values[1]]                
                    for i in range(len(tempdata)):
                        finaldata.extend((name, tempdata[i]))
                    if len(finaldata) > 0:
                        if (action == 'insert'):
                            if datatype == 'stockdata':
                                if(sectype == 'index'):
                                    InsertBloombergIndexData(finaldata)
                                else:
                                    InsertBloombergPriceData(finaldata)
                            elif datatype == 'analystdata':
                                AnalystData(finaldata)
                            elif datatype == 'EPSData':
                                Insert_BEST_EPSData(finaldata)
                            elif datatype == 'FutData':
                                InsertBloombergPriceDataFut(finaldata)
                        else:
                            if datatype == 'FutLiveData':
                                returndata = pandas.concat([returndata, BloombergPriceDataMultiDF(finaldata, MyFields)])
                            else:
                                returndata = pandas.concat([returndata,GetBloombergPriceDataDF(finaldata)])
                    finaldata = []                    
            if ev.eventType() == blpapi.Event.RESPONSE:
                print('completed bye')
                break     
    finally:
        session.stop()
    return returndata

######################################## Queries ########################################################
# Queries to get the list of stocks to update from the db and Last updaet date
def StocksTickerstoUpdate(priceconn = priceconn):
    curs = priceconn.cursor()    
    histDataTickers = pandas.DataFrame(curs.execute('select Date, Ticker, PX_LAST from StockPriceData where Date = (select MAX(Date) from StockPriceData where Date not in (select MAX(Date) from StockPriceData));').fetchall(), columns = [rec[0] for rec in curs.description])
    histDataTickers.index = histDataTickers.Ticker
    del histDataTickers['Ticker']
    
    allStocksinDB = curs.execute('select distinct(Ticker) from StockPriceData;').fetchall()
    allStocksinDB = [i[0] for i in allStocksinDB]
    
    allStocks = set([ i+ ' Equity' for i in GetComponentsForAll(priceconn)]) 
    
    
    
    allFutsinDB = curs.execute('select distinct(Ticker) from FutPriceData;').fetchall()
    allFutsinDB = [i[0] for i in allFutsinDB]
    
    histDataFuts = pandas.DataFrame(curs.execute('select Date, Ticker, PX_LAST from FutPriceData where Date = (select MAX(Date) from FutPriceData where Date not in (select MAX(Date) from FutPriceData));').fetchall(), columns = [rec[0] for rec in curs.description])
    histDataFuts.index = histDataFuts.Ticker
    del histDataFuts['Ticker']
    
    allFuts = curs.execute('select GenericTicker from FutLookUpTable;').fetchall()
    allFuts = [i[0]+ ' Equity' for i in allFuts]
    
    
    
    checkDate = histDataTickers['Date'][0]
    checkDate = datetime.datetime.strptime(checkDate, '%Y-%m-%d').date()
    bloomhistprice = BloomRequestData(list(histDataTickers.index), action = 'not insert', startdate = checkDate.strftime('%Y%m%d'), enddate = checkDate.strftime('%Y%m%d'))
    bloomhistprice.PX_LAST = [float(i) for i in bloomhistprice.PX_LAST]
    
    stocksPrNotMatch = (histDataTickers.PX_LAST/bloomhistprice.PX_LAST) -1
    stocksPrNotMatch = stocksPrNotMatch[numpy.abs(stocksPrNotMatch) > 0.005].dropna().index # if differs by more than 2 bps then there is some corporate action
    
    checkDateFut = histDataFuts['Date'][0]
    checkDateFut = datetime.datetime.strptime(checkDateFut, '%Y-%m-%d').date()
    bloomhistpriceFut = BloomRequestData(list(histDataFuts.index), datatype = 'FutData', action = 'not insert', startdate = checkDateFut.strftime('%Y%m%d'), enddate = checkDateFut.strftime('%Y%m%d'))
    bloomhistpriceFut.PX_LAST = [float(i) for i in bloomhistpriceFut.PX_LAST]
    
    futsPrNotMatch = (histDataFuts.PX_LAST/bloomhistpriceFut.PX_LAST) -1
    futsPrNotMatch = futsPrNotMatch[numpy.abs(futsPrNotMatch) > 0.005].dropna().index # if differs by more than 2 bps then there is some corporate action

    newStocks = set.difference(set(allStocks), set(allStocksinDB)) # remove those Stocks which dave data in DB-> trying to remove those stocks which are not active now but have hist data
    #newStocks = set.difference(newStocks, set(histDataTickers.index)) # remove those which have latest available data
    newStocks = set.union(newStocks, set(futsPrNotMatch))# add those for which some corporate actions
        #set.union(set.difference(set(allStocks), set.union(set(histDataTickers.index), set(allStocksinDB)) ),set(stocksPrNotMatch))
    oldStocks = set.difference(set(histDataTickers.index),set(stocksPrNotMatch))
    
    
    newFuts =  set.difference(set(allFuts), set(allFutsinDB))# remove those futures who are not active now
    #newFuts = set.difference(newFuts, set(histDataFuts.index))# remove those who have latest available data right
    newFuts = set.union(newFuts, set(futsPrNotMatch))# add those for which some corporate actions
    #set.union(set.difference(set(allFuts), set(histDataFuts.index)),set(futsPrNotMatch))
    oldFuts = set.difference(set(histDataFuts.index),set(futsPrNotMatch))
    
    newFuts = [it.replace(' INDEX Equity', ' Equity') for it in newFuts]
    oldFuts = [it.replace(' INDEX Equity', ' Equity') for it in oldFuts]
    
    return (list(newStocks), list(oldStocks), checkDate, list(newFuts), list(oldFuts), checkDateFut)


'''
###########################################################################################################
startday = datetime.date(2000,1, 1)
today = datetime.date.today()
monthstartdate = datetime.date(today.year, today.month, 1)#datetime.datetime(2001, 1, 1).date()

indicesList = ['SENSEX INDEX', 'NIFTY INDEX', 'NIFTYJR INDEX', 'BSE100 INDEX', 'NSE100 INDEX' , 'NSEMCAP INDEX', 'NSESMCP INDEX','BSE200 INDEX', 'BSE500 INDEX', 'NSE500 INDEX', 'NSE200 INDEX',
               'NSEAUTO INDEX', 'NSEBANK INDEX', 'NSEFMCG INDEX', 'NSEIT INDEX', 'NSEMET INDEX', 'NSEPHRM INDEX', 'NSEPSBK INDEX', 'NSEINFR INDEX', 'NSECON INDEX', 'NSEFIN INDEX', 
               'NSECMD INDEX', 'NSENRG INDEX', 'NSEPSE INDEX']
# for updating the Indices Constituents, in case of past dates run manually




(newStocks, oldStocks, checkDate, newFuts, oldFuts, checkDateFut) = StocksTickerstoUpdate()

#indices = ['NSE200 INDEX']#, 'NSECMD INDEX', 'NSENRG INDEX', 'NSEPSE INDEX']
monthstartdate = datetime.date(checkDate.year, checkDate.month, 1)
# for ind in indicesList:
#      RequestIndexData(monthstartdate, ind)#monthstartdate

futIndices = ['NZ1 Index', 'AF1 Index']
indicesList.extend(futIndices)

#BloomRequestData(tickerslist,datatype = 'stockdata',sectype = 'not index',action = 'insert', startdate = "20050101", enddate = datetime.date.today().strftime('%Y%m%d'))
print('Updating Price Data..')
d1 = BloomRequestData(oldStocks, startdate = checkDate.strftime('%Y%m%d')) # updating price data from last updated date
d2 = BloomRequestData(newStocks, startdate = startday.strftime('%Y%m%d')) # updating data for those where some corp action or new introduced

print('Updating Index Price Data..')
d3 = BloomRequestData(indicesList, sectype = 'index', startdate = checkDate.strftime('%Y%m%d')) # updating all the indices data, from last uddated date
#d4 = BloomRequestData(indices, sectype = 'index', startdate = startday.strftime('%Y%m%d'))

print('Updating Analyst Data..')
d5 = BloomRequestData(oldStocks, datatype = 'analystdata', startdate = checkDate.strftime('%Y%m%d'))
d6 = BloomRequestData(newStocks, datatype = 'analystdata', startdate = startday.strftime('%Y%m%d'))

print('Updating EPS Data..')
d7 = BloomRequestData(oldStocks, datatype = 'EPSData', startdate = checkDate.strftime('%Y%m%d'))
d8 = BloomRequestData(newStocks, datatype = 'EPSData', startdate = startday.strftime('%Y%m%d'))

print('Updating Fut Data..')
d9 = BloomRequestData(oldFuts, datatype = 'FutData', startdate = checkDateFut.strftime('%Y%m%d'))
d10 = BloomRequestData(newFuts, datatype = 'FutData', startdate = startday.strftime('%Y%m%d'))

###################################################################  END ################################
'''
'''
def CommaSeparatedList(MyList, appendapost = 1):
    if all([matplotlib.cbook.is_numlike(i) for i in MyList]):
        appendapost = 0
        
    if appendapost == 1:
        temp = ",".join(["'" + i + "'" for i in MyList])
    else:
        temp = ','.join([str(i) for i in MyList])
    
    temp = "(" + temp + ")"
    return temp



def BESTEPSGetTickers(conn = bloompricedataconn):
    curs = conn.cursor()
    tickersname = pandas.DataFrame(curs.execute('select * from tickernames;').fetchall(), columns = [rec[0] for rec in curs.description])['Ticker']
    datatickers = pandas.DataFrame(curs.execute('select Ticker, max(Date) as MaxDate from BEST_EPS_DATA group by Ticker;').fetchall(), columns = [rec[0] for rec in curs.description])
    datatickers.index = datatickers.Ticker
    del datatickers['Ticker']
    newintroduced = set.difference(set(tickersname),set(datatickers.index))
    oldtickers = set.intersection(set(tickersname),set(datatickers.index))
    datetoupdate = set(datatickers.sort('MaxDate')['MaxDate'].values)
    datetoupdate = pandas.DataFrame(list(datetoupdate)).sort(0)[-4:][:1].values[0][0].encode()
    datetoupdate = datetime.datetime.strptime(datetoupdate, "%Y-%m-%d").date()
    return (list(oldtickers), list(newintroduced), datetoupdate)

    


def GetAnalystDataLastUpdateDate(analystconn = analystconn):
    curs = analystconn.cursor()
    date = curs.execute('select max(Date) as Date from AnalystRatingData;').fetchall()[0][0]
    date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    return date    
    




def PartialStocksTickerstoUpdate():
    for i in range(5):
        twentyDaysoldActiveDate = datetime.datetime.today() - datetime.timedelta(30+i)
        if twentyDaysoldActiveDate.weekday() in range(5):
            break
    return twentyDaysoldActiveDate
    #twentyDaysoldActiveDate = datetime.datetime(2019, 4, 4)
def ReturnPastStocksPrice(twentyDaysoldActiveDate, TOP500TICKERS, bloompricedataconn = bloompricedataconn):
    bloompricedatacurs = bloompricedataconn.cursor()
    for i in range(5):
        twentyDaysoldActiveDate = twentyDaysoldActiveDate - datetime.timedelta(i)
        query = "select Date, Ticker, PX_LAST from StockPriceDataNew where Date = '%s' and Ticker in %s;"%(twentyDaysoldActiveDate, CommaSeparatedList(TOP500TICKERS))
        bloompricedatacurs.execute(query)
        df = pandas.DataFrame(bloompricedatacurs.fetchall(), columns = [rec[0] for rec in bloompricedatacurs.description])
        if len(df)>0:
            break
    return df

def DatetoUpdate(bloompricedataconn = bloompricedataconn):
    bloompricedatacurs = bloompricedataconn.cursor()    
    datetoupdate = pandas.DataFrame(bloompricedatacurs.execute('select max(Date) as MaxDate from StockPriceDataNew;').fetchall(), columns = [rec[0] for rec in bloompricedatacurs.description])
    datetoupdate = datetime.datetime.strptime(datetoupdate.ix[0].ix[0], "%Y-%m-%d").date()
    return  datetoupdate

def IndexTickerstoUpdate(bloombergindexdataconn = bloombergindexdataconn):
    bloombergindexdatacurs = bloombergindexdataconn.cursor()    
    indexnames = pandas.DataFrame(bloombergindexdatacurs.execute('select * from indexnames;').fetchall(), columns = [rec[0] for rec in bloombergindexdatacurs.description])['Ticker']   
    indextickers = pandas.DataFrame(bloombergindexdatacurs.execute('select Ticker, max(Date) as MaxDate from StockPriceFullData group by Ticker;').fetchall(), columns = [rec[0] for rec in bloombergindexdatacurs.description])
    indextickers.index = indextickers.Ticker
    del indextickers['Ticker']
    newintroduced = set.difference(set(indexnames),set(indextickers.index))
    oldindices = set.intersection(set(indexnames),set(indextickers.index))
    datetoupdate = set(indextickers.sort('MaxDate')['MaxDate'].values)
    datetoupdate = pandas.DataFrame(list(datetoupdate)).sort(0)[-2:][:1].values[0][0].encode()
    datetoupdate = datetime.datetime.strptime(datetoupdate, "%Y-%m-%d").date()
    return (list(oldindices),  list(newintroduced), datetoupdate)
    
def GetStocksPriceByDate(indate, tickerslist, bloompricedataconn = bloompricedataconn):
    tickerslist = [str(i) for i in tickerslist]
    tickerslist = [i.lower() for i in tickerslist]
    bloompricedatacurs = bloompricedataconn.cursor()
    df = pandas.DataFrame(bloompricedatacurs.execute("select Ticker, PX_LAST  from StockPriceDataNew where Date = '%s' and lower(Ticker) in %s;" %(indate, CommaSeparatedList(tickerslist))).fetchall(), columns = [rec[0] for rec in bloompricedatacurs.description])
    df.index = df.Ticker
    del df['Ticker']
    return df




    




tickerslist = ['INFO IN Equity',  'TRCL IN Equity']#,  'TRP IN Equity',  'CPBI IN Equity',  'TECHM IN Equity']


def BEST_ANALYST_BY_ID_RECS_BULK_messageparser(msg, analystconn = analystconn):
    curs = analystconn.cursor()
    #curs.execute('create table if not exists InformationData (ActionCode TEXT, AnalystID REAL, Recommendation TEXT, PX_LOW REAL, PX_HIGH REAL, MCAP REAL, TURNOVER REAL);')
    security_data = msg.getElement('securityData')
    temp1 = [i for i in security_data.values()][0]   
    name = [i1 for i1 in temp1.getElement('security').values()][0]
    field_data = temp1.getElement('fieldData')
    for j in range(field_data.numValues()):
        idrecs = [k for k in field_data.getElement('BEST_ANALYST_BY_ID_RECS_BULK').values()]
        for l in range(len(idrecs)):
            FirmName = [i for i in idrecs[l].getElement('Firm Name').values()][0]
            AnalystID = [i for i in idrecs[l].getElement('Analyst ID').values()][0]
            Recommendation = [i for i in idrecs[l].getElement('Recommendation').values()][0]
            Rating = [i for i in idrecs[l].getElement('Rating').values()][0]
            ActionCode = [i for i in idrecs[l].getElement('Action Code').values()][0]
            TargetPrice = [i for i in idrecs[l].getElement('Target Price').values()][0]
            Period = [i for i in idrecs[l].getElement('Period').values()][0]
            Date = [i for i in idrecs[l].getElement('Date').values()][0]
            BARR = [i for i in idrecs[l].getElement('BARR').values()][0]
            OneYrReturn = [i for i in idrecs[l].getElement('1 Year Return').values()][0]
            try:
                curs.execute('insert into InformationData values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (ActionCode, AnalystID, BARR, Date, FirmName, OneYrReturn, Period, Rating, Recommendation, TargetPrice, name))
                print('Working')
            except:
                pass
    analystconn.commit()
    return

def GetANRBulkData(tickerslist):
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost('localhost')
    sessionOptions.setServerPort(8194)
    print("Connecting..." )
    # Create a Session
    session = blpapi.Session(sessionOptions)
    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return
    try:
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            return    
        refDataService = session.getService("//blp/refdata")  
        request = refDataService.createRequest("ReferenceDataRequest")
        for item in tickerslist:
            request.getElement("securities").appendValue(item)
        request.append("fields", "BEST_ANALYST_BY_ID_RECS_BULK")
        session.sendRequest(request)
        counter = 0
        while(True):
            counter = counter + 1
            ev = session.nextEvent()
            for msg in ev:
                if counter>=4:
                    BEST_ANALYST_BY_ID_RECS_BULK_messageparser(msg, analystconn)
            if ev.eventType() == blpapi.Event.RESPONSE:
                print('completed bye')
                break     
    finally:
        session.stop()
    

def DividendData_BULK_messageparser(msg, analystconn = analystconn):
    curs = analystconn.cursor()
    #curs.execute('create table if not exists DividendBulkData (Ticker TEXT, DeclaredDate TEXT, ExDate TEXT, RecordDate TEXT, PayableDate TEXT, DividendAmount REAL, DividendFrequency REAL, DividendType TEXT);')
    security_data = msg.getElement('securityData')
    temp1 = [i for i in security_data.values()][0]   
    name = [i1 for i1 in temp1.getElement('security').values()][0]
    field_data = temp1.getElement('fieldData')
    for j in range(field_data.numValues()):
        idrecs = [k for k in field_data.getElement('DVD_HIST_ALL').values()]
        for l in range(len(idrecs)):
            try:
                DeclaredDate = [i for i in idrecs[l].getElement('Declared Date').values()][0]
            except:
                DeclaredDate = "NA"
            try:
                ExDate = [i for i in idrecs[l].getElement('Ex-Date').values()][0]
            except:
                ExDate = "NA"
            try:
                RecordDate = [i for i in idrecs[l].getElement('Record Date').values()][0]
            except:
                RecordDate = "NA"
            try:
                PayableDate = [i for i in idrecs[l].getElement('Payable Date').values()][0]
            except:
                PayableDate = numpy.NaN
            try:
                DividendAmount = [i for i in idrecs[l].getElement('Dividend Amount').values()][0]
            except:
                DividendAmount = numpy.NaN
            try:
                DividendFrequency = [i for i in idrecs[l].getElement('Dividend Frequency').values()][0]
            except:
                DividendFrequency = numpy.NaN
            try:
                DividendType = [i for i in idrecs[l].getElement('Dividend Type').values()][0]
            except:
                DividendType = "NA"
            try:
                curs.execute('insert into DividendBulkData values(?, ?, ?, ?, ?, ?, ?, ?)', (name, DeclaredDate, ExDate, RecordDate, PayableDate, DividendAmount, DividendFrequency, DividendType))
                #print 'Working'
            except:
                print('Some Issue', name)
                pass
    analystconn.commit()
    return

def GetDividendBulkData(tickerslist):
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost('localhost')
    sessionOptions.setServerPort(8194)
    print("Connecting..." )
    # Create a Session
    session = blpapi.Session(sessionOptions)
    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return
    try:
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            return    
        refDataService = session.getService("//blp/refdata")  
        request = refDataService.createRequest("ReferenceDataRequest")
        for item in tickerslist:
            request.getElement("securities").appendValue(item)
        request.append("fields", "DVD_HIST_ALL")
        session.sendRequest(request)
        counter = 0
        while(True):
            counter = counter + 1
            ev = session.nextEvent()
            for msg in ev:
                if counter>=4:
                    DividendData_BULK_messageparser(msg, analystconn)
            if ev.eventType() == blpapi.Event.RESPONSE:
                print('completed bye')
                break     
    finally:
        session.stop()

def MonthLastWorkDay():
    curDate = datetime.datetime.today().date()
    if curDate.month ==12:
        curMonthLastWorkDay = datetime.date(curDate.year+1, 1, 1)-datetime.timedelta(1)
    else:
        curMonthLastWorkDay = datetime.date(curDate.year, curDate.month+1, 1)-datetime.timedelta(1)
        
    while True:
        if curMonthLastWorkDay.weekday() in [0, 1, 2, 3, 4]:
            break
        else:
            curMonthLastWorkDay = curMonthLastWorkDay -datetime.timedelta(1)
    if curMonthLastWorkDay ==curDate:
        return True
    else:
        return False

print('Working')




#############################
startdate = datetime.datetime.today().date()#datetime.datetime(2000, 1, 1).date()#
indices = ['BSE100 INDEX', 'NSEMCAP INDEX', 'NSESMCP INDEX','BSE200 INDEX', 'BSE500 INDEX', 'NIFTY INDEX', 'SENSEX INDEX', 'NSE500 INDEX']
indices = ['BSE500 INDEX']
#indices = []
for ind in indices:
    RequestIndexData(startdate, ind)
    
print('Finished Indices Securties Names')
###############################

#d10 = GetDividendBulkData(TOP500TICKERS)

#############################
ForceDownload = False
if MonthLastWorkDay() and ForceDownload:
    oldtickers, newintroduced, datetoupdate = StocksTickerstoUpdate()
    datetoupdate = datetime.datetime(2018, 9, 28).date()#datetime.datetime.now().date()-datetime.timedelta(149)
    oldtickers = list(set.difference(set(oldtickers), set(TOP500TICKERS)))
    bloomhistprice = BloomRequestData(oldtickers, action = 'not insert', startdate = datetoupdate.strftime('%Y%m%d'), enddate = datetoupdate.strftime('%Y%m%d'))
    bloomhistprice.PX_LAST = [float(i) for i in bloomhistprice.PX_LAST]
    dbhistprice = GetStocksPriceByDate(datetoupdate, oldtickers)
    
    print('Tickers Comp Data Downloaded')
    for tickr in list(set.intersection(set(bloomhistprice.index), set(dbhistprice.index))):
        if numpy.abs(bloomhistprice.ix[tickr].values[0] - dbhistprice.ix[tickr].values[0]) > 0.001:
            newintroduced.append(tickr)
    
    oldtickers = list(set.difference(set(oldtickers), set(newintroduced)))
    print('Got the List of Tickers')
    while datetime.datetime.now().time() <= datetime.time(15, 31, 0):
        time.sleep(2*60)
    if len(newintroduced)>0:
        d1 = BloomRequestData(newintroduced, startdate = "2000101")
    d2 = BloomRequestData(oldtickers, startdate = datetoupdate.strftime('%Y%m%d'))

####################
TOP500TICKERS = [re.sub(' I[B|S|N]', ' IN Equity', i) for i in TOP500TICKERS]
TOP500TICKERS = list(set(TOP500TICKERS))


checkDate = datetime.datetime(2020, 2, 12).date()

#db500Data = PartialStocksTickerstoUpdate(TOP500TICKERS)

db500Data = ReturnPastStocksPrice(checkDate, TOP500TICKERS, bloompricedataconn = bloompricedataconn) # get data of checkDate to check for corporate actions

datetoupdate = PartialStocksTickerstoUpdate().date()
#datetoupdate = datetime.datetime.strptime(datetoupdate, "%Y-%m-%d").date()
dbhistprice = db500Data[['Ticker', 'PX_LAST']]
dbhistprice.index = dbhistprice['Ticker']
del dbhistprice['Ticker']

bloomhistprice = BloomRequestData(TOP500TICKERS, action = 'not insert', startdate = checkDate.strftime('%Y%m%d'), enddate = checkDate.strftime('%Y%m%d'))
bloomhistprice.PX_LAST = [float(i) for i in bloomhistprice.PX_LAST]

#while datetime.datetime.now().time() <= datetime.time(15, 31, 0):
#    print 'sleeping'
#    time.sleep(2*60)
#if len(TOP500TICKERS)>0:
print('Data Fetching Started')
#bloomhistprice = BloomRequestData(TOP500TICKERS, action = 'not insert', startdate = datetoupdate.strftime('%Y%m%d'), enddate = datetoupdate.strftime('%Y%m%d'))
#bloomhistprice.PX_LAST = [float(i) for i in bloomhistprice.PX_LAST]
#dbhistprice = GetStocksPriceByDate(datetoupdate, TOP500TICKERS)


newintroduced = []
for tickr in list(set.intersection(set(bloomhistprice.index), set(dbhistprice.index))):
    if numpy.abs(bloomhistprice.ix[tickr].values[0] - dbhistprice.ix[tickr].values[0]) > 0.001:
        newintroduced.append(tickr)

diff = list(set.difference(set(TOP500TICKERS), set(dbhistprice.index)))
if len(diff)>0:
    for item in diff:
        newintroduced.append(item)
oldtickers = list(set.difference(set(TOP500TICKERS), set(newintroduced)))

#if len(newintroduced)>50:
#    import pdb
#    pdb.set_trace()


if len(newintroduced)>0:
    d1 = BloomRequestData(newintroduced, startdate = "20000101")
#d2 = BloomRequestData(oldtickers, startdate = datetoupdate.strftime('%Y%m%d')) # now getting data from Cline

print('Price Data Finished')
## For indices



#while datetime.datetime.now().time() <= datetime.time(15, 30, 0):
#        time.sleep(1*60)


indexoldtickers = ['XU100 INDEX',  'DAX INDEX',  'UKX INDEX',  'TOP40 INDEX',  'XAU Curncy',  'INPIINDY INDEX',  'FIINNET$ INDEX',  'NSEADVC INDEX',  'NSEADCL INDEX',  'INVIXN INDEX',  'INR Curncy',
 'MXN CURNCY',  'MXAR INDEX',  'MXBD INDEX',  'MXCN INDEX',  'MXEG INDEX',  'MXEF Index',  'MXMX INDEX',  'NDX INDEX',  'NIFTY Index',  'CNX500 INDEX',  'CNXAUTO INDEX',  'CNXBANK INDEX',
 'CNXNRG INDEX',  'CNXFMCG INDEX',  'CNXINFR INDEX',  'CNXIT INDEX',  'CNXMET INDEX',  'CNXPHRM INDEX',  'GBEES IN EQUITY',  'RIY INDEX',  'SPX Index',  'BSE100 Index',  'BSE200 INDEX',
 'BSE500 INDEX', 'NSEMCAP INDEX','NSESMCP INDEX', 'NIFTYJR INDEX']
#d3 = BloomRequestData(indexnewintroduced,type = 'index')

#indexoldtickers = ['NIFTYJR INDEX']

#datetoupdate = datetime.datetime(2018, 5, 11)
d4 = BloomRequestData(indexoldtickers,sectype = 'index', startdate = datetoupdate.strftime('%Y%m%d'))

print('Index Data Finished')

## For AnalystData
analyststartdate = GetAnalystDataLastUpdateDate()
analyststartdate = min(datetoupdate, analyststartdate)
if len(newintroduced)>0:
    d5 = BloomRequestData(newintroduced, datatype = 'analystdata')
d6 = BloomRequestData(oldtickers, datatype = 'analystdata', startdate = analyststartdate.strftime('%Y%m%d')) #analyststartdate.strftime('%Y%m%d')


### BEST EPS DATA
#epsoldtickers, epsnewtickers, epsstartdate = BESTEPSGetTickers()
if len(newintroduced)>0:
    d7 = BloomRequestData(newintroduced,datatype = 'BEST_EPS_DATA', startdate = datetime.date(2000, 1, 1).strftime('%Y%m%d') )
d8 = BloomRequestData(oldtickers,datatype = 'BEST_EPS_DATA', startdate = analyststartdate.strftime('%Y%m%d'))
print('Analyst Data Finished')


####### Processing ANR Data ######
print('ANR Data')







tickerslist = list(set.union(set(oldtickers), set(newintroduced)))
d9 = GetANRBulkData(tickerslist) 

#d10 = GetDividendBulkData(TOP500TICKERS)
print('ANR Bulk Data Finished')




#indexoldtickers, indexnewintroduced, indexdatetoupdate = IndexTickerstoUpdate()

bloompricedataconn.close()
bloombergindexdataconn.close()
analystconn.close()

print('Successfully updated')
print('1). Indices from Date: ',  datetoupdate)
print('2). Stocks from Date: ',  datetoupdate)
print('3). Analyst Data from Date: ', analyststartdate)
print('4). BEST EPS Data from Date: ', datetoupdate)
print('5). ANR Bulk Data: ')
#ttp = pandas.read_csv('W:/BSE200-Aug15Tickers.txt', header = False)
#datetoupdate = datetime.date(2015, 8, 31)
#analyststartdate = datetime.date(2015, 8, 31)
#indexdatetoupdate = datetime.date(2015, 9, 31)


#=BDP("SBIN IN Equity", "BEST_ANALYST_REC", "BEST_DATA_SOURCE_OVERRIDE="&"JPM")
'''