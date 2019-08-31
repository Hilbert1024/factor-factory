# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime
import mysql.connector as mysql
import selectdatasilent as sd
import statsmodels.api as sm # statistical models


import warnings
warnings.filterwarnings("ignore")

"""
Toolkit functions
"""

def getToday():
    today = datetime.date.today() #.strftime("%Y-%m-%d")
    today = pd.Timestamp(today)
    return today

def str2ts(strs):
    timestamp = pd.to_datetime(strs, format = "%Y-%m-%d")
    return timestamp

def ts2str(ts):
    strs = ts.strftime("%Y-%m-%d")
    return strs

def df2mat(*args):
    if isinstance(args[0], pd.DataFrame):
        factorMat = np.matrix(args[0])
    else:
        factorMat = np.matrix(args).T
    return factorMat

def intersectArr(*args):
    if len(args) == 1:
        return args[0]
    else:
        temp = args[0]
        for arr in args[1:]:
            temp = np.intersect1d(temp,arr)
    return temp

def toLastMin(date):
    return date + datetime.timedelta(hours = 15)

def timeSeriesMin2timeSeries(timeSeriesMin):
    return np.sort(list(map(pd.Timestamp,set(timeSeriesMin.date))))
    
def getInfo(user, password, host):
    cnxDaily = mysql.connect(user=user, password=password, host=host, database = 'daily_data')
    cursorDaily = cnxDaily.cursor()
    cursorDaily.execute("SELECT timestamp FROM daily_data WHERE ticker = '000001'")
    timeStamp = np.array([pd.Timestamp(t[0]) for t in cursorDaily.fetchall()])
    cursorDaily.execute("SHOW columns FROM daily_data")
    field = [t[0] for t in cursorDaily.fetchall()]
    cnxMin = mysql.connect(user=user, password=password, host=host, database = 'stock_list')
    cursorMin = cnxMin.cursor()
    cursorMin.execute("SELECT ticker From stock")
    stockList1 = np.array([s[0] for s in cursorMin.fetchall()])
    cursorMin.execute("SELECT ticker From stock")
    stockList2 = np.array([s[0] for s in cursorMin.fetchall()])
    return timeStamp, stockList1, stockList2, field

def getTargetDate(timeStamp, targetDate):
    if not isinstance(targetDate, list):
        targetDate = [targetDate, targetDate]
    elif len(targetDate) == 1:
        targetDate = [targetDate[0], targetDate[0]]
    else:
        pass
    startDate, endDate = targetDate
    startDate = str2ts(startDate) if type(startDate) == str else startDate
    endDate = str2ts(endDate) if type(endDate) == str else endDate
    tick = 1
    if endDate in timeStamp:
        pass
    else:
        if endDate>timeStamp[-1]:
            print("Please wait for data updates. End date will be set as {}."\
                  .format(ts2str(timeStamp[-1])))
        while(timeStamp[-tick]>endDate):
            tick += 1
        endDate = timeStamp[-tick]
    if startDate in timeStamp:
        pass
    elif startDate > endDate:
        startDate = endDate
    else:
        while(timeStamp[-tick]>startDate):
            tick += 1
        startDate = timeStamp[-tick + 1]
    targetDate = [startDate, endDate]
    return targetDate

    
def getTime(timeStamp, pastDays, targetDate):
    timeIndex = np.argwhere(timeStamp == targetDate[0])[0,0]#timeStamp.index(targetDate)
    startDate = timeStamp[timeIndex - pastDays + 1]
    return startDate

def getTimeSeries(timeStamp, targetDate):
    startIndex = np.argwhere(timeStamp == targetDate[0])[0,0]
    endIndex = np.argwhere(timeStamp == targetDate[1])[0,0]
    return timeStamp[startIndex:endIndex+1]
    

def getData(*args):
    objData = sd.BHQ(*args)
    factorData = objData.selectDataMain()
    objPriceAdj = sd.dataPost(factorData)
    priceAdj = objPriceAdj.priceAdjMap(method = 'f')
    priceList = ['close','open','high','low']
    for item in factorData.keys():
        if item in priceList:
            factorData[item] = factorData[item] * priceAdj
        else:
            pass
    return factorData
    
def getUsedData(dataframe, timeStamp, startIndex, pastDays, dtype):
    if dtype == 'daily':
        df = dataframe.loc[:,timeStamp[startIndex - pastDays + 1]:timeStamp[startIndex]]
    elif dtype == 'min':
        df = dataframe.loc[:,timeStamp[startIndex - pastDays*240 + 1]:timeStamp[startIndex]]
    else:
        print("Please choose dtype from ['daily','min'].")
        df = dataframe
    return df

def getReturn(stockClose, step = 1):
    """
    Return of the close series.
    
    Parameters
    ----------
    stockClose : pd.DataFrame
        index is ticker and columns is timestamp.
    step : int
        (step + 1) is the length of sliding windows, default is 1 which returns
        daily/min return. When step > 1, results don't divided by step, so
        you might do this to get average daily/min return.
    """
    if isinstance(stockClose, pd.Series):
        stockClose = pd.DataFrame(stockClose).T
    stockCloseValue = stockClose.values
    stockReturnMatrix = stockCloseValue[:,step:] / stockCloseValue[:,:-step] - 1
    stockReturnMatrix[np.isinf(stockReturnMatrix)] = np.nan
    stockReturn = pd.DataFrame(stockReturnMatrix,
                               index = stockClose.index,
                               columns = stockClose.columns[step:])
    return stockReturn

def getAvalIndex(dataframe, pattern = 'nan'):
    """
    Delete rows which contain pattern.
    
    Parameters
    ----------
    dataframe : pd.DataFrame
    pattern : {'nan','inf'} or int,float, default 'nan'
        a pattern to match specific(usually abnormal) data in dataframe.
    """
    dfIndex = dataframe.index
    if isinstance(dataframe, pd.Series):
        if pattern == 'nan': # can not let x == np.nan but np.isnan(x)
            avalIndex = np.array(dfIndex[np.sum(np.isnan(dataframe)) == 0])
        elif pattern == 'inf':
            avalIndex = np.array(dfIndex[np.sum(np.isinf(dataframe)) == 0])
        else:
            avalIndex = np.array(dfIndex[np.sum(dataframe == pattern) == 0])
    else:
        if pattern == 'nan': # can not let x == np.nan but np.isnan(x)
            avalIndex = np.array(dfIndex[np.sum(np.isnan(dataframe), axis=1) == 0])
        elif pattern == 'inf':
            avalIndex = np.array(dfIndex[np.sum(np.isinf(dataframe), axis=1) == 0])
        else:
            avalIndex = np.array(dfIndex[np.sum(dataframe == pattern, axis=1) == 0])
    return avalIndex
    
def getResiduals(endog, exog, add_constant = True):
    Y = endog
    X = sm.add_constant(exog) if add_constant else exog
    model = sm.OLS(Y,X)
    results = model.fit()
    return results.resid

def getRsquared(endog, exog, add_constant = True):
    Y = endog
    X = sm.add_constant(exog) if add_constant else exog
    rSquared = []
    for col in range(Y.shape[1]):
        tempY = Y[:,col]
        model = sm.OLS(tempY,X)
        results = model.fit()
        rSquared.append(results.rsquared)
    return np.array(rSquared)

def getPercent(dataFrame, axis = 0):
    """
    Percent of data in each column/row.
    
    Parameters
    ----------
    axis : int
        0 by columns while 1 by index.
    """
    if axis:
        dataFrame = (dataFrame.T / np.sum(dataFrame, axis = 1)).T
    else:
        dataFrame = dataFrame / np.sum(dataFrame, axis = 0)
    return dataFrame

def getQuantile(dataFrame, quantile, axis = 0, tail = True):
    """
    Quantile of data in each column/row.
    
    Parameters
    ----------
    quantile : array_like of float
        Quantile or sequence of quantiles to compute, which must be between
        0 and 1 inclusive.
    axis : int
        0 by columns while 1 by index.
    tail : bool
        True means counting from 0 to quantile,
        False means counting from 1 to quantile.
    """
    if axis:
        quantileArr = np.quantile(dataFrame, quantile, axis = 1)
        quantileIndex = (dataFrame.T < quantileArr).T if tail\
        else (dataFrame.T > quantileArr).T
        dataFrame = dataFrame[quantileIndex]
    else:
        quantileArr = np.quantile(dataFrame, quantile, axis = 0)
        quantileIndex = dataFrame < quantileArr if tail\
        else dataFrame > quantileArr
        dataFrame = dataFrame[quantileIndex]
    return dataFrame

def getSortSeries(series, start = 1, end = 1, mode = 'min'):
    if mode == 'min':
        argsortSeries = np.argsort(series)
        series = series[argsortSeries[start-1:end]]
    elif mode == 'max':
        argsortSeries = np.argsort(-series)
        series = series[argsortSeries[start-1:end]]
    else:
        print("Please choose mode from ['min','max'].")
        return
    return series
        
def getSlidingWindow(series, windowRange, step = 1, mode = 'mean', func = False):
    """
    The results of series with silding window.
    
    Parameters
    ----------
    series : pd.Series or np.ndarry or list
    windowRange : int
        Size of the moving window. This is the number of observations used for
        calculating the statistic. Each window will be a fixed size.
    step : int
        The length of step that window moves.
    mode : str
        Provide a method for calculating.
        See the notes below for further information.
    
    Notes
    -----
    The recognized mode are:

    * ``mean``
    * ``std``
    * ``median``
    * ``max``
    * ``min``
    * ``corr``
    * ``cov``
    * ``sum``
    * ``return``
    * ``apply(with func)``
    """
    if mode == 'apply':
        if isinstance(series, pd.Series):
            series = eval('series.rolling(windowRange).' + 'apply(func)')
            series = series.values[windowRange - 1:][::step]
        else:
            series = pd.Series(series)
            series = eval('series.rolling(windowRange).' + 'apply(func)')
            series = series.values.flatten()[windowRange - 1:][::step]
    elif mode == 'return':
        if isinstance(series, pd.Series):
            series = eval('series.rolling(windowRange).' + 'apply(lambda x:x[-1] / x[0] - 1)')
            series = series.values[windowRange - 1:][::step]
        else:
            series = pd.Series(series)
            series = eval('series.rolling(windowRange).' + 'apply(lambda x:x[-1] / x[0] - 1)')
            series = series.values.flatten()[windowRange - 1:][::step]
    else:
        if isinstance(series, pd.Series):
            series = eval('series.rolling(windowRange).'+mode+'()')
            series = series.values[windowRange - 1:][::step]
        else:
            series = pd.Series(series)
            series = eval('series.rolling(windowRange).'+mode+'()')
            series = series.values.flatten()[windowRange - 1:][::step]
    return series
