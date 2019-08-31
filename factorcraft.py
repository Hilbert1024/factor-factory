# -*- coding: utf-8 -*-

import factorfunctions as ff
import dbinfo
import pandas as pd
import numpy as np


class factorGlobal():
    def __init__(self):
        self.user, self.password, self.host = dbinfo.getDataBaseInfo()
        self.timeStamp, self.stockList3513, self.stockList3631, self.field \
        = ff.getInfo(self.user, self.password, self.host)
        return


# ===== Factor_IVFFIVRDaily =====
class Sample(factorGlobal):
    """
    Each factor is an object.
    """
    def __init__(self, pastDays, targetDate, needDays = 0):
        """
        targetDate = [startDate, endDate]
        """
        super(Sample, self).__init__() # Calling parent class
        if needDays == 0:
            self.needDays = pastDays
        else:
            self.needDays = needDays
        self.pastDays = pastDays
        self.targetDate = ff.getTargetDate(self.timeStamp, targetDate)
        return
    
    def factorInfo(self):
        """
        Description of the factor.
        """
        factorName = ""
        Description = ""
        Author = "Hilbert"
        lastUpdate = "2019-08-31"
        print("Name:{}\nDescription:{}\nAuthor:{}\nLast update:{}\n"\
              .format(factorName,Description,Author,lastUpdate))
        return

    def factorGetData(self):
        """
        Data needed.
        Parameter
        ---------
        Nonparametric variables
        """
        db = 'daily' #
        field = ['close'] 
        ticker = 3631 # [3513,3631], default 3631
        eco = 0 # No need for saving memories


        # Load data from Database
        startDate = ff.getTime(self.timeStamp, self.needDays, self.targetDate)
        endDate = self.targetDate[1]
        databaseInfo = [field, self.host, self.user, self.password, ticker, db,
                ff.ts2str(startDate), ff.ts2str(endDate)]
        factorData = ff.getData(*databaseInfo)
        return factorData

    def factorFunc(self, factorData):
        """
        Calculating method of factor.
        """
        # factor parameters
        paraPercent = 1
        # Create a blank dataframe
        timeSeries = ff.getTimeSeries(self.timeStamp, self.targetDate)
        factor = pd.DataFrame(index = self.stockList3631, columns = timeSeries)
        # load all data
        stockCloseAll = factorData['close']
        stockReturnAll = ff.getReturn(stockCloseAll)
        for timeSer in timeSeries:
            # Choose avaliable index
            avalIndex = ff.getAvalIndex(stockReturnAll, pattern = 'nan')
            # Choose avaliable data
            startIndex = np.argwhere(self.timeStamp == timeSer)[0,0]
            stockReturn = ff.getUsedData(stockReturnAll, self.timeStamp, startIndex, self.pastDays, dtype = 'daily')
            stockReturn = stockReturn.loc[avalIndex]
            # calculate factors
            factorCal = 'Calculating method'
            # You may use paraPercent which defined by yourself.
            # fill factor
            factor[avalIndex, timeSer] = factorCal
        return factor
    def factorMain(self, savePath, pklSplit = True, factorInfo = False):
        """
        Main
        """
        if factorInfo:
            self.factorInfo()    
        factorData = self.factorGetData()
        factor = self.factorFunc(factorData)
        if pklSplit:
            for timeSer in factor.columns:
                fac = factor.loc[:,timeSer]
                fac = pd.DataFrame(fac).T
                fac.columns.name = ''
                fac.index.name = ''
                fac = fac.astype(float)
                fac.to_pickle(savePath + '_{}.pkl'.format(ff.ts2str(timeSer)))
        else:
            factor1.to_pickle(savePath + '{} to {}.pkl'\
                             .format(ff.ts2str(self.targetDate[0]), ff.ts2str(self.targetDate[0])))
        return