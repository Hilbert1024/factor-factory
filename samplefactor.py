# -*- coding: utf-8 -*-
class factorName(factorGlobal):
    """
    Each factor is an object.
    """
    def __init__(self, pastDays, targetDate, needDays = 0):
        """
        targetDate = [startDate, endDate]
        """
        super(factorName, self).__init__() # Calling parent class
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
        factorName = "TBD"
        Description = "TBD"
        Author = "TBD"
        lastUpdate = "TBD"
        print("Name:{}\nDescription:{}\nAuthor:{}\nLast update:{}\nLoading data will cost about 600s.\n"\
              .format(factorName,Description,Author,lastUpdate))
        return

    def factorGetData(self):
        """
        Data needed.
        Parameter
        ---------
        Nonparametric variables
        """
        db = 'daily' # For example
        field = ['close'] # For example 
        # See details in self.field(= factorName.field)
        ticker = 1000 # 

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
        
        # Create a blank dataframe
        timeSeries = ff.getTimeSeries(self.timeStamp, self.targetDate)
        factor = pd.DataFrame(index = self.stockList3631, columns = timeSeries)
        # load all data
        dfAll = factorData['']
        
        for timeSer in timeSeries:
            # choose avaliable index
            avalIndex
            # Choose avaliable data
#            Daily
#            startIndex = np.argwhere(self.timeStamp == timeSer)[0,0]
#            df = ff.getUsedData(dfAll, self.timeStamp, startIndex, self.pastDays, dtype = 'daily')
#            Min
#            timeSeriesMin = stockCloseAll.columns
#            timeSerLastMin = ff.toLastMin(timeSer)
#            startIndex = np.argwhere(timeSeriesMin == timeSerLastMin)[0,0]
#            df = ff.getUsedData(dfAll, timeSeriesMin, startIndex, self.pastDays, dtype = 'min')
            
            # calculate factors
            factorCal
            #fill factor
            factor.loc[avalIndex, timeSer] = factorCal

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
                fac.columns.name = 'SecuCode'
                fac.index.name = 'BusDay'
                fac = fac.astype(float)
                fac.to_pickle(savePath + '_{}.pkl'.format(ff.ts2str(timeSer)))
        else:
            factor.to_pickle(savePath + '{} to {}.pkl'\
                             .format(ff.ts2str(self.targetDate[0]), ff.ts2str(self.targetDate[0])))
        # When returning more than 1 factor
#        for index, factor in enumerate(factor):
#            if pklSplit:
#                for timeSer in factor.columns:
#                    fac = factor.loc[:,timeSer]
#                    fac = pd.DataFrame(fac).T
#                    fac.columns.name = 'SecuCode'
#                    fac.index.name = 'BusDay'
#                    fac = fac.astype(float)
#                    fac.to_pickle(savePath[index] + '_{}.pkl'.format(ff.ts2str(timeSer)))
#            else:
#                factor.to_pickle(savePath[index] + '{} to {}.pkl'\
#                                 .format(ff.ts2str(self.targetDate[0]), ff.ts2str(self.targetDate[0])))
        return