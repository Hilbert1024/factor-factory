# -*- coding: utf-8 -*-

import factorcraft as fc
import factorfunctions as ff

def factorFactoryMain():
    factorDatabase = ['Sample'] # show factors
    targetDate = ff.getToday()
    factorInfo = True
    
    Sample = fc.Sample(needDays = 60, pastDays = 20, targetDate = targetDate) # targetDate could be one day or an interval.
    Sample.factorMain('factor/Sample/Sample', factorInfo = factorInfo)

if __name__ == "__main__":
    factorFactoryMain()
