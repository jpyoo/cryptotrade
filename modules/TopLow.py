#Sunday 4:00pm PST is time to run
import csv
import math
from statistics import median
from modules.DBconnect import myDB
#using Binance
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import pandas as pd
import numpy as np

class toplow():
    def __init__(self):
        self.f = open('binance.key', 'r')
        self.client = Client(self.f.readline().strip(), self.f.readline().strip(), tld='us')

        self.mydb = myDB()
        self.mydb.load_db('db.key')
        self.mydb.dbconnect()

    #Getting All symbols from Binance US
    #Get 20 symbols per IP to not get blocked
    def getBiSymbols(self):
        raw_symbols = self.client.get_all_tickers()
        bi_symbols = []

        for s in range(len(raw_symbols)):
            bi_symbols.append(raw_symbols[s]['symbol'])

        for i in range(3):
            for w in bi_symbols:
                if w.endswith('USD'):
                    pass
                else:
                    bi_symbols.remove(w)
        for i in range(3):
            for w in bi_symbols:
                if w.endswith('BUSD'):
                    bi_symbols.remove(w)
                else:
                    pass
        return bi_symbols
    def getSymbols(self):
        symbols = []
        for elem in pd.read_sql_table('hltable', self.mydb.engine).columns:
            if elem == 'time':
                continue
            elif elem == 'index':
                continue
            elif elem == 'level_0':
                continue
            elif elem == 'level_1':
                continue
            symbols.append(elem.split('-')[0])
        symbols = set(symbols)
        return list(symbols)

    def getHLTable(self, bi_symbols, since):
        hltable=pd.DataFrame()

        #Get 20 symbols per IP to not get blocked
        for bs in range(0,len(bi_symbols)):
            klines = self.client.get_historical_klines(bi_symbols[bs], Client.KLINE_INTERVAL_1WEEK, since)
            df = pd.DataFrame(klines,columns = ['OpenTime','Open','High','Low','Close','Volume','CloseTime','QAV','Trades','TBAV','TQAV','ignore'])
            df = df.iloc[::-1]
            if bs == 0:
                hltable['time'] = df['OpenTime']
            hltable[bi_symbols[bs]+"-Close"] = df['Close']
            hltable[bi_symbols[bs]+"-High"] = df['High']
            hltable[bi_symbols[bs]+"-Low"] = df['Low']


        hltable.dropna(axis='columns', inplace=True)
        return hltable

    def getHLCompareTable(self, hltable):
        hlCompareTable=pd.DataFrame()
        hlCompareTable2=pd.DataFrame()

        #hlCompare Table saves Area, hlcompareTable2 saves High Price 
        tlen = hltable.shape[0]
        for col in hltable.columns:
            currency = col.split('-')[0]
            if col == 'time':
                continue
            elif col == 'index':
                continue
            elif col == 'level_0':
                continue
            elif col == 'level_1':
                continue
            elif currency in hlCompareTable.columns and currency in hlCompareTable2.columns:
                continue
            else:
                valueList = np.zeros(shape=(tlen-1,1))
                secondValueList = np.zeros(shape=(tlen-1,1))
                for i in range(tlen-1):
                    try:
                        c = float(hltable[currency+'-Close'][i+1])
                    except:
                        print(i)
                        print(currency)
                    h = float(hltable[currency+'-High'][i])
                    l = float(hltable[currency+'-Low'][i])

                    if pd.isna(c) and pd.isna(h) and pd.isna(l):    
                        break
                    else:
                        valueList[i][0] =((h-l)/c*100)
                        secondValueList[i][0] =((h-c)/c*100)
                hlCompareTable[currency] = pd.DataFrame(valueList, columns = [currency])[currency]
                hlCompareTable2[currency] = pd.DataFrame(secondValueList, columns = [currency])[currency]
        return hlCompareTable, hlCompareTable2


    # Get table of runners: calculated base price and runner price
    # CalcTable saves area difference, CalcTable2 saves high price difference.
    def getHLCalcTable(self, hlCompareTable, hlCompareTable2):
        tlen = hlCompareTable.shape[0]
        hlCalcTable=pd.DataFrame()
        hlCalcTable2=pd.DataFrame()

        for col in hlCompareTable.columns:
            if col == 'BTCUSD':
                pass
            elif col in hlCalcTable.columns and col in hlCalcTable2.columns:
                pass
            else:
                calcList = np.zeros(shape=(tlen-1,1))
                secondCalcList = np.zeros(shape=(tlen-1,1))
                for i in range(tlen-1):
                    baseArea = float(hlCompareTable['BTCUSD'][i+1])
                    runnerArea = float(hlCompareTable[col][i])

                    baseHigh = float(hlCompareTable2['BTCUSD'][i+1])
                    runnerHigh = float(hlCompareTable2[col][i])

                    calcList[i][0] = baseArea - runnerArea
                    secondCalcList[i][0] = baseHigh - runnerHigh

                hlCalcTable[col] = pd.DataFrame(calcList, columns = [col])[col]
                hlCalcTable2[col] = pd.DataFrame(secondCalcList, columns = [col])[col]
        return hlCalcTable, hlCalcTable2

    # Get a table of similarly moved runners
    def getSimilarTable(self, hlCalcTable, hlCalcTable2, hlCompareTable, hlCompareTable2):
        tlen = hlCalcTable.shape[0]
        similarTable=pd.DataFrame()
        signalCol = []
        signalArea = []
        signalHigh = []
        resultHigh = []
        signalIndex = []
        for col in hlCalcTable.columns:

            for i in range(tlen):
                if abs(hlCalcTable[col][i]) < 1:
                    if hlCalcTable2[col][i] < 1:
                            signalArea.append(round(hlCompareTable['BTCUSD'][i+1],2))
                            signalHigh.append(round(hlCompareTable2['BTCUSD'][i+1],2))
                            resultHigh.append(round(hlCompareTable2[col][i],2))
                            signalIndex.append(i+1)
                            signalCol.append(col)
        try:
            similarTable['Signal Col'] = pd.Series(signalCol)
            similarTable['Signal Area'] = pd.Series(signalArea)
            similarTable['Signal High'] = pd.Series(signalHigh)
            similarTable['Result High'] = pd.Series(resultHigh)
            similarTable['Signal Index'] = pd.Series(signalIndex)
        except:
                print("An exception occurred")
        similarTable = similarTable.loc[similarTable['Result High'] > 1]
        return similarTable

    #Getting Max signaled currency
    def getPivotTable(self, similarTable):
        pivotTable = similarTable.pivot_table(columns=['Signal Col'], aggfunc='size')
        pivotTable.sort_values(ascending=False, inplace =True)
        return pivotTable

    def getMaxList(self, pivotTable):
        maxSignal = pivotTable.max()

        maxSeries = pd.DataFrame(pivotTable.where(pivotTable.eq(maxSignal))).stack()
        maxList = []
        for i in range(maxSeries.size):
            maxList.append(maxSeries.index[i][0])
        return maxList

    # Get most high currency among max signaled currencies
    def getBuySignal(self, pivotTable, similarTable, hlCompareTable, hlCompareTable2):
        maxUnit = ''
        buySignal = False
        for m in pivotTable.index:
            signalIndexList = []
            if pivotTable[m] < 5:
                break
            indexList = similarTable.where(similarTable['Signal Col'] == m).dropna().index
            signalHighList = []
            signalAreaList = []
            for i in indexList:
                signalHighList.append(similarTable.where(similarTable['Signal Col'] == m).dropna()['Signal High'][i])
                signalAreaList.append(similarTable.where(similarTable['Signal Col'] == m).dropna()['Signal Area'][i])

            signalEvaluateTable = hlCompareTable[['BTCUSD', m]]
            signalEvaluateTable['BTCUSD-High'] = hlCompareTable2['BTCUSD']
            signalEvaluateTable[m+'-High'] = hlCompareTable2[m]

            for i in range(len(signalHighList)):
                sIndexList = signalEvaluateTable.loc[(signalEvaluateTable['BTCUSD'] > signalAreaList[i]-0.5) & (signalEvaluateTable['BTCUSD'] < signalAreaList[i]+0.5) & (signalEvaluateTable['BTCUSD-High'] < signalHighList[i]+0.5) & (signalEvaluateTable['BTCUSD-High'] < signalHighList[i]+0.5)].index
                for s in sIndexList:
                    signalIndexList.append(s)

            #if signalIndexList contains 0 -> buy signal
            # print(set(signalIndexList))
            if signalIndexList.count(0) > 0:
                buySignal = True
                maxUnit = m
                break
            else:
                buySignal = False
        return maxUnit, buySignal, signalIndexList

    # Result Median High is expected profit %
    def getMed(self, signalIndexList):
            sigCount = 0
            sigResultList = []
            for idx in signalIndexList:
                try:
                    sigResultList.append(hlCompareTable2[maxUnit][idx-1])
                    sigCount+=1
                except:
                    pass
            if sigResultList == []:
                med = 0
            else:
                med = median(sigResultList)
            return med

    def updateDB(self, maxUnit, buySignal, signalIndexList, target):
        if buySignal:
            print('Signal Unit: ', maxUnit)        
            print('Result median High: ', round(target,2))
            print('Signal Count : ',len(sigResultList))

            self.mydb.runQuery(maxUnit, round(target,2),"True")

        else:
            self.mydb.runQuery('None', 0,"False")
            print('NO SIGNAL')

    def addNewWeekData(self):
        symbols = self.getSymbols()
        updatetable = self.getHLTable(symbols[:], "1 week ago UTC")
        hltable = pd.read_sql_table('hltable', self.mydb.engine)
        df = pd.concat([updatetable, hltable]).drop_duplicates(subset=['time']).reset_index(drop = True)
        df.to_sql('hltable', con=self.mydb.engine, if_exists='replace')

    def runStrategy(self):
        hltable = pd.read_sql_table('hltable', self.mydb.engine)[:52]
        hlCompareTable, hlCompareTable2 = self.getHLCompareTable(hltable)
        hlCalcTable, hlCalcTable2 = self.getHLCalcTable(hlCompareTable, hlCompareTable2)

        similarTable = self.getSimilarTable(hlCalcTable, hlCalcTable2, hlCompareTable, hlCompareTable2)

        pivotTable = self.getPivotTable(similarTable)

        maxList = self.getMaxList(pivotTable)

        maxUnit, buySignal, signalIndexList = self.getBuySignal(pivotTable, similarTable, hlCompareTable, hlCompareTable2)

        #If buySignal = Ture then, check if next_series_hlCompareTable2[0] > target
        #If true, result *= target
        #If false, result *= next_series_hltable-Close[-2] - next_series_hltable-Close[-1]

        target = self.getMed(signalIndexList)
        self.updateDB(maxUnit, buySignal, signalIndexList, target)