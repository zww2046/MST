# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 17:02:50 2019

@author: Wangwei
"""

import datetime
from dateutil.relativedelta import relativedelta
import sqlalchemy
import requests_cache
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from statsmodels.tsa.vector_ar.vecm import coint_johansen

engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:oneway@localhost/marketdata')
expire_after = datetime.timedelta(days=3)
session = requests_cache.CachedSession(cache_name='cache', backend='sqlite', expire_after=expire_after)
start = datetime.datetime(2018,12, 1)
end = datetime.datetime(2019, 6, 30)

symbolXAGETF = 'SLV'
symbolXAUETF = 'GLD'
symbolXPTETF = 'PPLT'

sqlQuery = 'select * from equity_market where symbol=%s and date>=%s and date<=%s order by date'
silverETF = pd.read_sql(sqlQuery,con=engine,params=[symbolXAGETF,start,end])
goldETF = pd.read_sql(sqlQuery,con=engine,params=[symbolXAUETF,start,end])
platinumETF = pd.read_sql(sqlQuery,con=engine,params=[symbolXPTETF,start,end])

#plot time series
combinedETF = silverETF.merge(goldETF,on='date',how='outer',suffixes=('_silver','_gold'),sort=True)[-1000:-1]
fig = plt.figure(figsize=(20,20))
fig,ax = plt.subplots(3,1)
ax[0].plot(combinedETF["date"],combinedETF["close_silver"],color='b')
ax[0].set_ylabel('silver',color='b')
ax1 = ax[0].twinx()
ax1.plot(combinedETF["date"],combinedETF["close_gold"],color='r')
ax1.set_ylabel('gold',color='r')

#plot gold/silver ratio
AUAGRatio = np.log(combinedETF['close_gold']/combinedETF['close_silver'])
ax[1].hist(AUAGRatio.dropna()[-1000:-1],bins=20,density=True)
ax[2].plot(combinedETF['date'],AUAGRatio)

fig.savefig('plot.png',dpi=1000)
plt.close() 

# test cointegration using Johansen test
jres = coint_johansen(np.log(combinedETF.dropna().loc[:,['close_silver','close_gold']]), det_order=0, k_ar_diff=1)

#detect cointegration groups
def scanPairs1(indicator):
    #get stocks contains key words
    engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:oneway@localhost/marketdata')
    #1 denotes precious metal, 2 denotes industrial metal, 3 denotes energy, 4 denotes agriculture
    preciousMetalKeyWords = ['Precious Metal','Gold ','Silver ','Platinum']
    industrialMetalKeyWords = ['Industrial Metal','Copper','Lithium']
    energyKeyWords = ['Energy','Oil','Gas','Brent','WTI']
    agricultureKeyWords = ['Agriculture','Wheat','Soybean','Corn']
    sqlQuery = 'select symbol from symbol where remarks ~ %s'
    symbols = pd.DataFrame()
    if indicator==1:
        keyWords = preciousMetalKeyWords
    elif indicator==2:
        keyWords = industrialMetalKeyWords
    elif indicator==3:
        keyWords = energyKeyWords
    else:
        keyWords = agricultureKeyWords
    for keyWord in keyWords:
        symbols = symbols.append(pd.read_sql(sqlQuery,con=engine,params=[keyWord]),ignore_index=True)
    
    #filter stocks by volume
    end = datetime.date.today()
    start = (end-relativedelta(months=12)).replace(day=1)
    sqlQuery = 'select * from (select symbol, min(close*volume) as min_vol,count(1) as no_trade_day from equity_market where date>=%s and date<=%s and symbol in %s group by symbol) a where min_vol>=1000000 and no_trade_day>=200 order by symbol'
    symbols = pd.read_sql(sqlQuery,con=engine,params=[start,end,tuple(symbols['symbol'].tolist())])    
    
    #load close prices
    sqlQuery = 'select symbol,date,close from equity_market where date>=%s and date<=%s and symbol in %s order by symbol,date'
    if symbols.count()[0]>=2:
        closes = pd.read_sql(sqlQuery,con=engine,params=[start,end,tuple(symbols['symbol'].tolist())]) 
        matCloses = pd.pivot_table(closes,values='close',index=['date'],columns=['symbol'],aggfunc=np.max)  
    else:
        print('There are less than two stocks.')
        return
    
    #detect pairs
#    nSymbol = symbols.count()[0]
#    pairs = pd.DataFrame()
#    for i in range(nSymbol):
#        for j in range(i+1,nSymbol):
#            jres = coint_johansen(np.log(matCloses.dropna().iloc[:,[i,j]]), det_order=0, k_ar_diff=1) 
#            if 
    jres = coint_johansen(np.log(matCloses.dropna()), det_order=0, k_ar_diff=1)
    matReject = jres.cvm-jres.lr2[np.newaxis,:].transpose()*np.ones((jres.eig.size,3))
    #0 denotes failing to reject null hyperthesis, 1 denotes reject null hyperthesis at 90% confident interval,
    #2 denotes 95% and 3 denotes 99%
    matReject[matReject>=0]=0
    matReject[matReject<0]=1
    CIIndicator = np.sum(matReject,axis=1)
        
    return jres.evec,jres.eig,CIIndicator

#detect cointegration pairs
def scanPairs2(indicator,**kwargs):
    
    #get stocks contains key words
    engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:oneway@localhost/marketdata')
    #1 denotes precious metal, 2 denotes industrial metal, 3 denotes energy, 4 denotes agriculture
    preciousMetalKeyWords = ['Precious Metal','Gold ','Silver ','Platinum']
    industrialMetalKeyWords = ['Industrial Metal','Copper','Lithium']
    energyKeyWords = ['Energy','Oil','Gas','Brent','WTI']
    agricultureKeyWords = ['Agriculture','Wheat','Soybean','Corn']
    sqlQuery = 'select symbol from symbol where remarks ~ %s'
    symbols = pd.DataFrame()
    if indicator==1:
        keyWords = preciousMetalKeyWords
    elif indicator==2:
        keyWords = industrialMetalKeyWords
    elif indicator==3:
        keyWords = energyKeyWords
    else:
        keyWords = agricultureKeyWords
    for keyWord in keyWords:
        symbols = symbols.append(pd.read_sql(sqlQuery,con=engine,params=[keyWord]),ignore_index=True)
    
    #filter stocks by volume
    end = kwargs.get('end_date',None)
    if end is None:
        end = datetime.date.today()
    else:
        end = datetime.datetime.strptime(end, '%Y%m%d').date()
    start = (end-relativedelta(months=12)).replace(day=1)
    sqlQuery = 'select * from (select symbol, min(close*volume) as min_vol,count(1) as no_trade_day from equity_market where date>=%s and date<=%s and symbol in %s group by symbol) a where min_vol>=1000000 and no_trade_day>=200 order by symbol'
    symbols = pd.read_sql(sqlQuery,con=engine,params=[start,end,tuple(symbols['symbol'].tolist())])    
    
    #load close prices
    sqlQuery = 'select symbol,date,close from equity_market where date>=%s and date<=%s and symbol in %s order by symbol,date'
    if symbols.count()[0]>=2:
        closes = pd.read_sql(sqlQuery,con=engine,params=[start,end,tuple(symbols['symbol'].tolist())]) 
        matCloses = pd.pivot_table(closes,values='close',index=['date'],columns=['symbol'],aggfunc=np.max)  
    else:
        print('There are less than two stocks.')
        return
    
    #detect pairs
    nSymbol = symbols.count()[0]
    pairs = pd.DataFrame()
    for i in range(nSymbol):
        for k in range(i+1,nSymbol):
            jres = coint_johansen(np.log(matCloses.dropna().iloc[:,[i,k]]), det_order=0, k_ar_diff=1) 
            matReject = jres.cvt-jres.lr1[np.newaxis,:].transpose()*np.ones((jres.eig.size,3))
            matReject[matReject>=0]=0
            matReject[matReject<0]=1
    #0 denotes failing to reject null hyperthesis, 1 denotes reject null hyperthesis at 90% confident interval,
    #2 denotes 95% and 3 denotes 99%            
            testResult = np.sum(matReject,axis=1)
            if testResult[0]>0:
                pairs = pairs.append(pd.DataFrame({'symbol1': [symbols.iloc[i,0]],'symbol2':[symbols.iloc[k,0]],
                                           'ratio1': [jres.evec[0,0]],'ratio2':[jres.evec[1,0]],
                                           'r=0': [testResult[0]],'r<=1':[testResult[1]],'date':end}),ignore_index=True)
    
    #plot time series   
    plot_flag = kwargs.get('plot_flag',None)
    if plot_flag==1: 
        for i in range(pairs.count()[0]):
            fig = plt.figure(figsize=(20,20))
            symbol1 = pairs.iloc[i,0]
            symbol2 = pairs.iloc[i,1]
            fig,ax = plt.subplots(2,1)
            ax[0].plot(matCloses.index,matCloses.loc[:,symbol1],color='b')
            ax[0].set_ylabel(symbol1,color='b')
            ax1 = ax[0].twinx()
            ax1.plot(matCloses.index,matCloses.loc[:,symbol2],color='r')
            ax1.set_ylabel(symbol2,color='r')
            
            ax[1].plot(matCloses.index,np.log(matCloses.loc[:,symbol1])*pairs.iloc[i,2]+np.log(matCloses.loc[:,symbol2])*pairs.iloc[i,3])
            filename = '_'.join([symbol1,symbol2,end.strftime("%Y%m%d")])
            filename = filename + '.png'
            fig.savefig(filename,dpi=1000)
            plt.close(fig)
        plt.close()

    return pairs

#evec,eig,CIIndicator = scanPairs1(1)
pairs = scanPairs2(1,end_date='20190630',plot_flag=1)