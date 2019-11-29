# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 16:11:18 2019

@author: Wangwei
"""

import pandas_datareader.data as web
import pandas_datareader.nasdaq_trader as nasdaq
import datetime
import sqlalchemy
import requests_cache
import pandas as pd


engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:oneway@localhost/marketdata')
expire_after = datetime.timedelta(days=3)
session = requests_cache.CachedSession(cache_name='cache', backend='sqlite', expire_after=expire_after)
start = datetime.datetime(2019, 10, 1)
end = datetime.datetime(2019, 10, 31)

def updateSymbol():
    symbol = nasdaq.get_nasdaq_symbols(retry_count=3, timeout=30, pause=None)
    symbol = symbol[['Security Name','Listing Exchange','Market Category','ETF', 'Round Lot Size','Financial Status', 'CQS Symbol','NASDAQ Symbol','NextShares']]
    symbol.rename(columns={'Security Name':'remarks','Listing Exchange':'exchange','Market Category':'market_category','ETF':'etf_flag',
                          'Round Lot Size':'lot_size','Financial Status':'financial_status','CQS Symbol':'cqs_symbol','NASDAQ Symbol':'symbol','NextShares':'nextshares'},inplace=True)
    engine.execute("truncate table temp_symbol")
    symbol.to_sql('temp_symbol',con=engine,if_exists='append',index=False)
    engine.execute("insert into symbol select *,now() as creation_date from (select * from temp_symbol except select symbol,remarks,exchange,market_category,etf_flag,lot_size,financial_status,CQS_symbol,nextshares from symbol) as a")    

def loadDailyTradingData():
    symbols = pd.read_sql_table('symbol',con=engine,columns=['symbol'])
    symbols = symbols.drop_duplicates(subset=['symbol'])
    engine.execute("truncate table temp_equity_market")
    for symbol in symbols['symbol']:
        try:
            tradingData = web.DataReader(symbol,'yahoo',start, end)
            tradingData = tradingData.reset_index()
            tradingData.rename(columns={'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Adj Close':'adj_close',
                'Date':'date'},inplace=True)
            tradingData['symbol'] = symbol
            tradingData.to_sql("temp_equity_market",con=engine,if_exists='append',index=False)
        except Exception:
            print(symbol)
            continue
    engine.execute("insert into equity_market select * from (select * from temp_equity_market except select * from equity_market) as a")
    
#updateSymbol()
loadDailyTradingData()
engine.dispose()