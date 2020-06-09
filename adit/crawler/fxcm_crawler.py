import importlib
import pandas as pd
import datetime as dt
import config.const as const
import fxcmpy
from fxcmpy import fxcmpy_tick_data_reader as tdr

#con = fxcmpy.fxcmpy(access_token = const.FXCM_TOKEN, log_level = 'error')
#instruments = con.get_instruments_for_candles()
#print(instruments)
#start = dt.datetime(2007, 1, 1)
#stop = dt.datetime(2020, 6, 3)
#data = con.get_candles('EUR/USD', period='W1', start=start, stop=stop)
#print(data.head())
#print(list(data))
#con.close()

print(tdr.get_available_symbols())
start = dt.datetime(2020, 5, 1)
end = dt.datetime(2020, 5, 31)
dr = tdr('EURUSD', start, end, verbosity=True)
raw_data= dr.get_raw_data()
print(raw_data.info())

#TODO: ForexConnectAPI + https://www.fxcm.com/markets/algorithmic-trading/compare-api/
