from __future__ import annotations

import asyncio
import logging
import dateutil
import pandas as pd
import datetime as dt

import fxcmpy
from fxcmpy import fxcmpy_tick_data_reader as tdr

from adit.config import Config

__all__ = ['FXCMReceiver']


class FXCMReceiver:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.asyncio_loop = asyncio.get_event_loop()
        self.config = Config.instance()

        self.access_token = self.config.get_str("fxcm", "token")
        self.period = self.config.get_str("fxcm", "period")
        self.user = self.config.get_str("fxcm", "user")
        self.pwd = self.config.get_str("fxcm", "pass")
        self.currency_pair = self.config.get_str("fxcm", "ccypairs")

    def start(self) -> None:
        try:
            assert self.access_token is not None, ""
        except:
            pass
        self.runing_task = self.asyncio_loop.create_task(self.run())

    def stop(self) -> None:
        self.runing_task.cancel()

    async def crawl(self) -> None:
        pass

    async def run(self):
        while True:
            pass


# con = fxcmpy.fxcmpy(access_token = const.FXCM_TOKEN, log_level = 'error')
# instruments = con.get_instruments_for_candles()
# print(instruments)
# start = dt.datetime(2007, 1, 1)
# stop = dt.datetime(2020, 6, 3)
# data = con.get_candles('EUR/USD', period='W1', start=start, stop=stop)
# print(data.head())
# print(list(data))
# con.close()

#print(tdr.get_available_symbols())
#start = dt.datetime(2020, 5, 1)
#end = dt.datetime(2020, 5, 31)
#dr = tdr('EURUSD', start, end, verbosity=True)
#raw_data = dr.get_raw_data()
#print(raw_data.info())
