from __future__ import annotations

import asyncio
import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import fxcmpy

from adit.config import Config
from adit.controllers import EventLoopController, TileDBController, TPOOL

__all__ = ['FXCMCrawler']


# TODO: current implementation using asyncio -> we need to move to dask distributed scheduler instead.
class FXCMCrawler:
    TASK_NAME = "fxcm-crawler"
    __CRAWLER_CHECKPOINT = "crawler-checkpoint"

    __RETRY_LIMIT = 3

    CRAWLERS_REGISTER = []

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.evl = EventLoopController.instance()
        self.config = Config.instance()
        self.tiledb = TileDBController.instance()
        self.enabled = self.config.get_bool("crawlers", "fxcm")
        self.runing_task = None
        self.fxcm_conn = None
        self.paused = True
        if self.enabled:
            self.config_available = self.config.get_config("fxcm") is not None
            if self.config_available is not None:
                self.user = self.config.get_str("fxcm", "user")
                self.pwd = self.config.get_str("fxcm", "pwd")
                self.access_token = self.config.get_str("fxcm", "token")
                self.frequency = self.config.get_int("fxcm", "frequency", 60)  # default 5 mins
                self.period = self.config.get_str("fxcm", "period", "m1")  # default 1 min
                self.ccypairs = self.config.get_str("fxcm", "ccypairs", "").strip().split("\n")

        self.CRAWLERS_REGISTER.append(self)

    def get_conn(self):
        if self.fxcm_conn is None:
            self.fxcm_conn = fxcmpy.fxcmpy(access_token=self.access_token, log_level='debug')
        elif self.fxcm_conn.connection_status != 'established' or not self.fxcm_conn.is_connected():
            self.fxcm_conn.connect()
        return self.fxcm_conn

    def get_candle(self, fxcm_conn, pair, start, stop):
        try:
            df = fxcm_conn.get_candles(instrument=pair, period=self.period, start=start, stop=stop)
            return df
        except Exception as ex:
            self.logger.fatal(f"Failed to retrieve data for {pair} from {start} to {stop}", exc_info=ex)
            return None

    def start(self) -> None:
        if not self.enabled:
            self.logger.info("fxcm data crawlers is disabled")
            return

        if not self.config_available:
            self.logger.error("Config for fxcm is not available")
            raise Exception("Config for fxcm is not available, please check config file again.")

        self.evl.shedule_task(self.TASK_NAME, self._run)

    def stop(self) -> None:
        self.evl.stop_task(self.TASK_NAME)

    async def _crawl_pair(self, pair, queue):
        pairname = pair.replace("/", "")
        last_timestamp = self.tiledb.get_kv(self.__CRAWLER_CHECKPOINT+"-"+pairname, pairname)
        if last_timestamp is None:
            data_domain = self.tiledb.get_data_domain('raw', pairname)
            last_timestamp = data_domain[1]

        delta_second = 300
        # start = datetime.fromtimestamp(last_timestamp.astype('O')/1e9)
        start = datetime.utcfromtimestamp(last_timestamp.astype('O')/1e9)
        stop = start + timedelta(seconds=delta_second)
        if stop <= (datetime.now() - timedelta(seconds=delta_second)): # only crawl data lag 300 from now
            self.logger.info(f"crawling {pair} from {start} to {stop}")
            fxcm_conn = await self.evl.get_loop().run_in_executor(TPOOL, self.get_conn)
            for retried in range(0, self.__RETRY_LIMIT):  # retried until we get data or reach the retry limit
                df = await self.evl.get_loop().run_in_executor(TPOOL, self.get_candle, fxcm_conn, pair, start, stop)
                if df is not None and not df.empty and len(df.index) > 0:
                    df.index = pd.to_datetime(df.index)
                    self.tiledb.store_kv(self.__CRAWLER_CHECKPOINT+"-"+pairname, pairname, last_timestamp + np.timedelta64(delta_second, 's'))
                    self.tiledb.store_df(datatype="raw", name=pairname, df=df, sparse=True, data_df=True)
                    break
                elif retried >= (self.__RETRY_LIMIT - 1):
                    self.logger.error(f"Crawled data of {pair} from {start} to {stop} is empty. Skiping this time range")
                    self.tiledb.store_kv(self.__CRAWLER_CHECKPOINT + "-" + pairname, pairname, last_timestamp + np.timedelta64(delta_second, 's'))
                else:
                    self.logger.info(f"crawled data of {pair} from {start} to {stop} is empty. Will retry.")

    async def _crawl(self, queue):
        for pair in self.ccypairs:
            pair = pair.strip()
            await self._crawl_pair(pair=pair, queue=queue)

    def toggle_crawler(self):
        self.paused = not self.paused
        self.logger.info(f"FXCM crawler has been toggle {'to paused' if self.paused else 'back to normal'} state")

    async def _run(self, queue):
        self.logger.info("starting fxcm data crawler")
        while True:
            starttime = time.time()
            try:
                if self.paused:
                    self.logger.info(f"FXCM data crawler is currently in paused state.")
                    await asyncio.sleep(self.frequency)
                    continue

                await self._crawl(queue=queue)
                duration = time.time() - starttime
                if (self.frequency - duration) < 0:
                    self.logger.warning("fxcm crawler is taking longer time than the configured period.")
                else:
                    self.logger.info(f"fxcm crawler takes {duration}s to finished")
                await asyncio.sleep(self.frequency)
            except Exception as ex:
                self.logger.error("fxcm crawler has exception", exc_info=ex)
                try:
                    self.stop()
                except:
                    pass
                break
