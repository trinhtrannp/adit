from __future__ import annotations

import asyncio
import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import fxcmpy

from adit.config import Config
from adit.controllers import EventLoop, TileDB
import adit.constants as const

__all__ = ['FXCMCrawler']


# TODO: current implementation using asyncio -> we need to move to dask distributed scheduler instead.
class FXCMCrawler:
    TASK_NAME = "fxcm-crawler"
    __CRAWLER_CHECKPOINT = "crawler-checkpoint"

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.evl = EventLoop.instance()
        self.config = Config.instance()
        self.tiledb = TileDB.instance()
        self.enabled = self.config.get_bool("crawlers", "fxcm")
        self.runing_task = None
        self.fxcm_conn = None
        if self.enabled:
            self.config_available = self.config.get_config("fxcm") is not None
            if self.config_available is not None:
                self.user = self.config.get_str("fxcm", "user")
                self.pwd = self.config.get_str("fxcm", "pwd")
                self.access_token = self.config.get_str("fxcm", "token")
                self.frequency = self.config.get_int("fxcm", "frequency", 60)  # default 5 mins
                self.period = self.config.get_str("fxcm", "period", "m1")  # default 1 min
                self.ccypairs = self.config.get_str("fxcm", "ccypairs", "").strip().split("\n")

    def get_conn(self):
        if self.fxcm_conn is None:
            self.fxcm_conn = fxcmpy.fxcmpy(access_token=self.access_token, log_level='debug')
        return self.fxcm_conn

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
        self.logger.info(f"crawling {pair} from {start} to {stop}")
        fxcm_conn = self.get_conn()
        df = fxcm_conn.get_candles(instrument=pair, period=self.period, start=start, stop=stop)
        df.index = pd.to_datetime(df.index)
        if df is not None and not df.empty and len(df.index) > 0:
            self.tiledb.store_kv(self.__CRAWLER_CHECKPOINT+"-"+pairname, pairname, last_timestamp + np.timedelta64(delta_second, 's'))
            self.tiledb.store_df(datatype="raw", name=pairname, df=df, sparse=True, data_df=True)
        else:
            self.logger.info(f"crawled data of {pair} from {start} to {stop} is empty")

    async def _crawl(self, queue):
        for pair in self.ccypairs:
            pair = pair.strip()
            await self._crawl_pair(pair=pair, queue=queue)

    async def _run(self, queue):
        logging.info("starting fxcm data crawler")
        while True:
            starttime = time.time()
            try:
                await self._crawl(queue=queue)
                duration = time.time() - starttime
                if (self.frequency - duration) < 0:
                    self.logger.warning("fxcm crawler is taking longer time than the configured period.")
                else:
                    self.logger.info(f"fxcm crawler takes {duration}s to finished")
                await asyncio.sleep((self.frequency - duration) if (self.frequency - duration) >= 0 else 0)
            except Exception as ex:
                self.logger.error("fxcm crawler has exception", exc_info=ex)
                try:
                    self.stop()
                except:
                    pass
                break
