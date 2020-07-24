from __future__ import absolute_import

import logging
import pandas as pd
import asyncio

from adit.controllers import TileDBController, EventLoopController, TPOOL

__all__ = ['MetricsCalculator']


class MetricsCalculator:
    _CCY_PAIRS = ['EURUSD', 'USDJPY', 'EURJPY']
    TASK_NAME = "data-metric-cal"
    _INSTANCE = None

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tiledb = TileDBController.instance()
        self.evl = EventLoopController.instance()
        self.evl_loop = self.evl.get_loop()
        self.frequency = 86400 # 1 day

    def _cal_metrics(self, pair, from_ts, to_ts):
        self.logger.debug(f"getting data of pair {pair} to calculate midclose rate")
        df = self.tiledb.get_ts_dataframe('raw', pair, from_ts, to_ts)

        self.logger.debug(f"resample data of pair {pair} to daily and drop NaN data row")
        df = df.set_index('date').resample('D').last().dropna(axis=0, how='all')

        if len(df.index) < 2:
            self.logger.debug(f"data is not enough to perform metrics calculation, at least 2 day worth of data")
            return

        self.logger.debug(f"calculate midclose rate data of pair {pair} and drop unnecessary data")
        df['midclose'] = (df['bidclose'].abs() + df['askclose'].abs()) / 2
        df = df.drop(columns=['bidopen', 'bidclose', 'bidhigh', 'bidlow', 'askopen', 'askclose', 'askhigh', 'asklow', 'tickqty'])

        self.logger.debug(f"calculate log return of midclose of pair {pair}")
        logret_df = df.pct_change().rename(columns={"midclose": "logret"})

        self.logger.debug(f"calculate exponential moving average of log return of pair {pair}")
        ema_df = logret_df.ewm(alpha=0.5, adjust=True, ignore_na=True, min_periods=5).mean().rename(columns={"logret": "logret_ema"})

        df = pd.merge(df, logret_df, how='inner', left_index=True, right_index=True)
        df = pd.merge(df, ema_df, how='inner', left_index=True, right_index=True)
        self.logger.debug(f"store daily metrics of {pair} to tiledb")
        self.tiledb.store_df("health", f"{pair}_DAILY_METRICS", df)

    def cal_metrics(self, pair, from_ts, to_ts):
        self.logger.debug(f"awaiting for metric fcalculation rom {from_ts} to {to_ts} for pair {pair}")
        self._cal_metrics(pair, from_ts, to_ts)

    def cal(self, from_ts=None, to_ts=None):
        for pair in self._CCY_PAIRS:
            self.logger.debug(f"calculate metric from {from_ts} to {to_ts} for pair {pair}")
            data_domain = self.tiledb.get_data_domain('raw', pair)
            if not from_ts:
                from_ts = data_domain[0]

            if not to_ts:
                to_ts = data_domain[1]

            self.cal_metrics(pair, from_ts, to_ts)

    async def cal_async(self):
        for pair in self._CCY_PAIRS:
            from_ts = self.tiledb.get_data_domain("health", f"{pair}_DAILY_METRICS")[1]
            to_ts = self.tiledb.get_data_domain("raw", pair)[1]
            if to_ts > from_ts:
                self.logger.debug(f"calculate data metrics from {from_ts} to {to_ts}")
                await self.evl_loop.run_in_executor(TPOOL, self.cal_metrics, pair, from_ts, to_ts)
            else:
                self.logger.debug(f"data metrics is already up to date")

    def start(self):
        self.evl.shedule_task(self.TASK_NAME, self._run)

    def stop(self) -> None:
        self.evl.stop_task(self.TASK_NAME)

    async def _run(self, queue):
        self.logger.info("starting fxcm data crawler")
        while True:
            try:
                await self.cal_async()
                await asyncio.sleep(self.frequency)
            except Exception as ex:
                self.logger.error("fxcm crawler has exception", exc_info=ex)
                try:
                    self.stop()
                except:
                    pass
                break

    @classmethod
    def instante(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = MetricsCalculator()
        return cls._INSTANCE



