from __future__ import absolute_import

import logging
import numpy as np
from adit.controllers import TileDBController, EventLoopController, TPOOL
from collections import deque
import asyncio

__all__ = ['DataMonitorCache']


class DataMonitorCache:
    _INSTANCE = None

    _DEFAULT_CCYPAIR = ['EURUSD', 'USDJPY', 'EURJPY']

    def __init__(self, pairs=None, maxlen=100000, update_delta=300):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Initializing DataMonitorCache")

        if pairs is None:
            self.pairs = self._DEFAULT_CCYPAIR
        else:
            self.pairs = pairs
        self.update_delta = update_delta
        self.tiledb = TileDBController.instance()
        self.evl = EventLoopController.instance()
        self.evl_loop = self.evl.get_loop()

        self.data = {
            'date': {},
            'bidclose': {},
            'askclose': {},
        }
        self.metadata = {
            'count': {},
            'latest_timestamp': {},
        }

        for pair in self.pairs:
            self.metadata['count'][pair] = 0
            self.metadata['latest_timestamp'][pair] = None

            self.data['date'][pair] = deque(maxlen=maxlen)
            self.data['bidclose'][pair] = deque(maxlen=maxlen)
            self.data['askclose'][pair] = deque(maxlen=maxlen)

    def get_checkpoint(self, pair):
        return self.tiledb.get_kv(f"crawler-checkpoint-{pair}", pair)

    def get_ts_data(self, pair, from_ts, to_ts):
        return self.tiledb.get_ts_dataarray("raw", pair, from_ts=from_ts, to_ts=to_ts)

    async def _update_pair(self, pair):
        self.logger.debug(f"update cache of ccy pair {pair}")
        latest_timestamp = await self.evl_loop.run_in_executor(TPOOL, self.get_checkpoint, pair)
        if latest_timestamp != self.metadata['latest_timestamp'][pair]:
            self.metadata['latest_timestamp'][pair] = latest_timestamp
            to_ts = latest_timestamp
            from_ts = latest_timestamp - np.timedelta64(self.update_delta, 's')
            data = await self.evl_loop.run_in_executor(TPOOL, self.get_ts_data, pair, from_ts, to_ts)
            self.metadata['count'][pair] += len(data['date'])
            self.logger.debug(f"current data cache for pair {pair} is updated with data from={from_ts} to={to_ts} with length={len(data['date'])}")
        else:
            data = None
            self.logger.debug(f"no new data of pair {pair} to update the cache.")

        if data is not None:
            self.data['date'][pair].extend([(x / 1e6) for x in data['date'].tolist()])
            self.data['bidclose'][pair].extend(data['bidclose'].tolist())
            self.data['askclose'][pair].extend(data['askclose'].tolist())

    async def update(self, queue):
        self.logger.info("updating current data cache")
        DELAY = 2 # 2 seconds
        while True:
            try:
                for pair in self.pairs:
                    await self._update_pair(pair)
                await asyncio.sleep(DELAY)
            except Exception as ex:
                logging.info("An error occured while updateing data cache", exc_info=ex)
                break

    def start_periodic_update(self):
        self.evl.shedule_task('data-cache-update', self.update)

    def stop_periodic_update(self):
        self.evl.stop_task('data-cache-update')

    def get_next(self, pair, start):
        self.logger.debug(f"get next data points from cache {pair}")
        if start == self.metadata['count'][pair]:
            return {k: [] for k in self.data}

        istart = start - (self.metadata['count'][pair] - len(self.data['date'][pair]))
        istart = max(0, istart)

        seq = [i for i in range(istart, len(self.data['date'][pair]))]
        d = {k: [v[pair][i] for i in seq] for k, v in self.data.items()}
        return d

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = DataMonitorCache()
        return cls._INSTANCE