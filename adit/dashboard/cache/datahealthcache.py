from __future__ import absolute_import

import logging
import scipy
import numpy as np
from collections import deque
import asyncio

from adit.controllers import EventLoopController, TileDBController

__all__ = ['DataHealthCache']


class DataHealthCache:
    _INSTANCE = None
    _TASK_NAME = "data-health-cache-update"

    _DEFAULT_CCYPAIR = ['EURUSD', 'USDJPY', 'EURJPY']

    def __init__(self, pairs=None, maxlen=100000, update_delta=300):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.evl = EventLoopController.instance()
        self.evl_loop = self.evl.get_loop()

        self.tiledb = TileDBController.instance()
        if pairs is None:
            self.pairs = self._DEFAULT_CCYPAIR
        else:
            self.pairs = pairs

        self.update_delta = update_delta

        self.metadata = {
            'count': {},
            'latest_timestamp': {},
        }

        self.ts_data = {
            'date': {},
            'midclose': {},
            'logret': {},
            'logret_ema': {},
        }

        self.stats = {
            'sample_mean': {},
            'std_dev': {},
            'variance': {},
            'skew': {},
            'kurtosis': {},
            'std_err': {},
            't_stats': {},
            'ks_stats': {},
            'jarque_bera_stats': {},
            'sapiro_stats': {},
        }

    async def update(self, queue):
        self.logger.info("updating current data health cache")

    def start_periodic_update(self):
        self.evl.shedule_task(self._TASK_NAME, self.update)

    def stop_periodic_update(self):
        self.evl.stop_task(self._TASK_NAME)

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = DataHealthCache()
        return cls._INSTANCE