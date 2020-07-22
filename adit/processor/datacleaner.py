from __future__ import absolute_import

import logging
import numpy as np
import pandas as pd

from adit.controllers import TileDBController, EventLoopController


class DataCleaner:
    _CCY_PAIRS = ['EURUSD', 'USDJPY', 'EURJPY']
    _INSTANCE = None

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tiledb = TileDBController.instance()
        self.evl = EventLoopController.instance()
        self.io_loop = self.evl.get_loop()

    def _cal_mid_rate(self, start, pair):
        start_timestamp = start
        first_ddf = None
        while True:
            stop = start + np.timedelta64(365, 'D')
            data = self.tiledb.get_ts_dataarray('')
            df = pd.DataFrame.from_dict(data)[['bidclose', 'askclose', 'date']]
            df = df.set_index('date')
            print(len(df))
            ddf = dd.from_pandas(df, npartitions=4)
            if first_ddf is None:
                first_ddf = ddf
            else:
                first_ddf = dd.concat([first_ddf, ddf], interleave_partitions=True)

            del data
            del df
            del ddf
            gc.collect()
            start = stop
            first_ddf.compute()
            print(first_ddf.tail())

            if stop > np.datetime64('now'):
                break

        first_ddf['midopen'] = (first_ddf['askopen'].abs() + first_ddf['bidopen'].abs()) / 2
        first_ddf['midlow'] = (first_ddf['asklow'].abs() + first_ddf['bidlow'].abs()) / 2
        first_ddf['midhigh'] = (first_ddf['askhigh'].abs() + first_ddf['bidhigh'].abs()) / 2
        first_ddf['midclose'] = (first_ddf['askclose'].abs() + first_ddf['bidclose'].abs()) / 2

        first_ddf.compute()
        print(first_ddf.tail())

    def cal(self):
        for pair in self._CCY_PAIRS:
            start = self.tiledb.get_data_domain('raw', pair)[0]
            self._cal_mid_rate(start)


    @classmethod
    def instante(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = DataCleaner()
        return cls._INSTANCE



