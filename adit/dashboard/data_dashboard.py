from __future__ import absolute_import

import os
import logging
import tiledb
import numpy as np
import pandas as pd
from adit.utils import *
from adit.controllers import TileDB
from collections import deque
from datetime import datetime, timedelta
from adit import constants as const
from bokeh.layouts import column
from bokeh.models import (ColumnDataSource, DataRange1d)
from bokeh.plotting import figure
from bokeh.themes import Theme
from jinja2 import Environment, FileSystemLoader
from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (DashboardComponent)
from distributed.dashboard.utils import (without_property_validation, update)
from distributed.utils import log_errors

from tornado.ioloop import PeriodicCallback

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "themes", "default.yaml"))


class DataMonitorCache:
    def __init__(self, maxlen=10000):
        self.last = 0
        self.tiledb = TileDB.instance()

        self.eurusd_date = deque(maxlen=maxlen)
        self.eurusd_bidclose = deque(maxlen=maxlen)
        self.eurusd_askclose = deque(maxlen=maxlen)

        self.usdjpy_date = deque(maxlen=maxlen)
        self.usdjpy_bidclose = deque(maxlen=maxlen)
        self.usdjpy_askclose = deque(maxlen=maxlen)

        self.eurjpy_date = deque(maxlen=maxlen)
        self.eurjpy_bidclose = deque(maxlen=maxlen)
        self.eurjpy_askclose = deque(maxlen=maxlen)

        self.quantities = {'eurusd_date': self.eurusd_date,
                           'usdjpy_date': self.usdjpy_date,
                           'eurjpy_date': self.eurjpy_date,
                           'eurusd_bidclose': self.eurusd_bidclose,
                           'eurusd_askclose': self.eurusd_askclose,
                           'usdjpy_bidclose': self.usdjpy_bidclose,
                           'usdjpy_askclose': self.usdjpy_askclose,
                           'eurjpy_bidclose': self.eurjpy_bidclose,
                           'eurjpy_askclose': self.eurjpy_askclose}

    def get_next(self):
        seq = [i for i in range(0, len(self.eurusd_date))]
        d = {k: [v[i] for i in seq] for k, v in self.quantities.items()}
        return d

    def update(self):
        delta_second = 600
        latest_timestamp = self.tiledb.get_kv("crawler-checkpoint-EURUSD", "EURUSD")
        to_ts = latest_timestamp
        from_ts = latest_timestamp - np.timedelta64(delta_second, 's')
        eurusd_data = self.tiledb.get_ts_dataarray("raw", "EURUSD", from_ts=from_ts, to_ts=to_ts)

        latest_timestamp = self.tiledb.get_kv("crawler-checkpoint-USDJPY", "USDJPY")
        to_ts = latest_timestamp
        from_ts = latest_timestamp - np.timedelta64(delta_second, 's')
        usdjpy_data = self.tiledb.get_ts_dataarray("raw", "USDJPY", from_ts=from_ts, to_ts=to_ts)

        latest_timestamp = self.tiledb.get_kv("crawler-checkpoint-EURJPY", "EURJPY")
        to_ts = latest_timestamp
        from_ts = latest_timestamp - np.timedelta64(delta_second, 's')
        eurjpy_data = self.tiledb.get_ts_dataarray("raw", "EURJPY", from_ts=from_ts, to_ts=to_ts)

        if eurusd_data is not None:
            self.eurusd_date.extend([(x/1e6) for x in eurusd_data['date'].tolist()])
            self.eurusd_bidclose.extend(eurusd_data['bidclose'].tolist())
            self.eurusd_askclose.extend(eurusd_data['askclose'].tolist())

        if usdjpy_data is not None:
            self.usdjpy_date.extend([(x/1e6) for x in usdjpy_data['date'].tolist()])
            self.usdjpy_bidclose.extend(usdjpy_data['bidclose'].tolist())
            self.usdjpy_askclose.extend(usdjpy_data['askclose'].tolist())

        if eurjpy_data is not None:
            self.eurjpy_date.extend([(x/1e6) for x in eurjpy_data['date'].tolist()])
            self.eurjpy_bidclose.extend(eurjpy_data['bidclose'].tolist())
            self.eurjpy_askclose.extend(eurjpy_data['askclose'].tolist())


class DataMonitorDashboard(DashboardComponent):
    def __init__(self, worker, height=150, **kwargs):
        self.cache = DataMonitorCache()
        self.cache.update()
        self.cache_cb = PeriodicCallback(self.cache.update, 2000)

        names = self.cache.quantities
        self.source = ColumnDataSource({name: [] for name in names})
        update(self.source, self.get_data())

        tools = "reset,xpan,xwheel_zoom"

        self.eurusd = figure(
            title="EUR/USD",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            x_range=DataRange1d(follow="end", follow_interval=20000, range_padding=0),
            **kwargs
        )
        self.eurusd.line(source=self.source, x="eurusd_date", y="eurusd_bidclose", color="red")
        self.eurusd.line(source=self.source, x="eurusd_date", y="eurusd_askclose", color="blue")
        self.eurusd.yaxis.axis_label = "rate"

        self.usdjpy = figure(
            title="USD/JPY",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            x_range=DataRange1d(follow="end", follow_interval=20000, range_padding=0),
            **kwargs
        )
        self.usdjpy.line(source=self.source, x="usdjpy_date", y="usdjpy_bidclose", color="red")
        self.usdjpy.line(source=self.source, x="usdjpy_date", y="usdjpy_askclose", color="blue")
        self.usdjpy.yaxis.axis_label = "rate"

        self.eurjpy = figure(
            title="EUR/JPY",
            x_axis_type="datetime",
            height=height,
            x_range=DataRange1d(follow="end", follow_interval=20000, range_padding=0),
            tools=tools,
            **kwargs
        )
        self.eurjpy.line(source=self.source, x="eurjpy_date", y="eurjpy_bidclose", color="red")
        self.eurjpy.line(source=self.source, x="eurjpy_date", y="eurjpy_askclose", color="blue")
        self.eurjpy.yaxis.axis_label = "rate"

        # self.cpu.yaxis[0].formatter = NumeralTickFormatter(format='0%')
        # self.bandwidth.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
        # self.mem.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")

        plots = [self.eurusd, self.usdjpy, self.eurjpy]

        if "sizing_mode" in kwargs:
            kw = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            kw = {}

        self.usdjpy.y_range.start = 0
        self.eurusd.y_range.start = 0
        self.eurjpy.y_range.start = 0

        self.root = column(*plots, **kw)

    def get_data(self):
        d = self.cache.get_next()
        return d

    @without_property_validation
    def update(self):
        with log_errors():
            self.source.stream(self.get_data(), 5000)


def aditdata_doc(worker, extra, doc):
    with log_errors():
        datamonitor = DataMonitorDashboard(worker, sizing_mode="stretch_both")
        doc.title = "Adit: FX Data Monitor"
        add_periodic_callback(doc, datamonitor, 5000)

        doc.add_root(datamonitor.root)
        doc.template = env.get_template("aditdata.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


def init(mode):
    import distributed.dashboard.scheduler as d_dashboard
    import distributed.dashboard.worker as d_worker
    target = d_dashboard if mode == const.SERVER_MODE else d_worker if mode == const.CLIENT_MODE else None
    if target is not None:
        target.template_variables["pages"].append("adit-data")
        target.applications["/adit-data"] = aditdata_doc
