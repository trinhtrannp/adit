from __future__ import absolute_import

import os
import logging
from math import pi
from adit import constants as const
from bokeh.layouts import column
from bokeh.models import (ColumnDataSource, DataRange1d, DatetimeTickFormatter)
from bokeh.plotting import figure
from bokeh.themes import Theme
from jinja2 import Environment, FileSystemLoader
from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (DashboardComponent)
from distributed.dashboard.utils import (without_property_validation, update)
from distributed.utils import log_errors

from adit.dashboard.cache import DataMonitorCache

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "..", "themes", "default.yaml"))

__all__ = ['DataFeedDashboard', 'datafeed_doc']


class DataFeedDashboard(DashboardComponent):
    _DEFAULT_PAIRS = ['EURUSD', 'USDJPY', 'EURJPY']

    def __init__(self, worker, height=200, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initialize DataMonitorDashboard")

        self.cache = DataMonitorCache.instance()
        names = self.cache.data
        # TODO: configurable pairs
        self.pairs = self._DEFAULT_PAIRS
        self.last = {}
        self.source = {}
        self.figure = {}
        plots = []
        for pair in self.pairs:
            self.last[pair] = 0
            self.source[pair] = ColumnDataSource({name: [] for name in names})
            update(self.source[pair], self.get_data(pair))
            pair_figure = figure(
                title=pair,
                x_axis_type="datetime",
                height=height,
                x_range=DataRange1d(follow="end", follow_interval=20000000, range_padding=0),
                y_range=DataRange1d(follow="end", follow_interval=1, range_padding=0.15),
                output_backend="webgl",
                **kwargs
            )
            pair_figure.line(source=self.source[pair], x="date", y="bidclose", color="red", legend_label='bid close')
            pair_figure.line(source=self.source[pair], x="date", y="askclose", color="blue", legend_label='ask close')
            pair_figure.legend.location = "bottom_right"
            pair_figure.legend.click_policy = "hide"
            pair_figure.yaxis.axis_label = "Exchange Rate"
            pair_figure.xaxis.axis_label = "Time"
            pair_figure.xaxis.major_label_orientation = pi / 4
            pair_figure.xaxis.formatter = DatetimeTickFormatter(
                microseconds=['%fus'],
                milliseconds=['%3Nms', '%S.%3Ns'],
                seconds=['%H:%M:%S'],
                minsec=['%H:%M:%S'],
                minutes=['%d/%m/%y %H:%M:%S'],
                hourmin=['%d/%m/%y %H:%M:%S'],
                hours=['%d/%m/%y %H:%M:%S'],
                days=['%d/%m/%y %H:%M:%S'],
                months=['%d/%m/%y %H:%M:%S'],
                years=['%d/%m/%y %H:%M:%S'],
            )
            self.figure[pair] = pair_figure
            plots.append(self.figure[pair])

        if "sizing_mode" in kwargs:
            kw = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            kw = {}

        self.root = column(*plots, **kw)

    def get_data(self, pair):
        d = self.cache.get_next(pair, start=self.last[pair])
        self.last[pair] = self.cache.metadata['count'][pair]
        self.logger.debug(f"Got new data from cache pair={pair}")
        return d

    @without_property_validation
    def update(self):
        with log_errors():
            for pair in self.pairs:
                self.source[pair].stream(self.get_data(pair), 240)


def datafeed_doc(worker, extra, doc):
    with log_errors():
        datamonitor = DataFeedDashboard(worker, sizing_mode="stretch_both")
        doc.title = "Adit: FX Data Feed"
        add_periodic_callback(doc, datamonitor, 1000)

        doc.add_root(datamonitor.root)
        doc.template = env.get_template("aditdata.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


# TODO: DEPRECATE this method
def init(mode):
    import distributed.dashboard.scheduler as d_dashboard
    import distributed.dashboard.worker as d_worker
    target = d_dashboard if mode == const.SERVER_MODE else d_worker if mode == const.CLIENT_MODE else None
    if target is not None:
        target.template_variables["pages"].append("adit-datafeed")
        target.applications["/adit-datafeed"] = datafeed_doc
