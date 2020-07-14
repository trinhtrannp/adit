from __future__ import absolute_import

import os
import logging
import pandas as pd
from adit.utils import *
from adit import constants as const

import tiledb

from bokeh.layouts import column
from bokeh.models import (
    ColumnDataSource,
    DataRange1d,
    NumeralTickFormatter,
)
from bokeh.plotting import figure
from bokeh.themes import Theme

from jinja2 import Environment, FileSystemLoader

try:
    import numpy as np
except ImportError:
    np = False

from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (DashboardComponent)
from distributed.dashboard.utils import (without_property_validation, update)

from distributed.utils import log_errors

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "themes", "default.yaml"))


class DataMonitor(DashboardComponent):
    def __init__(self, worker, height=150, **kwargs):
        self.worker = worker

        names = self.worker.monitor.quantities
        self.last = 0
        self.source = ColumnDataSource({name: [] for name in names})
        update(self.source, self.get_data())

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        tools = "reset,xpan,xwheel_zoom"

        self.cpu = figure(
            title="CPU",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            x_range=x_range,
            **kwargs
        )
        self.cpu.line(source=self.source, x="time", y="cpu")
        self.cpu.yaxis.axis_label = "Percentage"
        self.mem = figure(
            title="Memory",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            x_range=x_range,
            **kwargs
        )
        self.mem.line(source=self.source, x="time", y="memory")
        self.mem.yaxis.axis_label = "Bytes"
        self.bandwidth = figure(
            title="Bandwidth",
            x_axis_type="datetime",
            height=height,
            x_range=x_range,
            tools=tools,
            **kwargs
        )
        self.bandwidth.line(source=self.source, x="time", y="read_bytes", color="red")
        self.bandwidth.line(source=self.source, x="time", y="write_bytes", color="blue")
        self.bandwidth.yaxis.axis_label = "Bytes / second"

        # self.cpu.yaxis[0].formatter = NumeralTickFormatter(format='0%')
        self.bandwidth.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
        self.mem.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")

        plots = [self.cpu, self.mem, self.bandwidth]

        if not is_windows():
            self.num_fds = figure(
                title="Number of File Descriptors",
                x_axis_type="datetime",
                height=height,
                x_range=x_range,
                tools=tools,
                **kwargs
            )

            self.num_fds.line(source=self.source, x="time", y="num_fds")
            plots.append(self.num_fds)

        if "sizing_mode" in kwargs:
            kw = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            kw = {}

        if not is_windows():
            self.num_fds.y_range.start = 0
        self.mem.y_range.start = 0
        self.cpu.y_range.start = 0
        self.bandwidth.y_range.start = 0

        self.root = column(*plots, **kw)
        self.worker.monitor.update()

    def get_data(self):
        d = self.worker.monitor.range_query(start=self.last)
        d["time"] = [x * 1000 for x in d["time"]]
        self.last = self.worker.monitor.count
        return d

    @without_property_validation
    def update(self):
        with log_errors():
            self.source.stream(self.get_data(), 1000)


def aditdata_doc(worker, extra, doc):
    with log_errors():
        datamonitor = DataMonitor(worker, sizing_mode="stretch_both")
        doc.title = "Adit: FX Data Monitor"
        add_periodic_callback(doc, datamonitor, 500)

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
