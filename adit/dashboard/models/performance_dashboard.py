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

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "..", "themes", "default.yaml"))

__all__ = ['modelperformance_doc']


class ModelPerformanceDashboard(DashboardComponent):

    def __init__(self, worker, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initialize ModelPerformanceDashboard")

        plots = []

        if "sizing_mode" in kwargs:
            kw = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            kw = {}

        self.root = column(*plots, **kw)


def modelperformance_doc(worker, extra, doc):
    with log_errors():
        modelperformance = ModelPerformanceDashboard(worker, sizing_mode="stretch_both")
        doc.title = "Adit: Model Performance Dashboard"
        add_periodic_callback(doc, modelperformance, 1000)

        doc.add_root(modelperformance.root)
        doc.template = env.get_template("aditdata.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME
