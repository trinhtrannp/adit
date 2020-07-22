from __future__ import absolute_import

import os
import logging
from math import pi
from bokeh.layouts import column, layout, row
from bokeh.models import (ColumnDataSource, DataRange1d, DatetimeTickFormatter, Button, Div)
from bokeh.plotting import figure
from bokeh.themes import Theme
from jinja2 import Environment, FileSystemLoader
from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (DashboardComponent)
from distributed.dashboard.utils import (without_property_validation, update)
from distributed.utils import log_errors

from adit import constants as const
from adit.ingestors import *

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "..", "themes", "default.yaml"))

__all__ = ['utilities_doc']


class UtilitiesDashboard(DashboardComponent):

    def __init__(self, worker, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initialize UtilitiesDashboard")

        self.toggle_datacrawler_btn = Button(label="Toggle Data Crawler", button_type="success")
        self.toggle_datacrawler_btn.on_click(self._toggle_datacrawler_btn_on_click)

        self.repopulate_data_btn = Button(label="Repopulate Data", button_type="success")
        self.repopulate_data_btn.on_click(self._repopulate_data_btn_on_click)

        self.datametric_cal_btn = Button(label="Calculate Data Metrics", button_type="success")
        self.datametric_cal_btn.on_click(self._datametric_cal_btn_on_click)

        self.pval_cal_btn = Button(label="Calculate p-value", button_type="success")
        self.pval_cal_btn.on_click(self._pval_cal_btn_on_click)

        if "sizing_mode" in kwargs:
            kw = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            kw = {}

        self.layout = layout([
            [
                self.toggle_datacrawler_btn,
                self.repopulate_data_btn,
                self.datametric_cal_btn,
                self.pval_cal_btn,
            ],
        ])
        # self.root = row(*plots, **kw)
        self.root = self.layout

    def _toggle_datacrawler_btn_on_click(self):
        self.logger.debug("Toggle state of data crawler")
        for crawler in FXCMCrawler.CRAWLERS_REGISTER:
            crawler.toggle_crawler()

    # TODO: implementation
    def _repopulate_data_btn_on_click(self):
        self.logger.debug("Trigger data repopulation")

    def _datametric_cal_btn_on_click(self):
        self.logger.debug("Trigger data metrics calculation")

    def _pval_cal_btn_on_click(self):
        self.logger.debug("Trigger p_value calculation")


def utilities_doc(worker, extra, doc):
    with log_errors():
        utilities = UtilitiesDashboard(worker, sizing_mode="fixed")
        doc.title = "Adit: Utilities Dashboard"
        add_periodic_callback(doc, utilities, 1000)

        doc.add_root(utilities.root)
        doc.template = env.get_template("aditdata.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME
