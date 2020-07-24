from __future__ import absolute_import

import os
import logging
from bokeh.layouts import layout
from bokeh.models import (ColumnDataSource, DataRange1d, DatetimeTickFormatter, Button)
from bokeh.plotting import figure
from bokeh.themes import Theme
from jinja2 import Environment, FileSystemLoader
from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (DashboardComponent)
from distributed.dashboard.utils import (without_property_validation, update)
from distributed.utils import log_errors

from adit.ingestors import FXCMCrawler

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "..", "themes", "default.yaml"))

__all__ = ['status_doc']


class StatusDashboard(DashboardComponent):

    def __init__(self, worker, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initialize StatusDashboard")

        self.repopulate_data_btn = Button(label="Repopulate Data", button_type="primary")
        self.repopulate_data_btn.on_click(self._repopulate_data_btn_on_click)

        self.toggle_datacrawler_btn = Button(label="Toggle Data Crawler", button_type="primary")
        self.toggle_datacrawler_btn.on_click(self._toggle_datacrawler_btn_on_click)

        if "sizing_mode" in kwargs:
            kw = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            kw = {}

        self.layout = layout([
            [self.repopulate_data_btn, self.toggle_datacrawler_btn],
        ])
        self.root = self.layout

    def _repopulate_data_btn_on_click(self):
        self.logger.debug("Repopulate data from data server")

    def _toggle_datacrawler_btn_on_click(self):
        self.logger.debug("Toggle state of data crawler")
        crawler = FXCMCrawler.CRAWLERS_REGISTER[0]
        crawler.toggle_crawler()
        self.toggle_datacrawler_btn.label = "PAUSE" if not crawler.paused else "UN-PAUSE"
        self.toggle_datacrawler_btn.button_type = "dander" if not crawler.paused else "primary"

    @without_property_validation
    def update(self):
        with log_errors():
            self.logger.info("update status dashboard")
            crawler = FXCMCrawler.CRAWLERS_REGISTER[0]
            self.toggle_datacrawler_btn.label = "PAUSE" if not crawler.paused else "UN-PAUSE"
            self.toggle_datacrawler_btn.button_type = "dander" if not crawler.paused else "primary"


def status_doc(worker, extra, doc):
    with log_errors():
        statusdb = StatusDashboard(worker, sizing_mode="stretch_both")
        doc.title = "Adit: Status Dashboard"
        add_periodic_callback(doc, statusdb, 1000)

        doc.add_root(statusdb.root)
        doc.template = env.get_template("aditdata.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME
