from __future__ import absolute_import

import os
import logging
import math
from math import pi
from bokeh.layouts import layout, column, row
from bokeh.models import (ColumnDataSource, DataRange1d, DatetimeTickFormatter, Button, DatePicker, DataTable, TableColumn, DateFormatter)
from bokeh.themes import Theme
from bokeh.plotting import figure
from jinja2 import Environment, FileSystemLoader
from distributed.dashboard.components.shared import (DashboardComponent)
from distributed.dashboard.utils import (without_property_validation, update)
from distributed.utils import log_errors
from datetime import datetime, timedelta, date
import numpy as np
from hurst import compute_Hc
from scipy import stats #ttest_1samp, ks_1samp, jarque_bera, shapiro, describe


from adit.processor import MetricsCalculator
from adit.controllers import EventLoopController, TPOOL, TileDBController
from adit.dashboard.cache import DataHealthCache

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "..", "themes", "default.yaml"))

__all__ = ['datahealth_doc']


class DataHealthDashboard(DashboardComponent):
    _DEFAULT_PAIRS = ['EURUSD', 'USDJPY', 'EURJPY']

    def __init__(self, worker, height=200, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initialize DataHealthDashboard")

        self.pairs = self._DEFAULT_PAIRS
        self.cache = DataHealthCache.instance()

        self.metric_calulator = MetricsCalculator.instante()
        self.evl = EventLoopController.instance()
        self.evl_loop = self.evl.get_loop()
        self.tiledb = TileDBController.instance()

        self.datametric_cal_btn = Button(label="Calculate Data Metrics", button_type="success", height=50)
        self.datametric_cal_btn.on_click(self._datametric_cal_btn_on_click)

        self.refresh_btn = Button(label="Refresh Charts/Table", button_type="success", height=50)
        self.refresh_btn.on_click(self._update_data_source)

        self.from_datepk = DatePicker(title="From Date", min_date=date(2000, 1, 1),
                                      max_date=(datetime.now() + timedelta(days=7)).date(),
                                      value=(datetime.now() - timedelta(days=30)).date(), height=50)

        self.to_datepk = DatePicker(title="To Date", min_date=date(2000, 1, 1),
                                    max_date=(datetime.now() + timedelta(days=7)).date(),
                                    value=date.today(), height=50)

        self.table_source = ColumnDataSource({
            'ccypair': [pair for pair in self.pairs],
            'sample_mean': ['NaN', 'NaN', 'NaN'],
            'std_dev': ['NaN', 'NaN', 'NaN'],
            'skew': ['NaN', 'NaN', 'NaN'],
            'variance': ['NaN', 'NaN', 'NaN'],
            'kurtosis': ['NaN', 'NaN', 'NaN'],
            't_test': ['NaN', 'NaN', 'NaN'],
            'hurst': ['NaN', 'NaN', 'NaN']
        })

        self.stats_table = DataTable(source=self.table_source, width=1305, height=100,
        columns=[
            TableColumn(field="ccypair", title="Currency Pair", width=65),
            TableColumn(field="sample_mean", title="Sample Mean", width=145),
            TableColumn(field="std_dev", title="Std. Deviation", width=135),
            TableColumn(field="variance", title="Variance", width=135),
            TableColumn(field="skew", title="Skew", width=120),
            TableColumn(field="kurtosis", title="Kurtosis", width=120),
            TableColumn(field="t_test", title="T-Test", width=320),
            TableColumn(field="hurst", title="Hurst Exponent", width=265),
        ])
        self.stats_table.index_width = 1

        ts_data_names = self.cache.ts_data
        self.ts_source = {}
        self.figure = {}
        plots = []
        for pair in self.pairs:
            self.ts_source[pair] = ColumnDataSource({name: [] for name in ts_data_names})
            pair_figure = figure(
                title=pair+" metrics",
                x_axis_type="datetime",
                height=350,
                x_range=DataRange1d(follow="end", follow_interval=99999999999, range_padding=0),
                y_range=DataRange1d(follow="end", follow_interval=1, range_padding=0.15),
                output_backend="webgl",
                sizing_mode="stretch_width",
            )
            pair_figure.line(source=self.ts_source[pair], x="date", y="logret", color="blue", legend_label='log_ret')
            pair_figure.line(source=self.ts_source[pair], x="date", y="logret_ema", color="green", legend_label='ema')
            pair_figure.legend.location = "bottom_right"
            pair_figure.legend.click_policy = "hide"
            pair_figure.legend.background_fill_alpha = 0.0
            pair_figure.yaxis.axis_label = "Log Return"
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

        kw = {"sizing_mode": "stretch_width"}

        self.layout = layout([
            row(*[self.refresh_btn, self.from_datepk, self.to_datepk, self.datametric_cal_btn], **kw),
            self.stats_table,
            column(*plots, **kw)
        ], sizing_mode='stretch_both')
        # self.root = row(*plots, **kw)
        self.root = self.layout

    def _datametric_cal_btn_on_click(self):
        self.logger.debug(f"Trigger data metrics calculation from {self.from_datepk.value} to {self.to_datepk.value}")
        from_ts = np.datetime64(self.from_datepk.value)
        to_ts = np.datetime64(self.to_datepk.value)
        self.metric_calulator.cal(from_ts, to_ts)

    def _update_data_source(self):
        self.logger.info(f"update data source with data from {self.from_datepk.value} to {self.to_datepk.value}")
        try:
            from_ts = np.datetime64(self.from_datepk.value)
            to_ts = np.datetime64(self.to_datepk.value)

            stats_data = {
                'ccypair': [pair for pair in self.pairs],
                'sample_mean': [],
                'variance': [],
                'std_dev': [],
                'skew': [],
                'kurtosis': [],
                't_test': [],
                'hurst': [],
            }

            for pair in self.pairs:
                self.logger.debug(f"Calculate data health metrics for pair{pair}")

                data = self.tiledb.get_ts_dataframe("health", f"{pair}_DAILY_METRICS", from_ts, to_ts)
                data.fillna(method='bfill', axis=0, inplace=True)
                if data is not None and len(data.index) > 0:
                    new_data = {col: data[col].tolist() for col in data.columns}
                    update(self.ts_source[pair], new_data)
                    logret_ema = data['logret_ema'].to_numpy()
                    stats_metrics = stats.describe(logret_ema, nan_policy='omit')
                    min, max = stats_metrics.minmax
                    t_test = stats.ttest_1samp(logret_ema, popmean=0.0, nan_policy='omit')
                    if len(data.index) > 100:
                        logret_ema += (0 if min > 0 else ((-min)+1.0))
                        H, c, data = compute_Hc(logret_ema, kind='price', simplified=True)
                        stats_data['hurst'].append(f"H={H}, c={c}")
                    else:
                        stats_data['hurst'].append(f"data too small")

                    stats_data['sample_mean'].append(f"{stats_metrics.mean}")
                    stats_data['variance'].append(f"{stats_metrics.variance}")
                    stats_data['std_dev'].append(f"{math.sqrt(stats_metrics.variance)}")
                    stats_data['skew'].append(f"{stats_metrics.skewness}")
                    stats_data['kurtosis'].append(f"{stats_metrics.kurtosis}")
                    stats_data['t_test'].append(f"t-stats={t_test.statistic}, p-value={(t_test.pvalue/2.0)}")

            update(self.table_source, stats_data)
            self.logger.debug("Finished update health data source")
        except Exception as ex:
            self.logger.error("Failed to update data source", exc_info=ex)

    @without_property_validation
    def update(self):
        with log_errors():
            self.logger.info("update datahealth dashboard data source")
            self._update_data_source()


def datahealth_doc(worker, extra, doc):
    with log_errors():
        datamonitor = DataHealthDashboard(worker, sizing_mode="fixed")
        doc.title = "Adit: FX Data Heath"
        #add_periodic_callback(doc, datamonitor, 1000)

        doc.add_root(datamonitor.root)
        doc.template = env.get_template("aditdata.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME
