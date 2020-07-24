from __future__ import absolute_import

import os
import logging
from math import pi
from bokeh.layouts import layout, column, row
from bokeh.models import (ColumnDataSource, DataRange1d, DatetimeTickFormatter, Button, DatePicker, Select)
from bokeh.themes import Theme
from bokeh.plotting import figure
from jinja2 import Environment, FileSystemLoader
from distributed.dashboard.components.shared import (DashboardComponent)
from distributed.dashboard.utils import (without_property_validation, update)
from distributed.utils import log_errors
from datetime import datetime, timedelta, date
import numpy as np


from adit.processor import LSTMModel
from adit.controllers import EventLoopController, TileDBController
from adit.dashboard.cache import ModelPerformanceCache

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "templates")
    )
)

logger = logging.getLogger(__name__)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "..", "themes", "default.yaml"))

__all__ = ['modelperformance_doc']


class ModelPerformanceDashboard(DashboardComponent):
    _DEFAULT_PAIRS = ['EURUSD', 'USDJPY', 'EURJPY']

    def __init__(self, worker, height=200, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initialize ModelPerformanceDashboard")

        self.pairs = self._DEFAULT_PAIRS
        self.cache = ModelPerformanceCache.instance()

        self.loss_function = "mse"
        self.model = LSTMModel(loss=self.loss_function)
        self.evl = EventLoopController.instance()
        self.evl_loop = self.evl.get_loop()
        self.tiledb = TileDBController.instance()

        self.pair_select = Select(title="CCY Pair:", value=self.pairs[0], options=self.pairs, sizing_mode="stretch_width", height=50)

        self.train_btn = Button(label="Train/Test Model", button_type="success", height=50)
        self.train_btn.on_click(self._train_btn_on_click)

        self.train_from_datepk = DatePicker(title="From Date", min_date=date(2000, 1, 1),
                                      max_date=(datetime.now() + timedelta(days=7)).date(),
                                      value=(datetime.now() - timedelta(days=30)).date(), height=50)

        self.train_to_datepk = DatePicker(title="To Date", min_date=date(2000, 1, 1),
                                    max_date=(datetime.now() + timedelta(days=7)).date(),
                                    value=date.today(), height=50)

        self.predict_btn = Button(label="Predict", button_type="success", height=50)
        self.predict_btn.on_click(self._predict_btn_on_click)

        self.predict_from_datepk = DatePicker(title="From Date", min_date=date(2000, 1, 1),
                                            max_date=(datetime.now() + timedelta(days=7)).date(),
                                            value=(datetime.now() - timedelta(days=30)).date(), height=50)

        self.predict_to_datepk = DatePicker(title="To Date", min_date=date(2000, 1, 1),
                                          max_date=(datetime.now() + timedelta(days=7)).date(),
                                          value=date.today(), height=50)
        self.train_source = ColumnDataSource({name: [] for name in ['epoch', 'acc_train', 'acc_test', 'loss_train', 'loss_test']})
        self.predict_source = ColumnDataSource({name: [] for name in ['date', 'actual', 'predict']})

        self.acc_figure = figure(
            title="Accuracy",
            height=400,
            output_backend="webgl",
            sizing_mode="stretch_width",
        )
        self.acc_figure.line(source=self.train_source, x="epoch", y="acc_train", color="blue", legend_label='train', line_width=5)
        self.acc_figure.line(source=self.train_source, x="epoch", y="acc_test", color="green", legend_label='test', line_width=5)
        self.acc_figure.legend.location = "bottom_right"
        self.acc_figure.legend.click_policy = "hide"
        self.acc_figure.legend.background_fill_alpha = 0.0
        self.acc_figure.yaxis.axis_label = "accuracy"
        self.acc_figure.xaxis.axis_label = "Epoch"

        self.loss_figure = figure(
            title="Loss",
            height=400,
            output_backend="webgl",
            sizing_mode="stretch_width",
        )
        self.loss_figure.line(source=self.train_source, x="epoch", y="loss_train", color="blue", legend_label='train', line_width=5)
        self.loss_figure.line(source=self.train_source, x="epoch", y="loss_test", color="green", legend_label='test', line_width=5)
        self.loss_figure.legend.location = "bottom_right"
        self.loss_figure.legend.click_policy = "hide"
        self.loss_figure.legend.background_fill_alpha = 0.0
        self.loss_figure.yaxis.axis_label = self.loss_function.upper()
        self.loss_figure.xaxis.axis_label = "Epoch"

        self.pred_figure = figure(
            title="Predict/Actual",
            x_axis_type="datetime",
            height=400,
            x_range=DataRange1d(follow="end", follow_interval=99999999999, range_padding=0),
            y_range=DataRange1d(follow="end", follow_interval=1, range_padding=0.15),
            output_backend="webgl",
            sizing_mode="stretch_width",
        )
        self.pred_figure.line(source=self.predict_source, x="date", y="actual", color="blue", legend_label='actual', line_width=5)
        self.pred_figure.line(source=self.predict_source, x="date", y="predict", color="green", legend_label='predict', line_width=5)
        self.pred_figure.legend.location = "bottom_right"
        self.pred_figure.legend.click_policy = "hide"
        self.pred_figure.legend.background_fill_alpha = 0.0
        self.pred_figure.yaxis.axis_label = "EMA Log Return"
        self.pred_figure.xaxis.axis_label = "Time"
        self.pred_figure.xaxis.major_label_orientation = pi / 4
        self.pred_figure.xaxis.formatter = DatetimeTickFormatter(
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

        kw = {"sizing_mode": "stretch_width"}

        self.layout = layout([
            self.pair_select,
            row(*[self.train_btn, self.train_from_datepk, self.train_to_datepk], **kw),
            row(*[self.predict_btn, self.predict_from_datepk, self.predict_to_datepk], **kw),
            column(*[self.loss_figure, self.pred_figure], **kw)
        ], sizing_mode='stretch_both')

        self.root = self.layout

    def _train_btn_on_click(self):
        self.logger.debug(f"Train LSTM model with train ccy pair {self.pair_select.value} data from {self.train_from_datepk.value} to {self.train_to_datepk.value}")
        selected_pair = self.pair_select.value
        from_ts = np.datetime64(self.train_from_datepk.value)
        to_ts = np.datetime64(self.train_to_datepk.value)
        self.model.train(selected_pair, from_ts, to_ts)
        train_history = self.model.history
        loss_train = train_history.history['loss']
        loss_test = train_history.history['val_loss']
        acc_train = train_history.history[f"{self.loss_function}"]
        acc_test = train_history.history[f"val_{self.loss_function}"]
        epoch = [i for i in range(0, len(loss_train))]
        new_train_metrics = {
            'epoch': epoch,
            'acc_train': acc_train,
            'acc_test': acc_test,
            'loss_train': loss_train,
            'loss_test': loss_test
        }
        update(self.train_source, new_train_metrics)

    def _predict_btn_on_click(self):
        self.logger.info(f"using trained model to predict with ccy pair {self.pair_select.value} data from {self.predict_from_datepk.value} to {self.predict_to_datepk.value}")
        selected_pair = self.pair_select.value
        from_ts = np.datetime64(self.predict_from_datepk.value)
        to_ts = np.datetime64(self.predict_to_datepk.value)
        next_points_df = self.model.predict(selected_pair, from_ts, to_ts)
        if next_points_df is not None:
            new_points_data = {
                'date': next_points_df['date'].tolist(),
                'actual': next_points_df['actual'].tolist(),
                'predict': next_points_df['predict'].tolist(),
            }
            update(self.predict_source, new_points_data)

    @without_property_validation
    def update(self):
        with log_errors():
            self.logger.info("update dashboard data source")


def modelperformance_doc(worker, extra, doc):
    with log_errors():
        datamonitor = ModelPerformanceDashboard(worker, sizing_mode="fixed")
        doc.title = "Adit: Model Performance"
        #add_periodic_callback(doc, datamonitor, 1000)

        doc.add_root(datamonitor.root)
        doc.template = env.get_template("aditdata.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME
