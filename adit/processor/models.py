from __future__ import absolute_import

import logging
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Activation

from adit.controllers import TileDBController

__all__ = ['LSTMModel']


class LSTMModel:
    _SEED = 13

    def __init__(self, epoch=10, train_split=0.76, batch_size=250,
                 buffer_size=10000, evaluation_interval=200, validation_step=50,
                 history_size=20, future_size=0, optimizer='adam', loss='mse', activation='softmax'):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.tiledb = TileDBController.instance()

        self.epoch = epoch
        self.train_split = train_split
        self.batch_size = batch_size
        self.buffer_size = buffer_size
        self.evaluation_interval = evaluation_interval
        self.validation_step = validation_step
        self.history_size = history_size
        self.future_size = future_size
        self.optimizer = optimizer
        self.loss = loss
        self.activation = activation
        self.model = None
        self.history = None

    def univariate_data(self, dataset, start_index, end_index, history_size, target_size):
        data = []
        labels = []

        start_index = start_index + history_size
        if end_index is None:
            end_index = len(dataset) - target_size

        for i in range(start_index, end_index):
            indices = range(i - history_size, i)
            data.append(np.reshape(dataset[indices], (history_size, 1)))
            labels.append(dataset[i + target_size])
        return np.array(data), np.array(labels)

    def create_time_steps(self, length):
        return list(range(-length, 0))

    def train(self, pair, from_ts, to_ts):
        self.logger.debug(f"training model with data of ccy pair {pair} from {from_ts} to {to_ts}")
        self.pair = pair
        self.from_ts = from_ts
        self.to_ts = to_ts
        tf.random.set_seed(self._SEED)
        try:
            data_df = self.tiledb.get_ts_dataframe("health", f"{pair}_DAILY_METRICS", from_ts, to_ts)
            data_df.fillna(method='bfill', inplace=True)

            logret_ema_data = data_df['logret_ema']
            logret_ema_data.index = data_df['date']
            TRAIN_SPLIT = int(self.train_split * len(logret_ema_data.index))
            logret_ema_data = logret_ema_data.values

            train_mean = logret_ema_data[:TRAIN_SPLIT].mean()
            train_std = logret_ema_data[:TRAIN_SPLIT].std()
            logret_ema_data = (logret_ema_data - train_mean) / train_std

            x_train_uni, y_train_uni = self.univariate_data(
                logret_ema_data, 0, TRAIN_SPLIT,
                self.history_size, self.future_size)

            x_val_uni, y_val_uni = self.univariate_data(
                logret_ema_data, TRAIN_SPLIT, None,
                self.history_size, self.future_size)

            self.model = Sequential()
            self.model.add(
                LSTM(8, input_shape=x_train_uni.shape[-2:], activation=self.activation))
            #self.model.add(LSTM(32, input_shape=x_train_uni.shape[-2:], return_sequences=True, activation=self.activation))
            #self.model.add(LSTM(32, input_shape=x_train_uni.shape[-2:][::-1], return_sequences=True, activation=self.activation))
            self.model.add(Dense(1))
            self.model.add(Activation("linear"))

            self.model.compile(optimizer=self.optimizer, loss=self.loss, metrics=[self.loss])

            train_univariate = tf.data.Dataset.from_tensor_slices((x_train_uni, y_train_uni))
            train_univariate = train_univariate.cache().shuffle(self.buffer_size).batch(self.batch_size).repeat()

            self.val_univariate = tf.data.Dataset.from_tensor_slices((x_val_uni, y_val_uni))
            self.val_univariate = self.val_univariate.batch(self.batch_size).repeat()

            self.history = self.model.fit(train_univariate, epochs=self.epoch,
                                steps_per_epoch=self.evaluation_interval,
                                validation_data=self.val_univariate,
                                validation_steps=self.validation_step)

            self.logger.debug(f"finished training model with data of ccy pair {pair} from {from_ts} to {to_ts}")
        except Exception as ex:
            self.logger.error(f"failed to train model on ccy pair {pair} data from {from_ts}, {to_ts}", exc_info=ex)

    def predict(self, pair, from_ts, to_ts):
        self.logger.debug(f"predicting next data point dat ccy pair {self.pair} from {self.from_ts} to {self.to_ts}")
        try:
            data_df = self.tiledb.get_ts_dataframe("health", f"{pair}_DAILY_METRICS", from_ts, to_ts)
            data_df.fillna(method='bfill', inplace=True)

            logret_ema_data = data_df['logret_ema']
            logret_ema_data.index = data_df['date']
            logret_ema_data = logret_ema_data.values
            x_0, y_0 = self.univariate_data(logret_ema_data, 0, len(logret_ema_data), self.history_size, self.future_size)
            y_pred = self.model.predict(x_0)
            y_pred = np.concatenate((np.full((self.history_size - 1,), np.NaN), y_pred.T[0]))

            pred_df = pd.DataFrame(index=data_df.index)
            pred_df['date'] = data_df['date']
            pred_df['actual'] = logret_ema_data
            pred_df['predict'] = y_pred

            return pred_df
        except Exception as ex:
            self.logger.error(f"Failed to next data point data ccy pair {self.pair} from {self.from_ts} to {self.to_ts}", exc_info=ex)
            return None



        #for x, y in self.val_univariate.take(3):
        #plot = show_plot([x[0].numpy(), y[0].numpy(),
        #    self.model.predict(x)[0]], 0, 'Simple LSTM model')
        #plot.show()
