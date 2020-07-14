from __future__ import annotations

import os
import tiledb
import numpy as np
import configparser
import logging
import logging.config
from typing import Union, Dict
import pandas as pd

from adit.config import Config
import adit.constants as const

__all__ = ['TileDB']


class TileDB:  ## example can be found in E:\python-env\msthesis\Lib\site-packages\tiledb\tests
    _INSTANCE = None

    META_BUCKET = "s3://adit-meta"
    RAW_BUCKET = "s3://adit-raw"
    CLEAN_BUCKET = "s3://adit-clean"
    POLICY_BUCKET = "s3://adit-policy"

    BUCKETS = {
        "raw": RAW_BUCKET,
        "clean": CLEAN_BUCKET,
        "policy": POLICY_BUCKET,
        "meta": META_BUCKET
    }

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = Config.instance()
        self.tiledb_conf = tiledb.Config()
        self.init_tiledb_conf()
        self.tiledb_ctx = tiledb.Ctx(config=self.config)
        self.array_conn = {}
        self.vfs = tiledb.VFS(ctx=self.tiledb_ctx, config=self.tiledb_conf)
        self.data_bucket_ready = False

    def init_tiledb_conf(self):
        self.tiledb_conf["sm.consolidation.mode"] = "fragment_meta"
        self.tiledb_conf["sm.vacuum.mode"] = "fragment_meta"
        self.tiledb_conf["sm.tile_cache_size"] = 100000000
        self.tiledb_conf["sm.num_reader_threads"] = 1
        self.tiledb_conf["sm.num_writer_threads"] = 1
        self.tiledb_conf["vfs.num_threads"] = 1
        self.tiledb_conf["vfs.s3.aws_access_key_id"] = "any"
        self.tiledb_conf["vfs.s3.aws_secret_access_key"] = "any"
        self.tiledb_conf["vfs.s3.scheme"] = "http"  # https for amazon s3
        self.tiledb_conf["vfs.s3.region"] = "us-east-1"  # us-east-1 for anazon s3
        self.tiledb_conf["vfs.s3.endpoint_override"] = f"localhost:{const.WEED_S3_PORT}"  # empty for amazon s3
        self.tiledb_conf["vfs.s3.use_virtual_addressing"] = "false"  # "true for amazon s3"
        tiledb.highlevel.initialize_ctx(config=self.tiledb_conf)

    def check_and_create_bucket(self):
        ## TODO: When it be best time to do it, for now we will check and create bucket.
        ##       Think about better solution.
        if not self.data_bucket_ready:
            for bucket_name, bucket_uri in self.BUCKETS.items():
                if not self.vfs.is_bucket(bucket_uri):
                    self.vfs.create_bucket(bucket_uri)

    def clean_data_bucket(self):
        ## TODO: Add method to clean each data bucket separately not all.
        if self.data_bucket_ready:
            for bucket_name, bucket_uri in self.BUCKETS.items():
                if self.vfs.is_bucket(bucket_uri):
                    self.vfs.remove_bucket(bucket_uri)

    def get_uri(self, datatype, name):
        if datatype not in self.BUCKETS:
            raise Exception("Bucket type does not exists")
        return self.BUCKETS[datatype] + "/" + name

    def store_kv(self, name, key, value):
        df = pd.DataFrame(data={key: [value]})
        self.store_df(self.get_uri('meta', name), df)

    def get_kv(self, name, key):
        try:
            with tiledb.open(self.get_uri('meta', name), 'r') as A:
                df = pd.DataFrame.from_dict(A[:])
                return df[key].values[0]
        except Exception as ex:
            self.logger.error(f"Cannot retrieve KV: {name} -> {key}", exc_info=ex)
            raise ex

    def store_df(self, datatype, name, df):
        uri = self.get_uri(datatype, name)
        array_existed = tiledb.highlevel.array_exists(uri)
        tiledb.from_pandas(uri, df,
                           sparse=True,
                           mode='append' if array_existed else 'ingest',
                           tile_order='row_major',
                           cell_order='row_major',
                           attrs_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000),
                           coords_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000))

    def get_ts_data(self, datatype, name, from_ts, to_ts):
        try:
            with tiledb.open(self.get_uri(datatype, name)) as B:
                df = pd.DataFrame.from_dict(B[from_ts:to_ts])
                return df
        except Exception as ex:
            self.logger.error(f"Failed to get raw data {name} from tiledb")
            raise ex

    def get_raw_data(self, name, from_ts, to_ts):
        return self.get_ts_data('raw', name, from_ts, to_ts)

    def get_clean_data(self, name, from_ts, to_ts):
        return self.get_ts_data('clean', name, from_ts, to_ts)

    def get_policy_data(self, name, from_ts, to_ts):
        return self.get_ts_data('policy', name, from_ts, to_ts)

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = TileDB()
        return cls._INSTANCE
