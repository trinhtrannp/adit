from __future__ import annotations

import tiledb
import logging
import logging.config
import pandas as pd
import numpy as np

from adit.config import Config
import adit.constants as const
from adit.utils import *

__all__ = ['TileDBController']


class TileDBController:
    _INSTANCE = None

    META_BUCKET = "s3://adit-meta"
    RAW_BUCKET = "s3://adit-raw"
    HEALTH_BUCKET = "s3://adit-health"

    _RAW_DATA = "raw"
    _HEALTH_DATA = "health"
    _META_DATA = "meta"

    BUCKETS = {
        _RAW_DATA: RAW_BUCKET,
        _HEALTH_DATA: HEALTH_BUCKET,
        _META_DATA: META_BUCKET
    }

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = Config.instance()
        self.tiledb_conf = tiledb.Config()
        self.init_tiledb_conf()
        self.tiledb_ctx = tiledb.Ctx(config=self.tiledb_conf)
        self.array_conn = {}
        self.vfs = tiledb.VFS(ctx=self.tiledb_ctx, config=self.tiledb_conf)
        self.data_bucket_ready = False
        self.check_and_create_bucket()

    def init_tiledb_conf(self):
        self.tiledb_conf["sm.consolidation.mode"] = "fragment_meta"
        self.tiledb_conf["sm.vacuum.mode"] = "fragment_meta"
        self.tiledb_conf["sm.tile_cache_size"] = 100000000
        self.tiledb_conf["sm.num_reader_threads"] = 1
        self.tiledb_conf["sm.num_writer_threads"] = 1
        self.tiledb_conf["vfs.num_threads"] = get_ncores()
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
        padding_value = type(value)()  # tiledb require to have atleast 2 value for each column
        df = pd.DataFrame(data={key: [padding_value, value]})
        self.store_df('meta', name, df, sparse=False, data_df=False)

    def get_kv(self, name, key):
        try:
            array_existed = tiledb.highlevel.array_exists(self.get_uri('meta', name))
            if not array_existed:
                return None

            result = None
            with tiledb.open(self.get_uri('meta', name), 'r') as A:
                result = A[:]

            if result is None:
                return None

            if not key in result.keys():
                return None

            return result[key][1]  # 0 store nothing, it is just to be compatible with tiledb domain structure
        except Exception as ex:
            self.logger.error(f"Cannot retrieve KV: {name} -> {key}", exc_info=ex)
            raise ex

    # TODO: support dynamic schema
    def store_df(self, datatype, name, df, sparse=True, data_df=True):
        uri = self.get_uri(datatype, name)
        array_existed = tiledb.highlevel.array_exists(uri)

        if not array_existed and data_df:
            if datatype == self._RAW_DATA:
                self.create_dataarray(uri)
            elif datatype == self._HEALTH_DATA:
                self.create_datahealtharray(uri)

            array_existed=True

        tiledb.from_pandas(uri, df,
                           sparse=sparse,
                           mode='append' if array_existed else 'ingest',
                           tile_order='row_major',
                           cell_order='row_major',
                           attrs_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000),
                           coords_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000))

    def get_ts_dataframe(self, datatype, name, from_ts, to_ts):
        try:
            with tiledb.open(self.get_uri(datatype, name)) as A:
                df = pd.DataFrame.from_dict(A[from_ts:to_ts])
                return df
        except Exception as ex:
            self.logger.error(f"Failed to get raw data {datatype} {name} from tiledb")
            return None

    def get_ts_dataarray(self, datatype, name, from_ts, to_ts):
        try:
            result = None
            with tiledb.open(self.get_uri(datatype, name)) as A:
                result = A[from_ts:to_ts]
            return result
        except Exception as ex:
            self.logger.error(f"Failed to get raw data {datatype} {name} from tiledb", exc_info=ex)
            return None

    def get_data_domain(self, datatype, name):
        try:
            domain = (None, None)
            with tiledb.open(self.get_uri(datatype, name)) as A:
                domain = A.nonempty_domain()[0]
                domain = [i.flat[0] for i in domain]
            return domain
        except Exception as ex:
            self.logger.error(f"failed to get domain of data {datatype} {name} from tiledb")
            return None

    def get_raw_data(self, name, from_ts, to_ts):
        return self.get_ts_dataarray('raw', name, from_ts, to_ts)

    def get_clean_data(self, name, from_ts, to_ts):
        return self.get_ts_dataarray('clean', name, from_ts, to_ts)

    def get_policy_data(self, name, from_ts, to_ts):
        return self.get_ts_dataarray('policy', name, from_ts, to_ts)

    def create_dataarray(self, uri):
        dimension = tiledb.Dim(name='date', domain=(np.datetime64('1900-01-01'), np.datetime64('2262-01-01')),
                               tile=np.timedelta64(365, 'ns'),
                               dtype=np.datetime64('', 'ns').dtype)

        domain = tiledb.Domain(dimension)

        attrs = [
            tiledb.Attr(name='bidopen', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='bidclose', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='bidhigh', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='bidlow', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='askopen', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='askclose', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='askhigh', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='asklow', dtype='float64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
            tiledb.Attr(name='tickqty', dtype='int64',
                        filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
        ]

        arraySchema = tiledb.ArraySchema(
            domain=domain,
            attrs=attrs,
            cell_order='row-major',
            tile_order='row-major',
            capacity=10000,
            sparse=True,
            allows_duplicates=False,
            coords_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000),
            offsets_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000))

        tiledb.SparseArray.create(uri, arraySchema)

    def create_datahealtharray(self, uri):
        if uri.endswith("DAILY_METRICS"):
            dimension = tiledb.Dim(name='date', domain=(np.datetime64('1900-01-01'), np.datetime64('2262-01-01')),
                                   tile=np.timedelta64(365, 'ns'),
                                   dtype=np.datetime64('', 'ns').dtype)

            arraySchema = tiledb.ArraySchema(
                domain=tiledb.Domain(dimension),
                attrs=[
                    tiledb.Attr(name='midclose', dtype='float64',
                            filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
                    tiledb.Attr(name='logret', dtype='float64',
                                filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
                    tiledb.Attr(name='logret_ema', dtype='float64',
                                filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000)),
                ],
                cell_order='row-major',
                tile_order='row-major',
                capacity=10000,
                sparse=True,
                allows_duplicates=False,
                coords_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000),
                offsets_filters=tiledb.FilterList([tiledb.GzipFilter(level=-1)], chunksize=512000))

            tiledb.SparseArray.create(uri, arraySchema)


    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = TileDBController()
        return cls._INSTANCE
