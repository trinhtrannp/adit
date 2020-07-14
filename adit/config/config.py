from __future__ import annotations

import os
import logging
import logging.config
import configparser
from typing import Union, Dict

import adit.constants as const

__all__ = ['Config']


class Config:
    _INSTANCE = None

    def __init__(self, config_file: str = None) -> None:
        self.config = configparser.ConfigParser()
        if config_file is None:
            self.__init_with_default()
        else:
            self.__init_with_file(config_file)

    def __init_with_default(self) -> None:
        self.config['adit'] = {}
        self.workdir = os.getenv(const.ADIT_HOME_ENV, const.DEFAULT_WORK_DIR)
        self.configfile = os.path.join(self.workdir, const.ADIT_CONF)
        self.loggingconf = os.path.join(self.workdir, const.LOGGING_CONF)
        self.logfile = os.path.join(self.workdir, 'logs', const.ADIT_LOGFILE)
        self.weedlogfile = os.path.join(self.workdir, 'logs', const.WEED_LOGFILE)
        self.dasklogfile = os.path.join(self.workdir, 'logs', const.DASK_LOGFILE)

        self.config['adit']['workdir'] = self.workdir
        self.config['adit']['config_file'] = self.configfile
        self.config['adit']['logging_conf'] = self.loggingconf
        self.config['adit']['adit_log'] = self.logfile
        self.config['adit']['weed_log'] = self.weedlogfile
        self.config['adit']['dask_log'] = self.dasklogfile
        self.config['adit']['dfs_engine'] = const.DEFAULT_DFS_ENGINE
        self.config['adit']['cluster_user'] = const.DEFAULT_CLUSTER_USER
        self.config['adit']['cluster_pass'] = const.DEFAULT_CLUSTER_PASS
        self.config['adit']['queue_size'] = const.DEFAULT_EVENT_LOOP_QUEUE_SIZE
        print(self.workdir)
        print(self.configfile)
        print(self.loggingconf)
        print(self.workdir)

    def __init_with_file(self, config_file: str = None) -> None:
        self.config._interpolation = configparser.ExtendedInterpolation()
        self.configfile = config_file
        self.config.read(self.configfile)

    def dump_config(self) -> None:
        with open(self.configfile, 'w') as cf:
            self.config.write(cf)

    def set(self, section: str, key: str, val: str) -> None:
        assert section is not None and section is not "", "section should never be None or empty"
        assert key is not None and key is not "", "key should never be None or empty"
        assert val is not None and val is not "", "val should never be None or empty"

        self.config[section][key] = val

    def get_config(self, section: str) -> Union[Dict, None]:
        assert section is not None and section is not "", "section should never be None or empty"
        if section not in self.config.sections():
            return None

        return self.config[section]

    def get_str(self, section: str, key: str, default: str = None) -> Union[str, None]:
        assert section is not None and section is not "", "section should never be None or empty"
        assert key is not None and key is not "", "key should never be None or empty"

        if section not in self.config.sections():
            return default

        if key not in self.config[section]:
            return default

        return self.config[section][key]

    def get_int(self, section: str, key: str, default: int = None) -> Union[int, None]:
        res = self.get_str(section=section, key=key, default=None)
        if res is None:
            return default
        else:
            try:
                res = int(res)
                return res
            except Exception as ex:
                logging.error(f"wrong config type for {section}.{key}. Expected integer value instead.", ex)
                raise Exception(f"wrong config type for {section}.{key}. Expected integer value instead.")

    def get_bool(self, section: str, key: str, default: bool = False) -> bool:
        res = self.get_str(section=section, key=key, default=None)
        if res is None:
            return default
        else:
            try:
                res = bool(res)
                return res
            except Exception as ex:
                logging.error(f"wrong config type for {section}.{key}. Expected boolean value instead.", ex)
                raise Exception(f"wrong config type for {section}.{key}. Expected boolean value instead.")

    @classmethod
    def init(cls, config_file: str = None) -> None:
        cls._INSTANCE = Config(config_file)

    @classmethod
    def instance(cls) -> Config:
        if cls._INSTANCE is None:
            raise Exception(f"Config instance have not been initialized. " +
                            f"Please call Config.init() to init Config first.")
        return cls._INSTANCE
