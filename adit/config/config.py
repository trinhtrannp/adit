from __future__ import annotations

import os
import sys
import configparser
from typing import Union

import adit.constants as const

__all__ = ['ConfigManager']


class ConfigManager:
    _INSTANCE = None

    def __init__(self, config_file: str = None) -> None:
        self.config_file = config_file if config_file is not None else sys._xoptions.get(const.CONFIG_FILE_PATH)
        self.config = configparser.ConfigParser()
        self.__init_config_parser()

    def __init_config_parser(self) -> None:
        self.config._interpolation = configparser.ExtendedInterpolation()
        self.config.read(self.config_file)

    def get_env_variable(self, key: str, default: str = None) -> Union[str, None]:
        return os.getenv(key, default)

    def get_str(self, section: str, key: str, default: str = None) -> Union[str, None]:
        assert section is not None and section is not "", "section should never be None or empty"
        assert key is not None and key is not "", "key should never be None or empty"

        if section not in self.config.sections():
            return default

        if key not in self.config[section]:
            return default

        return self.config[section][key]

    def get_int(self, section: str, key: str, default: int) -> Union[int, None]:
        res = self.get_str(section=section, key=key, default=None)
        if res is None:
            return default
        else:
            try:
                res = int(res)
                return res
            except:
                raise Exception(f"wrong config type for {section}.{key}. Expected integer value instead.")

    def get_bool(self, section: str, key: str, default: bool) -> bool:
        res = self.get_str(section=section, key=key, default=None)
        if res is None:
            return default
        else:
            try:
                res = bool(res)
                return res
            except:
                raise Exception(f"wrong config type for {section}.{key}. Expected boolean value instead.")

    @classmethod
    def get_instance(cls) -> ConfigManager:
        if cls._INSTANCE is None:
            cls._INSTANCE = ConfigManager()
        return cls._INSTANCE
