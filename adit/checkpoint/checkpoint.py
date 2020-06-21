from __future__ import annotations

from adit.config import ConfigManager
from typing import Union
import inspect

__all__ = ['CheckPoint', 'etcd_CheckPoint']


class CheckPoint:
    _CONFIG_SECTION = "adit.checkpoint"
    _ENGINE = ConfigManager.get_instance().get_str(_CONFIG_SECTION, "checkpoint_engine", "etcd")
    _CHECKPOINT_LOCAL_FILE = ConfigManager.get_instance().get_str(_CONFIG_SECTION, "checkpoint_file", "checkpoint")
    _INSTANCE = None

    @classmethod
    def get_instance(cls):
        if cls._INSTANCE is None:
            instance_cls = globals().get(cls._ENGINE + "_CheckPoint")
            assert inspect.isclass(instance_cls), "The given checkpoint engine is unknown."
            cls._INSTANCE = instance_cls()

        return cls._INSTANCE


class etcd_CheckPoint:
    def __init__(self):
        self.config = self._get_config_manager()
        self.checkpointdb_file = self.config.get_str("adit.collector.checkpoint", "dbname", "checkpoint.db")
        self._init_db()

    def _get_config_manager(self) -> ConfigManager:
        return ConfigManager.get_instance()

    def _init_db(self) -> None:
        self.db = ""

    def store(self, key: str, val: str) -> None:
        with self.db as db:
            pass

    def get_str(self, key: str) -> Union[str, None]:
        pass

    def __del__(self):
        try:
            self.db.close()
        except Exception as ignore:
            pass
