from __future__ import annotations

import os
import sys
import typing
import asyncio
import platform
import subprocess
from adit.config import ConfigManager


class EtcdServerController:
    _BASE_DIR = os.path.dirname(__file__)
    _INSTANCE = None

    def __init__(self):
        self.config = self._get_config_manager()
        self.platform = platform.system().lower()
        self.etcd_homepath = self.config.get_str("etcd", "home_path", None)
        if self.etcd_homepath is None:
            if "linux" in self.platform:
                self.etcd_homepath = os.path.join(self._BASE_DIR, "bin/linux_x64")
            elif "windows" in self.platform:
                self.etcd_homepath = os.path.join(self._BASE_DIR, "bin/windows_x64")
            else:
               raise Exception("unknown system platform. Cannot start etcd.")

        self.etcd_bin = os.path.join(self.etcd_homepath, "etcd" + (".exe" if "windows" in self.platform else ""))
        self.etcdctl_bin = os.path.join(self.etcd_homepath, "etcdctl" + (".exe" if "windows" in self.platform else ""))

    def _get_config_manager(self) -> ConfigManager:
        return ConfigManager.get_instance()

    def start(self):
        pass

    def stop(self):
        pass

    @classmethod
    def get_instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = EtcdServerController()
        return cls._INSTANCE