from __future__ import annotations

import sys
import logging
import adit.const as const


class Logger:
    def __init__(self, config_file = None):
        self.log_config_file = config_file if config_file is not None else sys._xoptions.get(const.LOG_CONFIG_FILE_PATH)
