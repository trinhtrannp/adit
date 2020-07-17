from __future__ import annotations

from adit.config import Config
from typing import Union
import inspect

__all__ = ['CheckPointController']


class CheckPointController:
    _INSTANCE = None

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = CheckPointController()

        return cls._INSTANCE
