import os
import sys
import asyncio
import subprocess

__all__ = ['DfsController']

class DfsController:
    _INSTANCE = None
    _OS = None

    def __init__(self):
        pass

    def start(self):
        pass

    def shutdown(self):
        pass

    def backup(self):
        pass

    @classmethod
    def get_instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = DfsController()
        return cls._INSTANCE
