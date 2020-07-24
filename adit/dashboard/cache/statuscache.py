from __future__ import absolute_import

from adit.controllers import EventLoopController

__all__ = ['AditStatusCache']


class AditStatusCache:
    _INSTANCE = None
    _TASK_NAME = "adit-status-cache-update"

    def __init__(self):
        self.evl = EventLoopController.instance()
        self.evl_loop = self.evl.get_loop()

        self.data = {
            'cpu': ''
        }

    async def update(self, queue):
        pass

    def start_periodic_update(self):
        self.evl.shedule_task(self._TASK_NAME, self.update)

    def stop_periodic_update(self):
        self.evl.stop_task(self._TASK_NAME)

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = AditStatusCache()
        return cls._INSTANCE