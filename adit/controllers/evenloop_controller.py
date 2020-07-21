from __future__ import annotations
import asyncio
from asyncio import Task, Queue, AbstractEventLoop
from typing import Dict, Callable

from adit.config import Config

__all__ = ['EventLoopController']


class EventLoopController:
    _INSTANCE = None

    def __init__(self):
        self.config = Config.instance()  #
        self.queue_size = self.config.get_int("adit", "queuesize", 50)
        self.queue: Queue = asyncio.Queue(self.queue_size)
        self.loop: AbstractEventLoop = asyncio.get_event_loop()
        self.taskmap: Dict[str, Task] = {}

    def shedule_task(self, name: str, func: Callable, **funcArgs) -> None:
        if name in self.taskmap:
            raise Exception("A task with the same name is already in the loop.")

        self.taskmap[name] = self.loop.create_task(func(self.queue, **funcArgs))

    def stop_task(self, name: str) -> None:
        if name not in self.taskmap:
            raise Exception("The given task is not in the task map.")

        task = self.taskmap.pop(name, None)
        if task is not None:
            task.cancel()

    def get_loop(self) -> AbstractEventLoop:
        return self.loop

    def get_queue(self) -> Queue:
        return self.queue

    def start(self) -> None:
        self.loop.run_forever()

    def stop(self) -> None:
        self.loop.close()

    @classmethod
    def instance(cls) -> EventLoopController:
        if cls._INSTANCE is None:
            cls._INSTANCE = EventLoopController()
        return cls._INSTANCE
