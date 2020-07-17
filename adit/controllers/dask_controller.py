from __future__ import absolute_import
import os
import logging
from typing import Dict
from subprocess import Popen

from adit.config import Config
from adit import constants as const
from adit.controllers.evenloop_controller import EventLoop
from adit.utils import *

from dask.distributed import Scheduler, Worker


__all__ = ['DaskController', 'DaskScheduler', 'DaskWorker']


class DaskController:
    _INSTANCE = None

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.config: Config = Config.instance()
        self.workdir: str = self.config.get_str(section='adit', key='workdir', default=const.DEFAULT_WORK_DIR)
        self.daskdir: str = os.path.join(self.workdir, 'dask')
        self.schedulerdir: str = os.path.join(self.daskdir, 'scheduler')
        self.workerdir: str = os.path.join(self.daskdir, 'worker')
        self.masterip: str = self.config.get_str(section='adit', key='server_ip', default='127.0.0.1')
        self.workers = {}
        self.evenloop = EventLoop.instance()
        self.dfsprocs: Dict[str, Popen] = dict()

    def _start(self):
        for name, worker in self.workers.items():
            self.logger.info(f"starting {name}")
            self.evenloop.shedule_task(name=name, func=worker.start)

    def start(self, mode: str = None) -> None:
        self.logger.info(f"Starting DASK with {mode} mode")

        if mode is const.SERVER_MODE:
            self.dask_scheduler = DaskScheduler(work_dir=self.schedulerdir)

            self.dask_worker = DaskWorker(work_dir=self.workerdir,
                                          scheduler_ip=self.masterip,
                                          scheduler_port=const.DASK_SCHEDULER_PORT)

            self.workers = {'dask-scheduler': self.dask_scheduler, 'dask-worker': self.dask_worker}
        elif mode is const.CLIENT_MODE:
            self.dask_worker = DaskWorker(work_dir=self.workerdir,
                                          scheduler_ip=self.masterip,
                                          scheduler_port=const.DASK_SCHEDULER_PORT)

            self.workers = {'dask-worker': self.dask_worker}
        else:
            self.logger.error(f"DASK is started in wrong mode, it can only be 'server' or 'client'")
            raise Exception(f"DASK is started in wrong mode, it can only be 'server' or 'client'")

        self._start()

    def shutdown(self):
        for name, worker in self.workers.items():
            self.logger.info(f"Shutting down {name}")
            self.evenloop.shedule_task(name=name, func=worker.stop)

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = DaskController()
        return cls._INSTANCE


class DaskScheduler:
    # TODO: allow user to customize dask scheduler startup
    def __init__(self, work_dir):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.scheduler = Scheduler(port=const.DASK_SCHEDULER_PORT, dashboard=True)

    async def start(self, queue, **kwargs):
        self.logger.info("Starting DASK Scheduler ... ")
        self.scheduler = await self.scheduler
        await self.scheduler.finished()

    async def stop(self, queue, **kwargs):
        self.logger.info("Stoping DASK Scheduler ... ")
        try:
            for worker in self.scheduler.workers_list(workers=None):
                try:
                    await self.scheduler.close_worker(worker=worker)
                except Exception as worker_close_ex:
                    self.logger.error(f"Failed to close worker {worker}", exc_info=worker_close_ex)
                    raise worker_close_ex
        except Exception as ex:
            self.logger.error("Failed to close workers", exc_info=ex)

        try:
            await self.scheduler.close()
        except Exception as ex:
            self.logger.error("Failed to close Dask Scheduler", exc_info=ex)


class DaskWorker:
    # TODO: allow user to customize resource, and more settings for Dask-Worker
    def __init__(self, work_dir, scheduler_ip, scheduler_port):

        self.logger = logging.getLogger(self.__class__.__name__)

        self.worker = Worker(scheduler_ip=scheduler_ip,
                             scheduler_port=scheduler_port,
                             nthreads=get_nthreads(),
                             dashboard=True,
                             local_directory=work_dir)

    async def start(self, queue, **kwargs):
        self.logger.info("Starting DASK worker ....")
        await self.worker
        await self.worker.finished()

    async def stop(self, queue, **kwargs):
        self.logger.info("Stoping DASK worker")
        stopped = False
        try:
            await self.worker.close_gracefully()
            stopped = True
        except Exception as ex:
            self.logger.error("Failed to gracefully close Dask worker. Try again...", exc_info=ex)

        try:
            if not stopped:
                await self.worker.close()
                stopped = True
        except Exception as ex:
            self.logger.error("Failed to forcibly close Dask worker. Try again...", exc_info=ex)