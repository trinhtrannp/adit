import os
import logging
import subprocess
from typing import Dict
from subprocess import Popen


from adit.config import Config
from adit import constants as const

from distributed.cli import dask_scheduler, dask_worker

__all__ = ['DaskController']


class DaskController:
    _INSTANCE = None

    _SCHEDULER_PORT = 8786
    _SERVER_COMMAND = {
        'dask-scheduler': "dask-scheduler",
        'dask-worker': "dask-worker tcp://{masterip}:{masterport}"
    }

    _WORKER_PORT = 9000
    _CLIENT_COMMAND = {
        'dask-worker': "dask-worker tcp://{masterip}:{masterport}"
    }

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.config: Config = Config.instance()
        self.workdir: str = self.config.get_str(section='adit', key='workdir', default=const.DEFAULT_WORK_DIR)
        self.daskdir: str = os.path.join(self.workdir, 'dask')
        self.schedulerdir: str = os.path.join(self.daskdir, 'scheduler')
        self.workerdir: str = os.path.join(self.daskdir, 'worker')
        self.masterip: str = self.config.get_str(section='adit', key='server_ip', default='127.0.0.1')
        self.dfsprocs: Dict[str, Popen] = dict()

    def start(self, mode: str = None) -> None:
        self.logger.info(f"Starting DASK with {mode} mode")
        #commands: Dict[str, str] = None
        #cwd: str = None
        from adit.controllers import daskscheduler, daskworker
        if mode is const.SERVER_MODE:
            #commands = self._SERVER_COMMAND
            #cwd = self.schedulerdir

            daskscheduler.main()
            daskworker.main(scheduler=f'tcp://{self.masterip}:{const.DASK_SCHEDULER_PORT}')
        elif mode is const.CLIENT_MODE:
            #commands = self._CLIENT_COMMAND
            #cwd = self.workerdir
            daskworker.main(scheduler=f'tcp://{self.masterip}:{const.DASK_SCHEDULER_PORT}')
        else:
            self.logger.error(f"DASK is started in wrong mode, it can only be 'server' or 'client'")
            raise Exception(f"DASK is started in wrong mode, it can only be 'server' or 'client'")

        #for name, command in commands.items():
        #    fullcommand = command.format(
        #        masterip=self.masterip,
        #        masterport=const.DASK_SCHEDULER_PORT
        #    )
        #    self.logger.info(f"Starting up {name}...")
        #    self.dfsprocs[name] = subprocess.Popen(fullcommand.split(" "), cwd=cwd)

    def shutdown(self):
        pass
        #for pname, process in self.dfsprocs.items():
        #    self.logger.info(f"Shutting down process {pname}")
        #    try:
        #        process.terminate()
        #        process.wait(30)
        #    except Exception as ex:
        #        self.logger.error(f"Failed to terminate process {pname}", exc_info=ex)
        #        try:
        #            process.kill()
        #        except Exception as killex:
        #            self.logger.error(f"Cannot even kill process {pname}, please check and manually kill it",
        #                              exc_info=killex)
        #            pass

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = DaskController()
        return cls._INSTANCE