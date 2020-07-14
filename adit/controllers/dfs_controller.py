from __future__ import annotations

import os
import logging
import subprocess
from subprocess import Popen
from typing import Dict

from adit.config import Config
from adit.utils import *
from adit import constants as const

__all__ = ['DfsController']


# TODO: support MacOS, ARM, X86 binaries
# TODO: improve signal handling and termination
# TODO: allow configuration for ports
class DfsController:
    _INSTANCE = None

    _SERVER_COMMANDS = {
        # TODO: make ip configurable because someone may one to choose a bin it to a specific network interface
        "mvfs-servers": "server -s3=true -filer=true -dir {datadir} -master.dir {masterdir} -ip {ip} -ip.bind {ip} -volume.max 10"
    }
    _CLIENT_COMMANDS = {
        # TODO: make ip configurable because someone may one to choose a bin it to a specific network interface
        "volume-server": 'volume -ip {ip} -ip.bind {ip} -dir {datadir} -max 10 -mserver {masterip}:{masterport}',
        "s3-server": "s3 -filer {masterip}:{filerport}"
    }

    def __init__(self):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.config: Config = Config.instance()
        self.workdir: str = self.config.get_str(section='adit', key='workdir', default=const.DEFAULT_WORK_DIR)
        self.datadir: str = os.path.join(self.workdir, 'data')
        self.masterdir: str = os.path.join(self.datadir, 'server')
        self.volumedir: str = os.path.join(self.datadir, 'volume')
        self.masterip: str = self.config.get_str(section='adit', key='server_ip', default='127.0.0.1')
        self.engine: str = self.config.get_str(section='adit', key='dfs_engine', default=const.DEFAULT_DFS_ENGINE)
        self.binname: str = self.engine + ".exe" if is_windows() else ""
        self.bindir: str = os.path.join(self.workdir, 'bin')
        self.binpath: str = os.path.join(self.bindir, self.binname)
        self.dfsprocs: Dict[str, Popen] = dict()

    def start(self, mode: str = None) -> None:
        self.logger.info(f"Starting up DFS with {self.binpath}")
        commands: Dict[str, str] = None
        cwd: str = None
        if mode is const.SERVER_MODE:
            commands = self._SERVER_COMMANDS
            cwd = self.masterdir
        elif mode is const.CLIENT_MODE:
            commands = self._CLIENT_COMMANDS
            cwd = self.volumedir
        else:
            self.logger.error(f"DFS is started in wrong mode, it can only be 'server' or 'client'")
            raise Exception(f"DFS is started in wrong mode, it can only be 'server' or 'client'")

        for name, command in commands.items():
            self.logger.info(f"Starting up {name}...")
            fullcommand = self.binpath + " " + command.format(
                datadir=self.datadir,
                masterdir=self.masterdir,
                ip=const.IP_ADDR,
                masterip=self.masterip,
                masterport=const.WEED_MASTER_PORT,
                filerport=const.WEED_FILER_PORT
            )
            self.dfsprocs[name] = subprocess.Popen(fullcommand.split(" "), cwd=cwd)

    def shutdown(self):  # will be call to gradually shutdown DFS when process died
        for pname, process in self.dfsprocs.items():
            self.logger.info(f"Shutting down process {pname}")
            try:
                process.terminate()
                process.wait(30)
            except Exception as ex:
                self.logger.error(f"Failed to terminate process {pname}", exc_info=ex)
                try:
                    process.kill()
                except Exception as killex:
                    self.logger.error(f"Cannot even kill process {pname}, please check and manually kill it", exc_info=killex)
                    pass

    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = DfsController()
        return cls._INSTANCE
