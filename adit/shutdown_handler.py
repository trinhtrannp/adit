import atexit
import signal
import sys

from adit.utils import *
from adit.controllers import DaskController, DfsController


# TODO: add more exit/terminate/kill handler
def atexit_handler():
    import logging
    logger = logging.getLogger(__file__+".atexit_handler")
    logger.info("Shuting down Adit..")
    try:
        logger.info("Shut down Dask...")
        dask_controller = DaskController.instance()
        dask_controller.shutdown()
    except Exception as ex:
        logger.error("Failed to shut down Dask, please consider to shutdown Dask manually.", exc_info=ex)

    try:
        logger.info("Shut down DFS...")
        dfs_controller = DfsController.instance()
        dfs_controller.shutdown()
    except Exception as ex:
        logger.error("Failed to shut down DFS, please consider to shutdown DFS manually.", exc_info=ex)

    sys.exit(0)


def signal_handler(sig, frame):
    print(f"receive sig {sig} at frame {frame}")
    atexit_handler()


def init():
    atexit.register(atexit_handler)
    if is_windows():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGBREAK, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    if is_linux():
        signal.signal(signal.SIGHUP, signal_handler)
        signal.signal(signal.SIGABRT, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGKILL, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
