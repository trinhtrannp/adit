import atexit
import signal
import sys

from adit.utils import *
from adit.controllers import *
from adit.dashboard import AditWebApp


# TODO: add more exit/terminate/kill handler
def atexit_handler():
    import logging
    logger = logging.getLogger(__file__+".atexit_handler")
    logger.info("Shutting down Adit.")
    try:
        logger.info("Shutting down Adit Web App...")
        webapp_ctr = AditWebApp.instance()
        webapp_ctr.stop()
    except Exception as ex:
        logger.error("Failed to shut down Adits' Web App.", exc_info=ex)

    try:
        logger.info("Shutting down Dask...")
        dask_ctr = DaskController.instance()
        dask_ctr.stop()
    except Exception as ex:
        logger.error("Failed to shut down Dask, please consider to shutdown Dask manually.", exc_info=ex)

    try:
        logger.info("Shutting down DFS...")
        dfs_ctr = DfsController.instance()
        dfs_ctr.stop()
    except Exception as ex:
        logger.error("Failed to shut down DFS, please consider to shutdown DFS manually.", exc_info=ex)

    try:
        logger.info("Shuting down AsyncIO Event Loop Controller...")
        evl_ctr = EventLoopController.instance()
        evl_ctr.stop()
    except Exception as ex:
        logger.error("Failed to shut down AsyncIO event loop controller...", exc_info=ex)

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
