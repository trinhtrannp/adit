import os
import sys
import logging
import logging.config

from adit.config import Config
from adit.controllers import DfsController, DaskController, EventLoopController
from adit.dashboard import AditWebApp
from adit import constants as const

# TODO: move the initialization of this into a class or a function and then report user if the environment have not been set
WORKDIR = os.getenv(const.ADIT_HOME_ENV, const.DEFAULT_WORK_DIR)


def init_logging() -> None:
    logger = logging.getLogger(os.path.basename(__file__))
    log_config = os.path.join(WORKDIR, const.LOGGING_CONF)
    try:
        if os.path.exists(log_config):
            logger.info(f"Initialize logging with {log_config}.")
            logging.config.fileConfig(log_config)
            logger_dict = getattr(getattr(getattr(logging, 'Logger'), 'manager'), 'loggerDict')
            logger_dict.pop(os.path.basename(__file__))
            logger = logging.getLogger(os.path.basename(__file__))
        else:
            logger.error(f"ERROR: Cannot found logging config at {log_config}. Please reinstall!")
    except Exception as ex:
        logger.error(f"Failed to init logging config. Please check the log config file under {log_config}.",
                     exc_info=ex)
        sys.exit(-1)


def init_config(mode: str = None, args: dict = None) -> None:
    logger = logging.getLogger(os.path.basename(__file__))
    configfile = os.path.join(WORKDIR, const.ADIT_CONF)
    try:
        if os.path.exists(configfile):
            logger.info(f"Initialize Adit config with {configfile}.")
            Config.init(config_file=configfile)
            config = Config.instance()
            logger.info(f"Add program arguments as config with.")
            config.set("adit", "mode", mode)
            if args is not None:
                for key, val in args.items():
                    config.set("adit", key, val)
                config.dump_config()
        else:
            logger.error(f"ERROR: Cannot found Adit config at {configfile}.")
    except Exception as ex:
        logging.error(f"Failed to init Adit config. Please check the log config file under {configfile}.", exc_info=ex)
        sys.exit(-1)


def start_dfs(mode: str = None) -> None:
    logger = logging.getLogger(os.path.basename(__file__))
    logger.info(f"Starting Distributed File System...")
    dfs_controller = DfsController.instance()
    dfs_controller.start(mode=mode)


def start_dask_and_webapp(mode: str = None) -> None:
    logger = logging.getLogger(os.path.basename(__file__))
    dask_controller = DaskController.instance()
    if mode == const.SERVER_MODE:
        webapp = AditWebApp.instance()

    logger.info(f"Init Dask...")
    dask_controller.init(mode=mode)

    if mode == const.SERVER_MODE:
        logger.info(f"Init Adit Web App ...")
        webapp.init()

    logger.info(f"Start Dask...")
    dask_controller.start(mode=mode)

    if mode == const.SERVER_MODE:
        logger.info(f"Start Adit Web App...")
        webapp.start(mode=mode)


def start_stream_engine(mode: str = None, args: dict = None) -> None:
    logger = logging.getLogger(os.path.basename(__file__))
    logger.info(f"Starting Streaming Engine...")
    pass


def start_ingestor():
    logger = logging.getLogger(os.path.basename(__file__))
    from adit.ingestors import FXCMCrawler
    logger.info("Starting ingestor....")
    crawler = FXCMCrawler()
    logger.info("Starting FXCM crawler....")
    crawler.start()


def start_dataprocessor():
    logger = logging.getLogger(os.path.basename(__file__))
    from adit.processor import MetricsCalculator
    logger.info("Starting data processor....")

    metric_cal = MetricsCalculator()
    logger.info("Starting metric calculator....")
    metric_cal.start()


def start_event_loop() -> None:
    logger = logging.getLogger(os.path.basename(__file__))
    logger.info(f"Starting Main Loop...")
    EventLoopController.instance().start()


def start(mode: str = None, args: dict = None) -> None:
    logger = logging.getLogger(os.path.basename(__file__))
    try:
        init_logging()
        logger = logging.getLogger(os.path.basename(__file__))
        init_config(mode=mode, args=args)
        start_dfs(mode=mode)
        start_dask_and_webapp(mode=mode)
        start_stream_engine(mode=mode, args=args)
        start_ingestor()
        #start_dataprocessor()
        start_event_loop()
    except Exception as ex:
        logger.error(f"Failed to start Adit int {mode} mode", exc_info=ex)
