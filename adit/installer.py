import os
import sys
import shutil
import logging
import zipfile
import tarfile

from adit.config import Config
from adit import constants as const
from adit.utils import *

logger = logging.getLogger(__file__)


def init_workdir(workdir: str = None) -> None:
    oldwd_exists = os.path.exists(workdir)
    override_oldwd = True
    if oldwd_exists:
        if not os.path.isdir(workdir):
            logger.error(
                f"ERROR: {workdir} is currently in used by another program. Please use different working directory.")
            sys.exit(-1)
        elif os.path.exists(os.path.join(workdir, const.ADIT_CONF)):
            logger.warning(f"WARN: found old Adit configuration at {workdir}")
            while True:
                answer = input("Do you want to override old Adit working directory ? (Y/n): ")
                if answer is 'Y':
                    override_oldwd = True
                    break
                elif answer is 'n':
                    override_oldwd = False
                    break
                else:
                    logger.error("ERROR: Please enter only Y or n !")
        else:
            logger.error(
                f"ERROR: {workdir} is currently in used by another program. Please use different working directory.")
            sys.exit(-1)

    if oldwd_exists:
        if override_oldwd:
            logger.info(f"Deleting {workdir} .... ")
            shutil.rmtree(workdir)
            logger.info(f"Re-create {workdir} .... ")
            os.makedirs(workdir)
        else:
            logger.error(
                f"You have chosen not to override old Adit working directory. Please specified a new working directory.")
            sys.exit(0)
    else:
        logger.info(f"Create {workdir} .... ")
        os.makedirs(workdir)

    # TODO: use a more reliable way to create ADIT_HOME variable
    logger.info(f"Add {workdir} to {const.ADIT_HOME_ENV} environment variable.")
    os.environ['ADIT_HOME'] = workdir
    if is_windows():
        logger.info(f"Set environment variable {const.ADIT_HOME_ENV} to {workdir}.")
        os.system(f"setx {const.ADIT_HOME_ENV} \"{workdir}\"")
    else:
        logger.info(f"Export environment variable {const.ADIT_HOME_ENV} to {workdir}.")
        os.system(f"echo 'export {const.ADIT_HOME_ENV}=\"{workdir}\"' >> ~/.bashrc")


def init_workdir_subdir(workdir: str = None) -> None:
    for d in [os.path.join(workdir, d) for d in const.WORK_DIR_SUBDIRS]:
        logger.info(f"Creating directory: {d} .... ")
        os.makedirs(d)


def init_config(workdir: str = None) -> None:
    # dump default config to workdir/adit.conf
    logger.info(f"Create {const.ADIT_CONF} under {workdir} ...")
    Config.init(None)
    Config.instance().dump_config()


def dump_log_config(workdir: str = None) -> None:
    # dump default logging conf to workdir/logging.conf
    logconf_path = os.path.join(workdir, const.LOGGING_CONF)
    log_file = os.path.join(workdir, 'logs', const.ADIT_LOGFILE)
    logconf_str = f"""
[loggers]
keys=root

[handlers]
keys=fileHandler

[formatters]
keys=simpleFormatter

[logger_root]
level=DEBUG
handlers=fileHandler

[handler_fileHandler]
class=logging.handlers.TimedRotatingFileHandler
level=DEBUG
formatter=simpleFormatter
args=(r"{log_file}", 'midnight', 1, 8,)

[formatter_simpleFormatter]
format=[%(asctime)s][%(levelname)8s] %(filename)s:%(lineno)s | %(name)s.%(funcName)s() - %(message)s
"""
    try:
        logger.info(f"Create {const.LOGGING_CONF} under {workdir} ...")
        with open(logconf_path, 'w') as logconf_file:
            logconf_file.write(logconf_str)
    except Exception as ex:
        logger.error(f"ERROR: failed to create {const.LOGGING_CONF} under {workdir}.", exc_info=ex)


def init_bin_dir(workdir: str = None) -> None:
    platform = get_platform()
    bindir = os.path.join(workdir, 'bin')
    logger.info(f"Preparing {bindir}....")
    logger.info(f"Downloading dependencies to {bindir}.")
    urls = [const.WEED_URLS[platform]]
    executable_suffix = ".exe" if is_windows() else ""  # TODO: MacOS may have other suffix
    for url in urls:
        filename = url.split('/')[-1]
        download_path = os.path.join(bindir, filename)
        logger.info(f"Downloading {filename} to {download_path}.")
        download_file_2(url, download_path)
        logger.info(f"Extracting {download_path}.")
        if zipfile.is_zipfile(download_path):
            with zipfile.ZipFile(download_path, 'r') as zip_file:
                for name in zip_file.namelist():
                    if name.endswith(executable_suffix):
                        executable_name = name.split('/')[-1]
                        zip_file.extract(name, bindir)
                        shutil.move(os.path.join(bindir, name), os.path.join(bindir, executable_name))
        elif tarfile.is_tarfile(download_path):
            with tarfile.TarFile(download_path, 'r') as tar_file:
                for name in tar_file.namelist():
                    if name.endswith(executable_suffix):
                        executable_name = name.split('/')[-1]
                        tar_file.extract(name, bindir)
                        shutil.move(os.path.join(bindir, name),
                                    os.path.join(bindir, executable_name))
        else:
            raise Exception(f"Unsupported {filename}'s file type.")
        logger.info(f"Cleaning {download_path}.")
        os.remove(download_path)

    logger.info(f"Cleaning up necessary file and folders in {bindir}.")
    for item in os.listdir(bindir):
        if os.path.isdir(os.path.join(bindir, item)):
            shutil.rmtree(os.path.join(bindir, item), ignore_errors=True)
        elif os.path.isfile(os.path.join(bindir, item)):
            if not item.endswith(executable_suffix):
                os.remove(os.path.join(bindir, item))


def install(workdir: str = None) -> None:
    if workdir is None:
        logger.info(f"No working directory specified. Will use {const.DEFAULT_WORK_DIR} instead.")
        workdir = const.DEFAULT_WORK_DIR

    if ":" in workdir:
        parts = workdir.split(":")
        workdir = parts[0].lower() + ":" + parts[1]

    try:
        init_workdir(workdir=workdir)
        init_workdir_subdir(workdir=workdir)
        init_config(workdir=workdir)
        dump_log_config(workdir=workdir)
        init_bin_dir(workdir=workdir)
        logger.info(f"Finished installation to {workdir}.")
    except Exception as ex:
        logger.error(f"FAILED TO INSTALL", exc_info=ex)
