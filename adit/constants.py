import os
import socket
from adit.utils import *

# TODO: should detect current IP address in a proper way, or allow user to config IP address.
IP_ADDR = None
try:
    IP_ADDR = socket.gethostbyname(socket.gethostname())
except Exception as ignore:
    raise Exception("Failed to detect IP address of the current machine.")

BASE_PATH = os.path.dirname(os.path.realpath(__file__))

USER_HOME = os.path.expanduser("~")

DEFAULT_WORK_DIR = os.path.join(USER_HOME, ".adit")

WORK_DIR_SUBDIRS = [
    'bin',
    os.path.join('logs', 'crawlers'),
    os.path.join('logs', 'receivers'),
    os.path.join('data', 'server'),
    os.path.join('data', 'volume'),
    os.path.join('dask', 'scheduler'),
    os.path.join('dask', 'worker'),
]

ADIT_HOME_ENV = "ADIT_HOME"

ADIT_CONF = "adit.conf"

LOGGING_CONF = "logging.conf"

ADIT_LOGFILE = "adit.log"
WEED_LOGFILE = "weed.log"
DASK_LOGFILE = "dask.log"

SERVER_MODE = "server"

CLIENT_MODE = "client"

DEFAULT_LOG_LEVEL = "debug"

DEFAULT_DFS_ENGINE = "weed"
DEFAULT_CLUSTER_USER = "aditadmin"
DEFAULT_CLUSTER_PASS = "aditadmin"

DEFAULT_EVENT_LOOP_QUEUE_SIZE = "50"

DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

WEED_VERSION = "1.82"
WEED_URLS = {
    'linux_arm': f'https://github.com/chrislusf/seaweedfs/releases/download/{WEED_VERSION}/linux_arm.tar.gz',
    'linux_x64': f'https://github.com/chrislusf/seaweedfs/releases/download/{WEED_VERSION}/linux_arm64.tar.gz',
    'windows_x64': f'https://github.com/chrislusf/seaweedfs/releases/download/{WEED_VERSION}/windows_amd64.zip',
}

ETCD_VERSION = "v3.4.9"
ETCD_URLS = {
    'linux_arm': f'https://github.com/etcd-io/etcd/releases/download/{ETCD_VERSION}/etcd-{ETCD_VERSION}-linux-arm64.tar.gz',
    'linux_x64': f'https://github.com/etcd-io/etcd/releases/download/{ETCD_VERSION}/etcd-{ETCD_VERSION}-linux-amd64.tar.gz',
    'windows_x64': f'https://github.com/etcd-io/etcd/releases/download/{ETCD_VERSION}/etcd-{ETCD_VERSION}-windows-amd64.zip',
}

#TODO: configurable port
DASK_SCHEDULER_PORT = 8786
DASK_SCHEDULER_DASKBOARD = 8787
DASK_WORKER_PORT = 9000
WEED_MASTER_PORT = 9333
WEED_FILER_PORT = 8888
WEED_S3_PORT = 8333
WEED_VOLUME_PORT = 8080