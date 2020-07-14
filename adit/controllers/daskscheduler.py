import atexit
import logging
import gc
import os
import re
import sys
import warnings

import dask

from tornado.ioloop import IOLoop

from distributed import Scheduler
from distributed.cli.utils import install_signal_handlers
from distributed.utils import deserialize_for_cli
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)

logger = logging.getLogger("distributed.scheduler")


def main(
    host="",
    port=None,
    bokeh_port=None,
    dashboard=True,
    bokeh=True,
    dashboard_prefix="",
    pid_file="",
    tls_ca_file=None,
    tls_cert=None,
    tls_key=None,
    dashboard_address=":8787",
    **kwargs
):
    g0, g1, g2 = gc.get_threshold()  # https://github.com/dask/distributed/issues/1653
    gc.set_threshold(g0 * 3, g1 * 3, g2 * 3)

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if bokeh_port is not None:
        warnings.warn(
            "The --bokeh-port flag has been renamed to --dashboard-address. "
            "Consider adding ``--dashboard-address :%d`` " % bokeh_port
        )
        dashboard_address = bokeh_port
    if bokeh is not None:
        warnings.warn(
            "The --bokeh/--no-bokeh flag has been renamed to --dashboard/--no-dashboard. "
        )
        dashboard = bokeh

    if port is None and (not host or not re.search(r":\d", host)):
        port = 8786

    sec = {
        k: v
        for k, v in [
            ("tls_ca_file", tls_ca_file),
            ("tls_scheduler_cert", tls_cert),
            ("tls_scheduler_key", tls_key),
        ]
        if v is not None
    }

    if "DASK_INTERNAL_INHERIT_CONFIG" in os.environ:
        config = deserialize_for_cli(os.environ["DASK_INTERNAL_INHERIT_CONFIG"])
        # Update the global config given priority to the existing global config
        dask.config.update(dask.config.global_config, config, priority="old")

    if not host and (tls_ca_file or tls_cert or tls_key):
        host = "tls://"

    if pid_file:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)

        atexit.register(del_pid_file)

    if sys.platform.startswith("linux"):
        import resource  # module fails importing on Windows

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    loop = IOLoop.current()
    logger.info("-" * 47)

    scheduler = Scheduler(
        loop=loop,
        security=sec,
        host=host,
        port=port,
        dashboard=dashboard,
        dashboard_address=dashboard_address,
        http_prefix=dashboard_prefix,
        **kwargs
    )
    logger.info("-" * 47)

    install_signal_handlers(loop)

    async def run():
        await scheduler
        await scheduler.finished()

    try:
        loop.run_sync(run)
    finally:
        scheduler.stop()

        logger.info("End scheduler at %r", scheduler.address)
