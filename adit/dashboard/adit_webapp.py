import os
import logging
import functools

from bokeh.server.server import BokehTornado
from tornado.ioloop import IOLoop
from tornado import web

try:
    from bokeh.server.util import create_hosts_allowlist
except ImportError:
    from bokeh.server.util import create_hosts_whitelist as create_hosts_allowlist
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application
import distributed.http.statics as dask_statics
import dask
import toolz

from adit.dashboard.models import datafeed_doc, datahealth_doc, modelperformance_doc, status_doc
from adit.dashboard.cache import *
from adit.controllers.dask_controller import DaskController, EventLoopController


__all__ = ['AditWebApp']

routes = [
    (
        r"/adit-statics/(.*)",
        web.StaticFileHandler,
        {"path": os.path.join(os.path.dirname(__file__), "static")},
    ),
    (
        r"/dask-statics/(.*)",
        web.StaticFileHandler,
        {"path": os.path.join(os.path.dirname(dask_statics.__file__), "static")},
    ),
]


class AditWebApp:
    _INSTANCE = None
    _ADIT_PREFIX = "/"
    _TEMPLATE_VARIABLE = {
        "pages": ["data-feed", "data-health", "model-performance", "status"]
    }

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.evl = EventLoopController.instance()
        self.tornado_loop = IOLoop.current()
        self.caches = {
            'data-monitor': DataMonitorCache.instance(),
            'dada-health': DataHealthCache.instance(),
            'status': AditStatusCache.instance(),
            'model-performance': ModelPerformanceCache.instance(),
        }

        # Append http routes config so that it will find statics data of adit
        import distributed.config
        dask_http_cfg = dask.config.get("distributed.scheduler.http")
        dask_http_cfg['routes'].append('adit.dashboard.adit_webapp')

    def init(self):
        self.dask_ctr = DaskController.instance()
        self.dask_scheduler = self.dask_ctr.dask_scheduler.scheduler
        self.application = self.dask_scheduler.http_application
        self.applications = {
            "/data-feed": datafeed_doc,
            "/data-health": datahealth_doc,
            "/model-performance": modelperformance_doc,
            "/status": status_doc,
        }
        self.adit_app = self.get_application(applications=self.applications, server=self.dask_scheduler,
                                             prefix=self._ADIT_PREFIX, template_variables=self._TEMPLATE_VARIABLE)
        self.application.add_application(self.adit_app)
        self.adit_app.initialize(self.tornado_loop)

    def get_application(self, applications, server, prefix="/", template_variables={}):
        prefix = prefix or ""
        prefix = "/" + prefix.strip("/")
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        extra = toolz.merge({"prefix": prefix}, template_variables)

        apps = {k: functools.partial(v, server, extra) for k, v in applications.items()}
        apps = {k: Application(FunctionHandler(v)) for k, v in apps.items()}
        kwargs = dask.config.get("distributed.scheduler.dashboard.bokeh-application").copy()
        extra_websocket_origins = create_hosts_allowlist(
            kwargs.pop("allow_websocket_origin"), server.http_server.port
        )

        application = BokehTornado(
            apps,
            prefix=prefix,
            use_index=False,
            extra_websocket_origins=extra_websocket_origins,
            **kwargs,
        )
        return application

    def start(self, mode):
        for name, cache_obj in self.caches.items():
            self.logger.debug(f'Starting {name} cache for dashboards')
            cache_obj.start_periodic_update()

    def stop(self):
        for name, cache_obj in self.caches.items():
            self.logger.debug(f'Starting {name} cache for dashboards')
            cache_obj.stop_periodic_update()


    @classmethod
    def instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = AditWebApp()
        return cls._INSTANCE
