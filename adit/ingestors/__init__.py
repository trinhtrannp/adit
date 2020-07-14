from .crawlers import *
from .receivers import *

__all__ = (
        crawlers.__all__ +
        receivers.__all__
)


class AbstractIngestor:

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass
