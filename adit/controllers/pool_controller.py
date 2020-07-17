from __future__ import absolute_import

from concurrent.futures import ProcessPoolExecutor

from adit.utils import *

__all__ = ['get_ppool']

P_POOL = ProcessPoolExecutor(max_workers=get_ncores())


def get_ppool() -> ProcessPoolExecutor:
    return P_POOL
