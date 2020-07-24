import concurrent.futures

from adit.utils import *

__all__ = ['TPOOL']

TPOOL = concurrent.futures.ThreadPoolExecutor(max_workers=get_ncores())