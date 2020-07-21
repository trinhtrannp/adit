import concurrent.futures

from adit.utils import *

__all__ = ['PPOOL', 'TPOOL']

PPOOL = concurrent.futures.ProcessPoolExecutor(max_workers=get_ncores()*2)
TPOOL = concurrent.futures.ThreadPoolExecutor(max_workers=get_ncores()*2)