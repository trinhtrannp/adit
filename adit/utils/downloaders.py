import shutil
import requests
from tqdm import tqdm
from urllib.request import urlretrieve

__all__ = ['download_file', 'download_file_2', 'download_file_3']


def download_file(url: str = None, dst: str = None) -> None:
    with requests.get(url, stream=True) as r:
        with open(dst, 'wb') as f:
            shutil.copyfileobj(r.raw, f)


def download_file_2(url: str = None, dst: str = None) -> None:
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_length = int(r.headers.get('content-length'))
        progress_bar = tqdm(desc='Downloading', total=total_length, unit='bit')
        with open(dst, 'wb') as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
                progress_bar.update(65536)


def download_file_3(url: str = None, dst: str = None) -> None:
    urlretrieve(url, dst)