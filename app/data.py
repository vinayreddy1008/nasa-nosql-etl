"""Official raw NASA log download; decompressing gzip is explicitly allowed."""
from __future__ import annotations
import gzip
import shutil
import urllib.request
from pathlib import Path

URLS = {
    'NASA_access_log_Jul95': 'https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz',
    'NASA_access_log_Aug95': 'https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz',
}


def download(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for name, url in URLS.items():
        destination = data_dir / name
        zipped = data_dir / (name + '.gz')
        if destination.exists():
            print(f'Already present: {destination}')
            continue
        print(f'Downloading official archive: {url}', flush=True)
        urllib.request.urlretrieve(url, zipped)
        with gzip.open(zipped, 'rb') as source, open(destination, 'wb') as target:
            shutil.copyfileobj(source, target)
        print(f'Decompressed unchanged: {destination} ({destination.stat().st_size} bytes)')
        zipped.unlink()  # raw decompressed original is retained
