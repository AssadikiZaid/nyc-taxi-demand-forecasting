"""Download the monthly TLC yellow-taxi files and the taxi zone lookup table.

Downloads are idempotent: a file already on disk with the size announced by the server is
left untouched, so the command can be re-run safely. Each file is written to a temporary
``.part`` file first and renamed only once complete, so an interrupted download never leaves
a truncated Parquet file behind.

Usage::

    python -m taxi_forecast.ingestion.tlc --start 2025-01 --end 2025-03
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from taxi_forecast.config import ProjectConfig, load_config, month_range
from taxi_forecast.paths import default_data_dir, raw_trip_file, zone_lookup_file

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1 << 20  # 1 MiB
TIMEOUT_SECONDS = 60


class FileNotPublishedError(RuntimeError):
    """Raised when the server does not have the requested file (for example a future month)."""


@dataclass(frozen=True)
class DownloadResult:
    """Outcome of one download."""

    path: Path
    status: Literal["downloaded", "skipped"]
    size_bytes: int


def make_session() -> requests.Session:
    """Return an HTTP session that retries transient server errors with a backoff."""
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("HEAD", "GET"),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _remote_size(session: requests.Session, url: str) -> int | None:
    """Return the size announced by the server, or ``None`` if it gives none.

    Raises:
        FileNotPublishedError: If the server answers 403 or 404 (CloudFront uses 403).
    """
    response = session.head(url, timeout=TIMEOUT_SECONDS, allow_redirects=True)
    if response.status_code in (403, 404):
        raise FileNotPublishedError(f"{url} is not available (HTTP {response.status_code})")
    response.raise_for_status()
    length = response.headers.get("Content-Length")
    return int(length) if length is not None else None


def download_file(url: str, destination: Path, session: requests.Session) -> DownloadResult:
    """Download ``url`` to ``destination`` unless an identical copy is already there.

    The local copy is considered identical when its size equals the size announced by the
    server. When the server announces no size, an existing file is kept as it is.
    """
    expected_size = _remote_size(session, url)
    if destination.exists() and expected_size in (None, destination.stat().st_size):
        logger.info("Skipping %s: already downloaded", destination.name)
        return DownloadResult(destination, "skipped", destination.stat().st_size)

    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    try:
        with session.get(url, stream=True, timeout=TIMEOUT_SECONDS) as response:
            if response.status_code in (403, 404):
                raise FileNotPublishedError(f"{url} is not available (HTTP {response.status_code})")
            response.raise_for_status()
            with partial.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    handle.write(chunk)
        written = partial.stat().st_size
        if expected_size is not None and written != expected_size:
            raise OSError(f"Incomplete download of {url}: {written} of {expected_size} bytes")
        os.replace(partial, destination)
    finally:
        partial.unlink(missing_ok=True)

    logger.info("Downloaded %s (%.1f MB)", destination.name, written / 1e6)
    return DownloadResult(destination, "downloaded", written)


def download_months(
    months: list[tuple[int, int]],
    config: ProjectConfig,
    data_dir: Path,
    session: requests.Session | None = None,
) -> list[DownloadResult]:
    """Download the zone lookup table and the yellow-taxi file of each month."""
    session = session or make_session()
    results = [download_file(config.data.zone_lookup_url, zone_lookup_file(data_dir), session)]
    for year, month in months:
        url = config.data.tlc_url(year, month)
        results.append(download_file(url, raw_trip_file(data_dir, year, month), session))
    return results


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point."""
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default=config.data.start_month, help="first month, YYYY-MM")
    parser.add_argument("--end", default=config.data.end_month, help="last month, YYYY-MM")
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = download_months(month_range(args.start, args.end), config, args.data_dir)
    downloaded = sum(result.status == "downloaded" for result in results)
    logger.info("%d file(s) downloaded, %d already present", downloaded, len(results) - downloaded)


if __name__ == "__main__":
    main()
