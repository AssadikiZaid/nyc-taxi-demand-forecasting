"""Download the hourly observed weather at Central Park from the Open-Meteo archive.

One Parquet file is written per month, holding every UTC hour of the New York calendar
month, so it lines up with the demand panel's partitions. The data is a reanalysis (ERA5
family) for a single point, the same for every taxi zone.

Downloads are idempotent: a month already on disk with every hour and no missing value is
skipped. The archive lags real time by a few days, so a month whose latest hours are not
available yet is not written; re-running later completes it.

Usage::

    python -m taxi_forecast.ingestion.weather --start 2023-01 --end 2026-08
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
import requests

from taxi_forecast.config import ProjectConfig, WeatherConfig, load_config, month_range
from taxi_forecast.ingestion.tlc import TIMEOUT_SECONDS, make_session
from taxi_forecast.paths import default_data_dir, raw_weather_file
from taxi_forecast.pipeline.hours import utc_hours_of_local_month

logger = logging.getLogger(__name__)


class WeatherNotAvailableError(RuntimeError):
    """Raised when the archive does not cover the whole month yet."""


@dataclass(frozen=True)
class WeatherResult:
    """Outcome of one month."""

    path: Path
    status: Literal["downloaded", "skipped"]
    hours: int


def expected_hours(year: int, month: int, timezone: str) -> pd.DatetimeIndex:
    """Return the UTC hours of the New York calendar month, as a time-zone-aware index."""
    return pd.DatetimeIndex(utc_hours_of_local_month(year, month, timezone)).tz_localize("UTC")


def fetch_month(
    session: requests.Session, weather: WeatherConfig, year: int, month: int, timezone: str
) -> pd.DataFrame:
    """Return one row per UTC hour of the month with the configured weather variables.

    Raises:
        WeatherNotAvailableError: If some hours are missing or have no value yet.
        requests.HTTPError: If the API answers with an error.
    """
    utc_hours = utc_hours_of_local_month(year, month, timezone)
    hours = expected_hours(year, month, timezone)
    params = {
        "latitude": weather.latitude,
        "longitude": weather.longitude,
        "start_date": utc_hours[0].date().isoformat(),
        "end_date": utc_hours[-1].date().isoformat(),
        "hourly": ",".join(weather.hourly_variables),
        "timezone": "GMT",
        "timeformat": "unixtime",
    }
    response = session.get(weather.archive_url, params=params, timeout=TIMEOUT_SECONDS)
    if response.status_code >= 400:
        reason = response.json().get("reason", response.text) if response.content else ""
        raise requests.HTTPError(f"Open-Meteo answered {response.status_code}: {reason}")
    hourly = response.json()["hourly"]

    frame = pd.DataFrame({name: hourly[name] for name in weather.hourly_variables})
    frame.index = pd.to_datetime(hourly["time"], unit="s", utc=True)
    frame = frame.reindex(hours)
    frame.index.name = "hour_utc"

    missing = frame.isna().any(axis=1)
    if missing.any():
        first = frame.index[missing][0]
        raise WeatherNotAvailableError(
            f"{year}-{month:02d}: {missing.sum()} of {len(frame)} hours have no weather yet "
            f"(first one {first:%Y-%m-%d %H:%M} UTC); the archive lags by a few days"
        )
    if "weather_code" in frame:
        frame["weather_code"] = frame["weather_code"].astype("int16")
    return frame.reset_index()


def is_complete(
    path: Path, year: int, month: int, timezone: str, variables: tuple[str, ...]
) -> bool:
    """Tell whether a month on disk has every hour and every variable, with no gap."""
    if not path.exists():
        return False
    frame = pd.read_parquet(path)
    if set(variables) - set(frame.columns):
        return False
    hours = expected_hours(year, month, timezone)
    return (
        len(frame) == len(hours)
        and bool((pd.DatetimeIndex(frame["hour_utc"]) == hours).all())
        and not frame[list(variables)].isna().any().any()
    )


def download_months(
    months: list[tuple[int, int]],
    config: ProjectConfig,
    data_dir: Path,
    session: requests.Session | None = None,
) -> list[WeatherResult]:
    """Download every month that is not complete on disk yet."""
    session = session or make_session()
    timezone, variables = config.data.timezone, config.weather.hourly_variables
    results = []
    for year, month in months:
        path = raw_weather_file(data_dir, year, month)
        if is_complete(path, year, month, timezone, variables):
            logger.info("Skipping weather %d-%02d: already complete", year, month)
            results.append(WeatherResult(path, "skipped", len(pd.read_parquet(path))))
            continue
        frame = fetch_month(session, config.weather, year, month, timezone)
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + ".part")
        frame.to_parquet(partial, index=False)
        partial.replace(path)
        logger.info("Downloaded weather %d-%02d (%d hours)", year, month, len(frame))
        results.append(WeatherResult(path, "downloaded", len(frame)))
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
    complete = len(results) - downloaded
    logger.info("%d month(s) downloaded, %d already complete", downloaded, complete)


if __name__ == "__main__":
    main()
