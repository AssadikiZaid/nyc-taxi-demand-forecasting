"""Locations of the data files, all derived from one data directory.

The data directory defaults to ``data/`` at the repository root and can be moved with the
``TAXI_FORECAST_DATA_DIR`` environment variable (for example to a larger drive).
"""

from __future__ import annotations

import os
from pathlib import Path

from taxi_forecast.config import PROJECT_ROOT


def default_data_dir() -> Path:
    """Return the data directory, honouring ``TAXI_FORECAST_DATA_DIR`` when it is set."""
    return Path(os.environ.get("TAXI_FORECAST_DATA_DIR", PROJECT_ROOT / "data"))


def raw_trip_file(data_dir: Path, year: int, month: int) -> Path:
    """Return the path of the raw TLC yellow-taxi file of one month."""
    return data_dir / "raw" / "yellow" / f"yellow_tripdata_{year}-{month:02d}.parquet"


def zone_lookup_file(data_dir: Path) -> Path:
    """Return the path of the TLC taxi zone lookup table."""
    return data_dir / "raw" / "taxi_zone_lookup.csv"


def demand_hourly_dir(data_dir: Path) -> Path:
    """Return the directory of the hourly demand panel, partitioned by year and month."""
    return data_dir / "processed" / "demand_hourly"


def quality_report_file(data_dir: Path, year: int, month: int) -> Path:
    """Return the path of the JSON quality report of one month."""
    return data_dir / "quality" / f"quality_{year}-{month:02d}.json"
