"""Join the demand panel, the weather and the calendar into one table per month.

The result has one row per taxi zone and UTC hour: the number of pickups, the weather at
Central Park for that hour and the New York calendar facts. It is the starting point of
every model; lag features are computed later, inside the backtest, so that they only use
data available at each forecast origin.

A month is accepted only if the join keeps exactly the panel's rows and leaves no weather
or calendar value missing.

Usage::

    python -m taxi_forecast.features.base_table --start 2023-01 --end 2026-08
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from taxi_forecast.config import ProjectConfig, load_config, month_range
from taxi_forecast.paths import (
    base_hourly_dir,
    calendar_file,
    default_data_dir,
    demand_hourly_dir,
    raw_weather_file,
)

logger = logging.getLogger(__name__)


class BaseTableError(RuntimeError):
    """Raised when a month cannot be joined completely."""


def build_month(
    data_dir: Path,
    year: int,
    month: int,
    calendar: pd.DataFrame,
    weather_variables: tuple[str, ...],
) -> pd.DataFrame:
    """Return the joined rows of one month, sorted by zone and hour.

    Raises:
        FileNotFoundError: If the panel or the weather of the month is missing.
        BaseTableError: If the join loses or duplicates rows, or leaves values missing.
    """
    partition = demand_hourly_dir(data_dir) / f"year={year}" / f"month={month}"
    weather_path = raw_weather_file(data_dir, year, month)
    if not partition.exists():
        raise FileNotFoundError(f"{partition} is missing: build the panel with `make panel`")
    if not weather_path.exists():
        raise FileNotFoundError(f"{weather_path} is missing: download it with `make weather`")

    panel = pd.read_parquet(partition, columns=["zone_id", "hour_utc", "pickups"])
    weather = pd.read_parquet(weather_path, columns=["hour_utc", *weather_variables])
    try:
        base = panel.merge(weather, on="hour_utc", how="left", validate="many_to_one").merge(
            calendar, on="hour_utc", how="left", validate="many_to_one"
        )
    except pd.errors.MergeError as error:
        raise BaseTableError(f"{year}-{month:02d}: duplicate hours in the inputs") from error

    label = f"{year}-{month:02d}"
    if len(base) != len(panel):
        raise BaseTableError(f"{label}: {len(base)} rows after the join, {len(panel)} before")
    missing = base[[*weather_variables, *calendar.columns]].isna().sum()
    missing = missing[missing > 0]
    if not missing.empty:
        raise BaseTableError(f"{label}: missing values after the join: {missing.to_dict()}")
    return base.sort_values(["zone_id", "hour_utc"], ignore_index=True)


def run(months: list[tuple[int, int]], config: ProjectConfig, data_dir: Path) -> int:
    """Build and write every month; return the number of rows written."""
    path = calendar_file(data_dir)
    if not path.exists():
        raise FileNotFoundError(f"{path} is missing: build it with `make calendar`")
    calendar = pd.read_parquet(path)

    total = 0
    for year, month in months:
        base = build_month(data_dir, year, month, calendar, config.weather.hourly_variables)
        output = base_hourly_dir(data_dir) / f"year={year}" / f"month={month}"
        output.mkdir(parents=True, exist_ok=True)
        partial = output / "part-0.parquet.part"
        base.to_parquet(partial, index=False)
        partial.replace(output / "part-0.parquet")
        total += len(base)
        logger.info("%d-%02d: %d rows", year, month, len(base))
    return total


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point."""
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default=config.data.start_month, help="first month, YYYY-MM")
    parser.add_argument("--end", default=config.data.end_month, help="last month, YYYY-MM")
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    total = run(month_range(args.start, args.end), config, args.data_dir)
    logger.info("Base table written: %d rows in %s", total, base_hourly_dir(args.data_dir))


if __name__ == "__main__":
    main()
