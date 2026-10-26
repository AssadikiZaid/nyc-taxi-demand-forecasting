"""Build the hourly calendar table: one row per UTC hour with New York calendar facts.

Every column is known in advance, so the calendar can be built beyond the last month of
trips and used for the hours being forecast. Hours follow the panel's convention: every UTC
hour of each New York calendar month (see ``taxi_forecast.pipeline.hours``).

Columns:

- ``hour_utc``: the hour, UTC (time-zone aware);
- ``local_date``, ``local_hour``: date and hour on the New York wall clock;
- ``day_of_week``: 0 for Monday to 6 for Sunday; ``is_weekend``;
- ``holiday``: name of the US federal holiday that day, including observed days, else
  empty; ``is_holiday``, ``is_day_before_holiday``, ``is_day_after_holiday``;
- ``is_ny_state_holiday``: the extra New York State holidays (Lincoln's Birthday, Election
  Day...), kept apart because most businesses stay open;
- ``utc_offset_hours``: -5 in winter, -4 in summer; ``is_clock_change_day``: the day lasts
  23 or 25 hours.

Usage::

    python -m taxi_forecast.features.calendar
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import holidays
import pandas as pd

from taxi_forecast.config import load_config, month_range, parse_month
from taxi_forecast.paths import calendar_file, default_data_dir
from taxi_forecast.pipeline.hours import utc_hours_of_local_month


def next_month(month: str) -> str:
    """Return the ``YYYY-MM`` month that follows ``month``."""
    year, number = parse_month(month)
    return f"{year + 1}-01" if number == 12 else f"{year}-{number + 1:02d}"


def build_calendar(start_month: str, end_month: str, timezone: str) -> pd.DataFrame:
    """Return the calendar of every UTC hour from ``start_month`` to ``end_month``."""
    hours = pd.DatetimeIndex(
        [
            hour
            for y, m in month_range(start_month, end_month)
            for hour in utc_hours_of_local_month(y, m, timezone)
        ]
    ).tz_localize("UTC")
    local = hours.tz_convert(timezone)
    local_dates = pd.Series(local.date, index=hours)

    years = range(local.year.min() - 1, local.year.max() + 2)
    federal = holidays.country_holidays("US", years=years)
    state_only = {
        day: name
        for day, name in holidays.country_holidays("US", subdiv="NY", years=years).items()
        if day not in federal
    }

    def shifted(days: int) -> pd.Series:
        return local_dates.map(lambda day: (day + timedelta(days=days)) in federal)

    hours_per_day = local_dates.groupby(local_dates).transform("size")
    calendar = pd.DataFrame(
        {
            "hour_utc": hours,
            "local_date": local_dates.to_numpy(),
            "local_hour": local.hour.astype("int8"),
            "day_of_week": local.dayofweek.astype("int8"),
            "is_weekend": local.dayofweek >= 5,
            "holiday": local_dates.map(lambda day: federal.get(day, "")).to_numpy(),
            "is_holiday": local_dates.map(lambda day: day in federal).to_numpy(),
            "is_day_before_holiday": shifted(1).to_numpy(),
            "is_day_after_holiday": shifted(-1).to_numpy(),
            "is_ny_state_holiday": local_dates.map(lambda day: day in state_only).to_numpy(),
            "utc_offset_hours": (local.tz_localize(None) - hours.tz_localize(None))
            .total_seconds()
            .astype("int32")
            // 3600,
            "is_clock_change_day": (hours_per_day != 24).to_numpy(),
        }
    )
    calendar["utc_offset_hours"] = calendar["utc_offset_hours"].astype("int8")
    return calendar


def holidays_in(calendar: pd.DataFrame) -> dict[date, str]:
    """Return the federal holidays the calendar covers, by local date."""
    days = calendar.loc[calendar["is_holiday"], ["local_date", "holiday"]].drop_duplicates()
    return dict(zip(days["local_date"], days["holiday"], strict=True))


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point."""
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default=config.data.start_month, help="first month, YYYY-MM")
    parser.add_argument(
        "--end",
        default=next_month(config.data.end_month),
        help="last month, YYYY-MM (default: one month after the last month of trips, so the "
        "hours being forecast are covered)",
    )
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    args = parser.parse_args(argv)

    calendar = build_calendar(args.start, args.end, config.data.timezone)
    path = calendar_file(args.data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    calendar.to_parquet(path, index=False)
    print(
        f"Wrote {path}: {len(calendar):,} hours from {args.start} to {args.end}, "
        f"{len(holidays_in(calendar))} federal holiday days"
    )


if __name__ == "__main__":
    main()
