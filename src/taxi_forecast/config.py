"""Load and validate the project configuration stored in ``configs/project.yaml``."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "project.yaml"

_MONTH_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parse_month(value: str) -> tuple[int, int]:
    """Parse a ``YYYY-MM`` string into a ``(year, month)`` tuple.

    Raises:
        ValueError: If the string is not a valid ``YYYY-MM`` month.
    """
    match = _MONTH_PATTERN.match(value)
    if match is None:
        raise ValueError(f"Expected a month formatted as YYYY-MM, got {value!r}")
    return int(match.group(1)), int(match.group(2))


def month_range(start: str, end: str) -> list[tuple[int, int]]:
    """Return every ``(year, month)`` from ``start`` to ``end`` (``YYYY-MM``), both included.

    Raises:
        ValueError: If a month is malformed or ``start`` is after ``end``.
    """
    year, month = parse_month(start)
    last = parse_month(end)
    if (year, month) > last:
        raise ValueError(f"start month {start} is after end month {end}")
    months = []
    while (year, month) <= last:
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


@dataclass(frozen=True)
class DataConfig:
    """Where the trip data comes from and which period is used."""

    start_month: str
    end_month: str
    timezone: str
    tlc_url_template: str
    zone_lookup_url: str
    excluded_zone_ids: tuple[int, ...]

    def __post_init__(self) -> None:
        """Check that the period is well formed and not empty."""
        if parse_month(self.start_month) > parse_month(self.end_month):
            raise ValueError(f"start_month {self.start_month} is after end_month {self.end_month}")

    def months(self) -> list[tuple[int, int]]:
        """Return every ``(year, month)`` of the period, both ends included, in order."""
        return month_range(self.start_month, self.end_month)

    def tlc_url(self, year: int, month: int) -> str:
        """Return the download URL of the TLC yellow-taxi file for one month."""
        return self.tlc_url_template.format(year=year, month=month)


@dataclass(frozen=True)
class CleaningConfig:
    """Thresholds of the trip-level quality rules."""

    max_duration_hours: float
    max_trip_distance_miles: float
    min_ride_seconds: int

    def __post_init__(self) -> None:
        """Check that every threshold is positive."""
        thresholds = (self.max_duration_hours, self.max_trip_distance_miles, self.min_ride_seconds)
        if min(thresholds) <= 0:
            raise ValueError("cleaning thresholds must be positive")


@dataclass(frozen=True)
class WeatherConfig:
    """Weather source and the hourly variables requested from it."""

    provider: str
    archive_url: str
    latitude: float
    longitude: float
    hourly_variables: tuple[str, ...]


@dataclass(frozen=True)
class ForecastConfig:
    """Definition of the forecasting task."""

    horizon_hours: int
    origin_hour_local: int
    quantiles: tuple[float, ...]
    top_n_zones: int

    def __post_init__(self) -> None:
        """Check the horizon, the origin hour and the quantile levels."""
        if not 1 <= self.horizon_hours <= 168:
            raise ValueError(f"horizon_hours must be between 1 and 168, got {self.horizon_hours}")
        if not 0 <= self.origin_hour_local <= 23:
            raise ValueError(
                f"origin_hour_local must be between 0 and 23, got {self.origin_hour_local}"
            )
        if not all(0 < q < 1 for q in self.quantiles):
            raise ValueError(f"quantiles must be strictly between 0 and 1, got {self.quantiles}")
        if list(self.quantiles) != sorted(set(self.quantiles)):
            raise ValueError(f"quantiles must be strictly increasing, got {self.quantiles}")
        if 0.5 not in self.quantiles:
            raise ValueError("quantiles must include the median (0.5)")
        if self.top_n_zones < 1:
            raise ValueError(f"top_n_zones must be positive, got {self.top_n_zones}")


@dataclass(frozen=True)
class ProjectConfig:
    """Full project configuration."""

    data: DataConfig
    cleaning: CleaningConfig
    weather: WeatherConfig
    forecast: ForecastConfig

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ProjectConfig:
        """Build a validated configuration from the parsed YAML mapping."""
        data = dict(raw["data"])
        data["excluded_zone_ids"] = tuple(data["excluded_zone_ids"])
        weather = dict(raw["weather"])
        weather["hourly_variables"] = tuple(weather["hourly_variables"])
        forecast = dict(raw["forecast"])
        forecast["quantiles"] = tuple(forecast["quantiles"])
        return cls(
            data=DataConfig(**data),
            cleaning=CleaningConfig(**raw["cleaning"]),
            weather=WeatherConfig(**weather),
            forecast=ForecastConfig(**forecast),
        )


def load_config(path: Path | str | None = None) -> ProjectConfig:
    """Read the YAML configuration file and return a validated ``ProjectConfig``.

    Args:
        path: Path to the YAML file. Defaults to ``configs/project.yaml`` at the repository root.
    """
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    with config_path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return ProjectConfig.from_dict(raw)
