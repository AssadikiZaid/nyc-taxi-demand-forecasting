import pandas as pd
import pytest

from taxi_forecast.config import load_config
from taxi_forecast.features.base_table import BaseTableError, build_month, run
from taxi_forecast.features.calendar import build_calendar
from taxi_forecast.ingestion.weather import expected_hours
from taxi_forecast.paths import (
    base_hourly_dir,
    calendar_file,
    demand_hourly_dir,
    raw_weather_file,
)

CONFIG = load_config()
NEW_YORK = CONFIG.data.timezone
VARIABLES = CONFIG.weather.hourly_variables


def write_inputs(data_dir, year=2025, month=11, zones=(1, 2), weather_hours=None):
    hours = expected_hours(year, month, NEW_YORK)
    panel = pd.DataFrame(
        [(zone, hour, zone) for zone in zones for hour in hours],
        columns=["zone_id", "hour_utc", "pickups"],
    )
    partition = demand_hourly_dir(data_dir) / f"year={year}" / f"month={month}"
    partition.mkdir(parents=True)
    panel.to_parquet(partition / "part-00000.snappy.parquet", index=False)
    (partition / "_SUCCESS").write_text("")  # Spark leaves marker files next to the data

    weather_hours = hours if weather_hours is None else weather_hours
    weather = pd.DataFrame({"hour_utc": weather_hours})
    for i, name in enumerate(VARIABLES):
        weather[name] = float(i)
    path = raw_weather_file(data_dir, year, month)
    path.parent.mkdir(parents=True)
    weather.to_parquet(path, index=False)

    calendar = build_calendar(f"{year}-{month:02d}", f"{year}-{month:02d}", NEW_YORK)
    calendar_file(data_dir).parent.mkdir(parents=True, exist_ok=True)
    calendar.to_parquet(calendar_file(data_dir), index=False)
    return calendar


def test_join_keeps_every_row_and_fills_every_column(tmp_path):
    calendar = write_inputs(tmp_path)

    base = build_month(tmp_path, 2025, 11, calendar, VARIABLES)

    assert len(base) == 2 * 721
    assert base[["zone_id", "hour_utc"]].duplicated().sum() == 0
    assert not base.isna().any().any()
    assert list(base.columns[:3]) == ["zone_id", "hour_utc", "pickups"]
    assert {"temperature_2m", "is_holiday", "local_hour"} <= set(base.columns)
    repeated = base[(base.zone_id == 1) & (base.local_date.astype(str) == "2025-11-02")]
    assert repeated["local_hour"].tolist().count(1) == 2  # clock change kept as two hours


def test_missing_weather_hours_stop_the_month(tmp_path):
    hours = expected_hours(2025, 11, NEW_YORK)
    calendar = write_inputs(tmp_path, weather_hours=hours[:-3])

    with pytest.raises(BaseTableError, match="missing values"):
        build_month(tmp_path, 2025, 11, calendar, VARIABLES)


def test_missing_inputs_point_to_the_right_command(tmp_path):
    calendar = build_calendar("2025-11", "2025-11", NEW_YORK)

    with pytest.raises(FileNotFoundError, match="make panel"):
        build_month(tmp_path, 2025, 11, calendar, VARIABLES)
    with pytest.raises(FileNotFoundError, match="make calendar"):
        run([(2025, 11)], CONFIG, tmp_path)


def test_run_writes_one_partition_per_month(tmp_path):
    write_inputs(tmp_path)

    assert run([(2025, 11)], CONFIG, tmp_path) == 2 * 721
    written = pd.read_parquet(base_hourly_dir(tmp_path) / "year=2025" / "month=11")
    assert len(written) == 2 * 721
    assert not list(base_hourly_dir(tmp_path).rglob("*.part"))
