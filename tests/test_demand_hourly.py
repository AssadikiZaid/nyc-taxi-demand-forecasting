import json
import math
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from taxi_forecast.config import load_config
from taxi_forecast.paths import demand_hourly_dir, quality_report_file, raw_trip_file
from taxi_forecast.pipeline.demand_hourly import (
    RULES_VERSION,
    is_up_to_date,
    nanosecond_timestamp_columns,
    process_month,
    read_zone_ids,
    run,
)
from taxi_forecast.pipeline.quality_report import load_reports, render_markdown
from tlc_samples import trip, write_trips, write_zone_lookup

pytestmark = pytest.mark.spark

CONFIG = load_config()
ZONES = [1, 2, 3, 4]

VALID_JANUARY = [
    trip("2025-01-15 08:30:00", zone=1, fare=10.0),
    trip("2025-01-15 08:45:00", zone=1, fare=11.0),
    trip("2025-01-15 08:59:59", zone=2),
    trip("2025-01-31 23:59:00", zone=1),  # last local hour of the month
    # Seen in the real data and kept on purpose (docs/decisions.md, D-013):
    trip("2025-01-20 10:00:00", dropoff="2025-01-20 10:00:00", zone=3),  # no dropoff time
    trip("2025-01-20 10:30:00", minutes=12, zone=3, distance=0.0),  # distance not recorded
    trip("2025-01-20 11:00:00", zone=3, fare=-4.75),  # negative fare without a twin
    trip("2025-01-21 09:00:00", zone=2, fare=27.5),  # original of the reversal below
]
REJECTED_JANUARY = {
    "exact_duplicate": [trip("2025-01-15 08:30:00", zone=1, fare=10.0)],
    "pickup_outside_month": [trip("2024-12-31 23:50:00"), trip("2025-02-01 00:10:00")],
    "invalid_zone": [
        trip("2025-01-10 12:00:00", zone=264),
        trip("2025-01-10 12:00:00", zone=265),
        trip("2025-01-10 12:00:00", zone=None),
        trip("2025-01-10 12:00:00", zone=999),
        trip("2025-01-10 12:00:00", zone=264, distance=0.0),  # first broken rule wins
    ],
    "reversal_record": [trip("2025-01-21 09:00:00", zone=2, fare=-27.5)],
    "negative_duration": [trip("2025-01-10 12:00:00", dropoff="2025-01-10 11:00:00")],
    "duration_too_long": [trip("2025-01-10 12:00:00", minutes=7 * 60)],
    "no_ride": [
        trip("2025-01-10 12:00:00", minutes=0.5, distance=0.0),
        trip("2025-01-10 12:00:00", dropoff="2025-01-10 12:00:00", distance=0.0),
        trip("2025-01-10 12:00:00", minutes=0.5, distance=None),
        trip("2025-01-10 12:00:00", minutes=0.5, distance=math.nan),
    ],
    "distance_too_long": [trip("2025-01-10 12:00:00", distance=150.0)],
}


@pytest.fixture
def january(tmp_path: Path) -> Path:
    rejected = [row for rows in REJECTED_JANUARY.values() for row in rows]
    write_trips(raw_trip_file(tmp_path, 2025, 1), VALID_JANUARY + rejected)
    return tmp_path


def read_panel(data_dir: Path, year: int, month: int) -> pd.DataFrame:
    path = demand_hourly_dir(data_dir) / f"year={year}" / f"month={month}"
    return pd.read_parquet(path)


def pickups_at(panel: pd.DataFrame, zone: int, hour_utc: datetime) -> int:
    hour = pd.Timestamp(hour_utc, tz=UTC)
    return int(panel.loc[(panel.zone_id == zone) & (panel.hour_utc == hour), "pickups"].iloc[0])


def test_each_rule_counts_the_trips_it_removes(spark, january):
    report = process_month(spark, 2025, 1, CONFIG, january, ZONES)

    expected = {reason: len(rows) for reason, rows in REJECTED_JANUARY.items()}
    assert report["rejected"] == expected
    assert report["rows_raw"] == len(VALID_JANUARY) + sum(expected.values())
    assert report["rows_kept"] == len(VALID_JANUARY)


def test_panel_is_complete_and_keeps_every_trip(spark, january):
    report = process_month(spark, 2025, 1, CONFIG, january, ZONES)
    panel = read_panel(january, 2025, 1)

    assert len(panel) == len(ZONES) * 744
    assert not panel.duplicated(["zone_id", "hour_utc"]).any()
    assert panel.pickups.sum() == len(VALID_JANUARY)
    assert panel.loc[panel.zone_id == 3, "pickups"].sum() == 3  # the three kept edge cases
    assert (panel.loc[panel.zone_id == 4, "pickups"] == 0).all()
    assert report["panel"]["zones_without_pickups"] == 1
    assert sum(report["pickups_by_local_hour"]) == len(VALID_JANUARY)


def test_pickups_are_placed_in_the_right_utc_hour(spark, january):
    process_month(spark, 2025, 1, CONFIG, january, ZONES)
    panel = read_panel(january, 2025, 1)

    assert str(panel.hour_utc.dt.tz) == "UTC"
    assert pickups_at(panel, 1, datetime(2025, 1, 15, 13)) == 2  # 08:xx EST
    assert pickups_at(panel, 2, datetime(2025, 1, 15, 13)) == 1
    assert pickups_at(panel, 1, datetime(2025, 2, 1, 4)) == 1  # 23:59 EST on 31 January


def test_spring_forward_month(spark, tmp_path):
    trips = [
        trip("2025-03-09 01:30:00", zone=1),
        trip("2025-03-09 02:30:00", zone=1),  # clock error: this hour does not exist
        trip("2025-03-09 03:15:00", zone=1),
    ]
    write_trips(raw_trip_file(tmp_path, 2025, 3), trips)

    process_month(spark, 2025, 3, CONFIG, tmp_path, ZONES)
    panel = read_panel(tmp_path, 2025, 3)

    assert len(panel) == len(ZONES) * 743
    assert pickups_at(panel, 1, datetime(2025, 3, 9, 6)) == 1  # 01:30 EST
    assert pickups_at(panel, 1, datetime(2025, 3, 9, 7)) == 2  # 02:30 shifted to 03:00 EDT


def test_fall_back_month_splits_the_repeated_hour(spark, tmp_path):
    trips = [trip("2025-11-02 01:30:00", zone=2, fare=10.0 + i) for i in range(3)]
    write_trips(raw_trip_file(tmp_path, 2025, 11), trips)

    process_month(spark, 2025, 11, CONFIG, tmp_path, ZONES)
    panel = read_panel(tmp_path, 2025, 11)

    assert len(panel) == len(ZONES) * 721
    assert pickups_at(panel, 2, datetime(2025, 11, 2, 5)) == 2  # first 01:xx (EDT)
    assert pickups_at(panel, 2, datetime(2025, 11, 2, 6)) == 1  # second 01:xx (EST)


def test_rerunning_a_month_replaces_it_instead_of_appending(spark, january):
    process_month(spark, 2025, 1, CONFIG, january, ZONES)
    process_month(spark, 2025, 1, CONFIG, january, ZONES)

    assert len(read_panel(january, 2025, 1)) == len(ZONES) * 744


def test_missing_raw_file_points_to_the_download_step(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="make data"):
        process_month(spark, 2025, 1, CONFIG, tmp_path, ZONES)


def test_run_writes_quality_reports_that_render_as_markdown(spark, january):
    write_zone_lookup(january / "raw" / "taxi_zone_lookup.csv", [*ZONES, 264, 265])

    run([(2025, 1)], CONFIG, january, spark)

    report = json.loads(quality_report_file(january, 2025, 1).read_text())
    assert report["month"] == "2025-01"
    assert report["rules_version"] == RULES_VERSION
    markdown = render_markdown(load_reports(january / "quality"))
    assert "| 2025-01 |" in markdown
    assert "**Total**" in markdown


def test_reports_from_different_rule_versions_are_not_mixed(spark, january):
    write_zone_lookup(january / "raw" / "taxi_zone_lookup.csv", [*ZONES, 264, 265])
    run([(2025, 1)], CONFIG, january, spark)
    old = json.loads(quality_report_file(january, 2025, 1).read_text())
    old.pop("rules_version")
    old["month"] = "2024-12"
    quality_report_file(january, 2024, 12).write_text(json.dumps(old))

    with pytest.raises(ValueError, match="different rule versions"):
        render_markdown(load_reports(january / "quality"))


def test_zone_ids_exclude_unknown_zones(tmp_path):
    lookup = write_zone_lookup(tmp_path / "zones.csv", [1, 2, 264, 265])

    assert read_zone_ids(lookup, (264, 265)) == [1, 2]


def test_nanosecond_timestamps_are_converted_before_spark_reads_them(spark, tmp_path):
    rows = [trip("2025-01-15 08:30:00", zone=1), trip("2025-01-15 09:30:00", zone=2)]
    path = write_trips(raw_trip_file(tmp_path, 2025, 1), rows)
    table = pq.read_table(path)
    for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        column = table.column(name).cast(pa.timestamp("ns"))
        table = table.set_column(table.schema.get_field_index(name), name, column)
    pq.write_table(table, path)
    assert nanosecond_timestamp_columns(path) == ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    report = process_month(spark, 2025, 1, CONFIG, tmp_path, ZONES)

    assert report["rows_kept"] == 2
    assert not list((tmp_path / "interim").glob("*.parquet"))  # temporary copy removed


def test_up_to_date_months_are_skipped(spark, january):
    write_zone_lookup(january / "raw" / "taxi_zone_lookup.csv", [*ZONES, 264, 265])
    assert not is_up_to_date(january, 2025, 1)

    run([(2025, 1)], CONFIG, january, spark)
    assert is_up_to_date(january, 2025, 1)
    assert run([(2025, 1)], CONFIG, january, spark, skip_up_to_date=True) == []

    write_trips(raw_trip_file(january, 2025, 1), VALID_JANUARY)  # the TLC republished it
    assert not is_up_to_date(january, 2025, 1)


def test_rows_differing_only_in_an_unused_column_are_not_duplicates(spark, tmp_path):
    first = trip("2025-01-15 08:30:00", zone=1, fare=10.0)
    second = dict(first, passenger_count=3)  # same trip fields, another passenger count
    write_trips(raw_trip_file(tmp_path, 2025, 1), [first, second, first])

    report = process_month(spark, 2025, 1, CONFIG, tmp_path, ZONES)

    assert report["rejected"]["exact_duplicate"] == 1
    assert report["rows_kept"] == 2


def test_a_missing_column_gives_a_clear_error(spark, tmp_path):
    path = write_trips(raw_trip_file(tmp_path, 2025, 1), [trip("2025-01-15 08:30:00")])
    pq.write_table(pq.read_table(path).drop_columns(["fare_amount"]), path)

    with pytest.raises(ValueError, match="fare_amount"):
        process_month(spark, 2025, 1, CONFIG, tmp_path, ZONES)
