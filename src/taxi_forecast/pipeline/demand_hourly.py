"""Build the hourly demand panel: number of pickups per taxi zone and per UTC hour.

For each month, the job:

1. reads the raw TLC file (only this month, so memory stays bounded);
2. removes exact duplicate rows;
3. flags every trip with the first quality rule it breaks, and counts trips per rule;
4. counts the remaining pickups per zone and per New York wall-clock hour;
5. moves those counts to UTC hours (see ``taxi_forecast.pipeline.hours``);
6. builds the complete grid of zones x UTC hours and fills hours without trips with 0;
7. writes the month as a Parquet partition and a JSON quality report.

Two invariants are checked before a month is accepted: the panel has exactly one row per
zone and hour, and it contains exactly as many pickups as trips kept.

Usage::

    python -m taxi_forecast.pipeline.demand_hourly --start 2025-01 --end 2025-03
    python -m taxi_forecast.pipeline.demand_hourly --skip-up-to-date   # only new months
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812 (PySpark convention)
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampNTZType

from taxi_forecast.config import CleaningConfig, ProjectConfig, load_config, month_range
from taxi_forecast.paths import (
    default_data_dir,
    demand_hourly_dir,
    quality_report_file,
    raw_trip_file,
    zone_lookup_file,
)
from taxi_forecast.pipeline.hours import (
    ONE_HOUR,
    local_hour_mapping,
    month_start,
    next_month_start,
    utc_hours_of_local_month,
)

logger = logging.getLogger(__name__)

PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"
ZONE = "PULocationID"
DROPOFF_ZONE = "DOLocationID"
VENDOR = "VendorID"
DISTANCE = "trip_distance"
FARE = "fare_amount"

# Quality rules in the order they are applied. A trip is counted under the first rule it
# breaks only, so the counts add up to the number of trips removed. The goal is to count
# every pickup that happened exactly once; see docs/decisions.md (D-011, D-013).
REJECT_REASONS = (
    "exact_duplicate",
    "pickup_outside_month",
    "invalid_zone",
    "reversal_record",
    "negative_duration",
    "duration_too_long",
    "no_ride",
    "distance_too_long",
)

# Bumped whenever the rules change, so a quality report always says which rules made it.
RULES_VERSION = 3

# Columns that identify one trip when looking for its reversal record.
_TRIP_KEY = (VENDOR, PICKUP, DROPOFF, ZONE, DROPOFF_ZONE)
# The only raw columns the rules read.
_USED_COLUMNS = (*_TRIP_KEY, DISTANCE, FARE)


class PanelCheckError(RuntimeError):
    """Raised when a month's panel breaks one of its invariants."""


def read_zone_ids(lookup_path: Path, excluded: tuple[int, ...]) -> list[int]:
    """Return the sorted taxi zone ids of the lookup table, without the excluded ones."""
    with lookup_path.open(newline="", encoding="utf-8") as handle:
        ids = {int(row["LocationID"]) for row in csv.DictReader(handle)}
    return sorted(ids - set(excluded))


def _ntz_literal(value: datetime) -> Column:
    """Return a Spark literal for a wall-clock timestamp without time zone."""
    return F.lit(value.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")


def _epoch_seconds(column: Column) -> Column:
    """Return a wall-clock timestamp as seconds, to compute durations.

    The session time zone is UTC, so the conversion adds no offset and differences between
    two such values are exact durations in wall-clock seconds.
    """
    return column.cast("timestamp").cast("long")


def deduplicated_trips(raw: DataFrame) -> DataFrame:
    """Return the columns the rules need, one row per distinct raw row.

    Two rows are exact duplicates when every one of their columns is equal. Rather than
    shuffling all the raw columns (about twenty) to find them, each row is reduced to a
    64-bit hash of all its columns plus the few columns used afterwards; duplicates are
    removed on that narrow row. This keeps the memory needed by the job small enough for a
    laptop, at the cost of a collision probability that is negligible here (two different
    rows would need identical used columns and the same 64-bit hash).
    """
    missing = sorted(set(_USED_COLUMNS) - set(raw.columns))
    if missing:
        raise ValueError(f"The raw file has no column {missing}: has the TLC schema changed?")
    return raw.select(F.xxhash64(*raw.columns).alias("row_hash"), *_USED_COLUMNS).dropDuplicates()


def flag_trips(
    trips: DataFrame,
    year: int,
    month: int,
    zone_ids: list[int],
    cleaning: CleaningConfig,
    timezone: str,
) -> DataFrame:
    """Return the pickup hour, the zone and the first broken rule of each trip.

    The output has three columns: ``local_hour`` (wall-clock hour of the pickup, without time
    zone), ``zone_id`` and ``reject_reason`` (``null`` for a trip that passes every rule).

    A trip is counted when a pickup happened, so the dropoff time and the fare only matter
    as evidence: a dropoff equal to the pickup (some vendors never record it) is kept when
    the taxi moved, and a trip that did not move is kept when it lasted at least
    ``cleaning.min_ride_seconds``. A negative fare is a reversal only when the same trip
    also appears with the opposite fare; on its own it is kept.

    When clocks fall back, a trip picked up during the first 01:00-01:59 and dropped off
    during the second one has a dropoff that looks earlier than its pickup. Such trips are
    real: their duration is corrected by one hour instead of being treated as clock errors.
    """
    fare_cents = F.round(F.abs(F.col(FARE).cast("double")) * 100).cast("long")
    positive_fares = (
        trips.where(F.col(FARE) > 0)
        .select(*_TRIP_KEY, fare_cents.alias("fare_cents"))
        .distinct()
        .withColumn("has_positive_twin", F.lit(True))
    )
    trips = trips.withColumn("fare_cents", fare_cents).join(
        positive_fares, [*_TRIP_KEY, "fare_cents"], "left"
    )

    pickup = F.col(PICKUP).cast("timestamp_ntz")
    dropoff = F.col(DROPOFF).cast("timestamp_ntz")
    wall_clock_duration = _epoch_seconds(dropoff) - _epoch_seconds(pickup)
    in_repeated_hour = F.lit(False)
    for hour in _repeated_local_hours(year, month, timezone):
        start, end = _ntz_literal(hour), _ntz_literal(hour + ONE_HOUR)
        in_repeated_hour = in_repeated_hour | (
            (pickup >= start) & (pickup < end) & (dropoff >= start) & (dropoff < end)
        )
    crosses_fall_back = in_repeated_hour & (wall_clock_duration < 0)
    duration = F.when(crosses_fall_back, wall_clock_duration + 3600).otherwise(wall_clock_duration)
    distance = F.col(DISTANCE).cast("double")
    zone = F.col(ZONE).cast("int")
    no_distance = distance.isNull() | F.isnan(distance) | (distance <= 0)
    too_short = duration.isNull() | (duration < cleaning.min_ride_seconds)

    reason = (
        F.when(
            pickup.isNull()
            | (pickup < _ntz_literal(month_start(year, month)))
            | (pickup >= _ntz_literal(next_month_start(year, month))),
            "pickup_outside_month",
        )
        .when(zone.isNull() | ~zone.isin(zone_ids), "invalid_zone")
        .when((F.col(FARE) < 0) & F.col("has_positive_twin").isNotNull(), "reversal_record")
        .when(duration < 0, "negative_duration")
        .when(duration > cleaning.max_duration_hours * 3600, "duration_too_long")
        .when(no_distance & too_short, "no_ride")
        .when(distance > cleaning.max_trip_distance_miles, "distance_too_long")
    )
    return trips.select(
        F.date_trunc("hour", pickup).cast("timestamp_ntz").alias("local_hour"),
        zone.alias("zone_id"),
        reason.alias("reject_reason"),
    )


def _repeated_local_hours(year: int, month: int, timezone: str) -> list[datetime]:
    """Return the wall-clock hours that happen twice in the month (when clocks fall back)."""
    return sorted(
        {m.local_hour for m in local_hour_mapping(year, month, timezone) if m.n_shares == 2}
    )


def _hour_mapping_frame(spark: SparkSession, year: int, month: int, timezone: str) -> DataFrame:
    schema = StructType(
        [
            StructField("local_hour", TimestampNTZType(), False),
            StructField("utc_hour", TimestampNTZType(), False),
            StructField("share", IntegerType(), False),
            StructField("n_shares", IntegerType(), False),
        ]
    )
    rows = [
        (m.local_hour, m.utc_hour, m.share, m.n_shares)
        for m in local_hour_mapping(year, month, timezone)
    ]
    return spark.createDataFrame(rows, schema)


def build_panel(
    spark: SparkSession,
    local_counts: DataFrame,
    year: int,
    month: int,
    zone_ids: list[int],
    timezone: str,
) -> DataFrame:
    """Turn pickup counts per wall-clock hour into the complete zone x UTC hour panel.

    Args:
        spark: Active Spark session.
        local_counts: Columns ``zone_id``, ``local_hour`` and ``trips``.
        year: Year of the month being built.
        month: Month being built (calendar month in ``timezone``).
        zone_ids: Every zone that must appear in the panel.
        timezone: Time zone of the wall-clock hours.

    Returns:
        Columns ``zone_id``, ``hour_utc`` (UTC timestamp), ``pickups``, ``year``, ``month``.
    """
    mapping = _hour_mapping_frame(spark, year, month, timezone)
    per_utc_hour = (
        local_counts.join(F.broadcast(mapping), "local_hour")
        .select(
            "zone_id",
            "utc_hour",
            F.expr("trips div n_shares + IF(share < trips % n_shares, 1, 0)").alias("pickups"),
        )
        .groupBy("zone_id", "utc_hour")
        .agg(F.sum("pickups").alias("pickups"))
    )

    zones = spark.createDataFrame([(zone_id,) for zone_id in zone_ids], "zone_id int")
    hours = spark.createDataFrame(
        [(hour,) for hour in utc_hours_of_local_month(year, month, timezone)],
        StructType([StructField("utc_hour", TimestampNTZType(), False)]),
    )
    return (
        zones.crossJoin(hours)
        .join(per_utc_hour, ["zone_id", "utc_hour"], "left")
        .select(
            "zone_id",
            # With the session in UTC, this reads the wall-clock UTC value as a UTC instant.
            F.col("utc_hour").cast("timestamp").alias("hour_utc"),
            F.coalesce(F.col("pickups"), F.lit(0)).cast("int").alias("pickups"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
    )


def nanosecond_timestamp_columns(path: Path) -> list[str]:
    """Return the timestamp columns of a Parquet file stored with nanosecond precision.

    Spark cannot read nanosecond Parquet timestamps, and some TLC files may use them. Only
    the file footer is read.
    """
    return [
        field.name
        for field in pq.read_schema(path)
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns"
    ]


def read_trips(
    spark: SparkSession, source: Path, scratch_dir: Path
) -> tuple[DataFrame, Path | None]:
    """Read a raw TLC file with Spark, converting nanosecond timestamps first if needed.

    Returns the DataFrame and the temporary copy that was written (``None`` when the file
    could be read as it is). The caller deletes the copy once the month is processed.
    """
    columns = nanosecond_timestamp_columns(source)
    if not columns:
        return spark.read.parquet(str(source)), None

    logger.info("%s stores %s in nanoseconds: converting to microseconds", source.name, columns)
    table = pq.read_table(source)
    for name in columns:
        position = table.schema.get_field_index(name)
        converted = table.column(name).cast(pa.timestamp("us"), safe=False)
        table = table.set_column(position, name, converted)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    copy = scratch_dir / source.name
    pq.write_table(table, copy)
    return spark.read.parquet(str(copy)), copy


def process_month(
    spark: SparkSession,
    year: int,
    month: int,
    config: ProjectConfig,
    data_dir: Path,
    zone_ids: list[int],
) -> dict[str, Any]:
    """Clean, aggregate and write one month, then return its quality report."""
    source = raw_trip_file(data_dir, year, month)
    if not source.exists():
        raise FileNotFoundError(f"{source} is missing: download it first with `make data`")

    raw, scratch_copy = read_trips(spark, source, data_dir / "interim")
    try:
        return _process_trips(spark, raw, source, year, month, config, data_dir, zone_ids)
    finally:
        if scratch_copy is not None:
            scratch_copy.unlink(missing_ok=True)


def _process_trips(
    spark: SparkSession,
    raw: DataFrame,
    source: Path,
    year: int,
    month: int,
    config: ProjectConfig,
    data_dir: Path,
    zone_ids: list[int],
) -> dict[str, Any]:
    rows_raw = raw.count()  # read from the Parquet footers, no scan
    trips = deduplicated_trips(raw).persist(StorageLevel.MEMORY_AND_DISK)
    flagged = flag_trips(trips, year, month, zone_ids, config.cleaning, config.data.timezone)
    flagged = flagged.persist(StorageLevel.MEMORY_AND_DISK)
    flagged.count()  # materialize, so the raw trips can be released
    trips.unpersist()

    reason_rows = flagged.groupBy("reject_reason").count().collect()
    by_reason = {row["reject_reason"]: row["count"] for row in reason_rows}
    rows_kept = by_reason.pop(None, 0)
    rejected = {"exact_duplicate": rows_raw - rows_kept - sum(by_reason.values())}
    rejected.update({reason: by_reason.get(reason, 0) for reason in REJECT_REASONS[1:]})

    local_counts = (
        flagged.filter(F.col("reject_reason").isNull())
        .groupBy("zone_id", "local_hour")
        .agg(F.count("*").alias("trips"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    by_local_hour = dict(
        local_counts.groupBy(F.hour("local_hour").alias("h")).agg(F.sum("trips")).collect()
    )

    output = demand_hourly_dir(data_dir)
    panel = build_panel(spark, local_counts, year, month, zone_ids, config.data.timezone)
    panel.write.mode("overwrite").partitionBy("year", "month").parquet(str(output))
    flagged.unpersist()
    local_counts.unpersist()

    is_this_month = (F.col("year") == year) & (F.col("month") == month)
    written = spark.read.parquet(str(output)).where(is_this_month)
    stats = written.agg(
        F.count("*").alias("rows"),
        F.sum("pickups").alias("pickups"),
        F.sum(F.when(F.col("pickups") == 0, 1).otherwise(0)).alias("zero_cells"),
        F.countDistinct("zone_id").alias("zones"),
        F.countDistinct("hour_utc").alias("hours"),
    ).first()
    zones_without_pickups = (
        written.groupBy("zone_id").agg(F.sum("pickups").alias("total")).where("total = 0").count()
    )

    n_hours = len(utc_hours_of_local_month(year, month, config.data.timezone))
    expected_rows = len(zone_ids) * n_hours
    if stats["rows"] != expected_rows or stats["hours"] != n_hours:
        raise PanelCheckError(
            f"{year}-{month:02d}: {stats['rows']} rows and {stats['hours']} hours, "
            f"expected {expected_rows} rows and {n_hours} hours"
        )
    if (stats["pickups"] or 0) != rows_kept:
        raise PanelCheckError(
            f"{year}-{month:02d}: the panel holds {stats['pickups']} pickups "
            f"but {rows_kept} trips were kept"
        )

    return {
        "month": f"{year}-{month:02d}",
        "source_file": source.name,
        "source_bytes": source.stat().st_size,
        "rows_raw": rows_raw,
        "rejected": rejected,
        "rows_kept": rows_kept,
        "kept_share": rows_kept / rows_raw if rows_raw else 0.0,
        "rules_version": RULES_VERSION,
        "thresholds": {
            "max_duration_hours": config.cleaning.max_duration_hours,
            "max_trip_distance_miles": config.cleaning.max_trip_distance_miles,
            "min_ride_seconds": config.cleaning.min_ride_seconds,
        },
        "panel": {
            "zones": stats["zones"],
            "hours": stats["hours"],
            "rows": stats["rows"],
            "pickups": stats["pickups"] or 0,
            "zero_cell_share": stats["zero_cells"] / stats["rows"],
            "zones_without_pickups": zones_without_pickups,
        },
        "pickups_by_local_hour": [int(by_local_hour.get(hour) or 0) for hour in range(24)],
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def is_up_to_date(data_dir: Path, year: int, month: int) -> bool:
    """Tell whether a month was already built from the current raw file with the current rules.

    That is the case when its quality report exists, was written by the current
    ``RULES_VERSION`` and describes a raw file of the same size as the one on disk, and its
    panel partition exists.
    """
    report_path = quality_report_file(data_dir, year, month)
    source = raw_trip_file(data_dir, year, month)
    partition = demand_hourly_dir(data_dir) / f"year={year}" / f"month={month}"
    if not (report_path.exists() and source.exists() and partition.exists()):
        return False
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return (
        report.get("rules_version") == RULES_VERSION
        and report.get("source_bytes") == source.stat().st_size
    )


def run(
    months: list[tuple[int, int]],
    config: ProjectConfig,
    data_dir: Path,
    spark: SparkSession,
    skip_up_to_date: bool = False,
) -> list[dict[str, Any]]:
    """Process each month in turn and write its JSON quality report.

    With ``skip_up_to_date``, months already built from the same raw file with the current
    rules are skipped, so only new or changed months are processed.
    """
    zone_ids = read_zone_ids(zone_lookup_file(data_dir), config.data.excluded_zone_ids)
    reports = []
    for year, month in months:
        if skip_up_to_date and is_up_to_date(data_dir, year, month):
            logger.info("%d-%02d: up to date, skipped", year, month)
            continue
        report = process_month(spark, year, month, config, data_dir, zone_ids)
        path = quality_report_file(data_dir, year, month)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        logger.info(
            "%s: %d raw trips, %d kept (%.2f %%), panel %d rows",
            report["month"],
            report["rows_raw"],
            report["rows_kept"],
            100 * report["kept_share"],
            report["panel"]["rows"],
        )
        reports.append(report)
    return reports


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point."""
    from taxi_forecast.pipeline.spark import get_spark, total_memory_mb

    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default=config.data.start_month, help="first month, YYYY-MM")
    parser.add_argument("--end", default=config.data.end_month, help="last month, YYYY-MM")
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    parser.add_argument(
        "--skip-up-to-date",
        action="store_true",
        help="skip months already built from the same raw file with the current rules",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = get_spark("demand-hourly")
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "Spark driver memory %s, %s cores, %s MB of RAM visible",
        spark.sparkContext.getConf().get("spark.driver.memory"),
        os.cpu_count(),
        total_memory_mb(),
    )
    try:
        months = month_range(args.start, args.end)
        run(months, config, args.data_dir, spark, skip_up_to_date=args.skip_up_to_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
