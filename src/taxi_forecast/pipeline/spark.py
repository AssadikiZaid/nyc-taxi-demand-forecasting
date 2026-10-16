"""Create the local Spark session used by the pipeline."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "taxi-forecast",
    master: str = "local[*]",
    driver_memory: str | None = None,
    shuffle_partitions: int = 16,
) -> SparkSession:
    """Return a Spark session configured for this project.

    The session time zone is UTC so that timestamps are never shifted by the machine's own
    time zone. The driver memory defaults to ``SPARK_DRIVER_MEMORY`` or 3 GB, which fits a
    WSL2 machine with 8 GB of RAM because months are processed one at a time.
    """
    memory = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "3g")
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "true")
        # Write standard UTC-adjusted timestamps instead of the legacy INT96 type, so that
        # pandas and PyArrow read ``hour_utc`` back as a time-zone-aware UTC column.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
