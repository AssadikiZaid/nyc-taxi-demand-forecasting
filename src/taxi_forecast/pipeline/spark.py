"""Create the local Spark session used by the pipeline."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

# Share of the machine's memory given to the Spark driver when SPARK_DRIVER_MEMORY is unset.
# The rest is left to the operating system, Python and the JVM's own overhead.
DRIVER_MEMORY_SHARE = 0.4
MIN_DRIVER_MB = 1024
MAX_DRIVER_MB = 3072


def total_memory_mb() -> int | None:
    """Return the memory visible to this machine (the WSL2 VM, on Windows), in MB."""
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 2**20
    except (AttributeError, OSError, ValueError):  # not available on this platform
        return None


def default_driver_memory() -> str:
    """Return the driver memory: ``SPARK_DRIVER_MEMORY`` if set, else 40 % of the RAM.

    The value is kept between 1 GB and 3 GB. A month needs about 1.5 GB of RAM in total
    with a 1 GB driver; going beyond what the machine has makes the system swap, which
    slows the job down by an order of magnitude.
    """
    if os.environ.get("SPARK_DRIVER_MEMORY"):
        return os.environ["SPARK_DRIVER_MEMORY"]
    total = total_memory_mb()
    if total is None:
        return "2g"
    return f"{max(MIN_DRIVER_MB, min(MAX_DRIVER_MB, int(total * DRIVER_MEMORY_SHARE)))}m"


def get_spark(
    app_name: str = "taxi-forecast",
    master: str = "local[*]",
    driver_memory: str | None = None,
    shuffle_partitions: int = 16,
) -> SparkSession:
    """Return a Spark session configured for this project.

    The session time zone is UTC so that timestamps are never shifted by the machine's own
    time zone. The driver memory follows ``default_driver_memory`` unless given.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory or default_driver_memory())
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
