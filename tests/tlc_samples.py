"""Build small synthetic TLC files for tests."""

from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Same physical types as recent TLC yellow-taxi files (timestamps without time zone).
TLC_SCHEMA = pa.schema(
    [
        ("VendorID", pa.int32()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("total_amount", pa.float64()),
    ]
)


def trip(
    pickup: str,
    minutes: float = 15,
    zone: int | None = 1,
    distance: float | None = 2.0,
    *,
    dropoff: str | None = None,
    fare: float = 12.5,
) -> dict:
    """Return one synthetic trip; the dropoff is ``minutes`` after the pickup by default."""
    start = datetime.fromisoformat(pickup)
    end = datetime.fromisoformat(dropoff) if dropoff else start + timedelta(minutes=minutes)
    return {
        "VendorID": 2,
        "tpep_pickup_datetime": start,
        "tpep_dropoff_datetime": end,
        "passenger_count": 1,
        "trip_distance": distance,
        "PULocationID": zone,
        "DOLocationID": 1,
        "payment_type": 1,
        "fare_amount": fare,
        "total_amount": fare + 3,
    }


def write_trips(path: Path, trips: list[dict]) -> Path:
    """Write synthetic trips as a Parquet file with the TLC schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(trips, schema=TLC_SCHEMA), path)
    return path


def write_zone_lookup(path: Path, zone_ids: list[int]) -> Path:
    """Write a minimal taxi zone lookup table."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ['"LocationID","Borough","Zone","service_zone"']
    lines += [f'{zone_id},"Manhattan","Zone {zone_id}","Yellow Zone"' for zone_id in zone_ids]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
