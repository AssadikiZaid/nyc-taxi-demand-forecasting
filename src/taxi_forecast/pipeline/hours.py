"""Map New York wall-clock hours to UTC hours, including daylight saving changes.

TLC timestamps are wall-clock times in New York with no time zone attached. The demand panel
is indexed by UTC hour instead, because every UTC hour exists exactly once: lags such as
"24 hours earlier" then always point to the right row.

Two wall-clock hours per year need a rule:

* **Spring forward** (second Sunday of March): 02:00-02:59 does not exist. Trips stamped in
  that hour (clock errors) are assigned to the next hour, 03:00, as Java and Spark do.
* **Fall back** (first Sunday of November): 01:00-01:59 happens twice and the timestamps
  cannot tell the two apart. Its trips are split evenly between the two UTC hours, so the
  series shows neither a doubled hour nor an artificial zero.

A month always means a calendar month in New York time, like the TLC files.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

ONE_HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class HourMapping:
    """One wall-clock hour linked to one UTC hour.

    Attributes:
        local_hour: Wall-clock hour in New York, without time zone.
        utc_hour: Matching UTC hour, without time zone.
        share: Position of ``utc_hour`` among the UTC hours of this wall-clock hour.
        n_shares: Number of UTC hours this wall-clock hour is split between (1 or 2).
    """

    local_hour: datetime
    utc_hour: datetime
    share: int
    n_shares: int


def month_start(year: int, month: int) -> datetime:
    """Return midnight on the first day of the month, without time zone."""
    return datetime(year, month, 1)


def next_month_start(year: int, month: int) -> datetime:
    """Return midnight on the first day of the following month, without time zone."""
    return datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)


def utc_hours_of_local_month(year: int, month: int, timezone: str) -> list[datetime]:
    """Return every UTC hour that falls in the given calendar month of ``timezone``.

    The result is complete and has no duplicates: 744 hours for a 31-day month, one less in
    the month that springs forward and one more in the month that falls back.
    """
    zone = ZoneInfo(timezone)
    start = month_start(year, month).replace(tzinfo=zone).astimezone(UTC)
    end = next_month_start(year, month).replace(tzinfo=zone).astimezone(UTC)
    hours = []
    hour = start
    while hour < end:
        hours.append(hour.replace(tzinfo=None))
        hour += ONE_HOUR
    return hours


def local_hour_mapping(year: int, month: int, timezone: str) -> list[HourMapping]:
    """Link each wall-clock hour of the month to the UTC hour(s) its trips belong to."""
    zone = ZoneInfo(timezone)
    utc_by_local: dict[datetime, list[datetime]] = defaultdict(list)
    for utc_hour in utc_hours_of_local_month(year, month, timezone):
        local = utc_hour.replace(tzinfo=UTC).astimezone(zone).replace(tzinfo=None)
        utc_by_local[local].append(utc_hour)

    mappings = []
    local = month_start(year, month)
    end = next_month_start(year, month)
    while local < end:
        utc_hours = sorted(utc_by_local.get(local, []))
        if not utc_hours:  # the hour skipped when clocks spring forward
            utc_hours = [min(utc_by_local[local + ONE_HOUR])]
        mappings.extend(
            HourMapping(local, utc_hour, share, len(utc_hours))
            for share, utc_hour in enumerate(utc_hours)
        )
        local += ONE_HOUR
    return mappings


def split_count(count: int, share: int, n_shares: int) -> int:
    """Return the part of ``count`` given to share ``share`` out of ``n_shares``.

    The parts are as equal as possible and always add up to ``count``; the earlier shares
    receive the remainder.
    """
    return count // n_shares + (1 if share < count % n_shares else 0)
