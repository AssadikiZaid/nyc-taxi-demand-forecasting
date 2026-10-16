from collections import Counter
from datetime import datetime

import pytest

from taxi_forecast.pipeline.hours import (
    local_hour_mapping,
    split_count,
    utc_hours_of_local_month,
)

NEW_YORK = "America/New_York"


@pytest.mark.parametrize(
    ("year", "month", "expected"),
    [
        (2025, 1, 744),  # 31 days, no clock change
        (2025, 2, 672),  # 28 days
        (2025, 3, 743),  # spring forward on 9 March: one hour fewer
        (2025, 11, 721),  # 30 days, fall back on 2 November: one hour more
        (2024, 2, 696),  # leap year
    ],
)
def test_month_has_the_right_number_of_utc_hours(year, month, expected):
    hours = utc_hours_of_local_month(year, month, NEW_YORK)

    assert len(hours) == expected
    assert len(set(hours)) == expected


def test_month_starts_at_local_midnight():
    hours = utc_hours_of_local_month(2025, 1, NEW_YORK)

    assert hours[0] == datetime(2025, 1, 1, 5)  # midnight EST = 05:00 UTC
    assert hours[-1] == datetime(2025, 2, 1, 4)


def test_consecutive_months_neither_overlap_nor_leave_a_gap():
    december = utc_hours_of_local_month(2024, 12, NEW_YORK)
    january = utc_hours_of_local_month(2025, 1, NEW_YORK)

    assert (january[0] - december[-1]).total_seconds() == 3600


def test_regular_hour_maps_to_one_utc_hour():
    mapping = {m.local_hour: m for m in local_hour_mapping(2025, 1, NEW_YORK)}

    entry = mapping[datetime(2025, 1, 15, 8)]
    assert entry.utc_hour == datetime(2025, 1, 15, 13)
    assert entry.n_shares == 1


def test_skipped_spring_forward_hour_goes_to_the_next_hour():
    mappings = [m for m in local_hour_mapping(2025, 3, NEW_YORK) if m.local_hour.day == 9]
    by_local = {m.local_hour.hour: m.utc_hour for m in mappings}

    # 02:00 does not exist on 9 March 2025; 03:00 EDT is 07:00 UTC.
    assert by_local[2] == by_local[3] == datetime(2025, 3, 9, 7)
    assert by_local[1] == datetime(2025, 3, 9, 6)  # 01:00 EST


def test_repeated_fall_back_hour_is_split_between_two_utc_hours():
    mappings = [m for m in local_hour_mapping(2025, 11, NEW_YORK) if m.local_hour.day == 2]
    repeated = [m for m in mappings if m.local_hour.hour == 1]

    assert [m.utc_hour for m in repeated] == [datetime(2025, 11, 2, 5), datetime(2025, 11, 2, 6)]
    assert [(m.share, m.n_shares) for m in repeated] == [(0, 2), (1, 2)]


def test_every_utc_hour_of_the_month_receives_some_local_hour():
    for month in (1, 3, 11):
        mapped = {m.utc_hour for m in local_hour_mapping(2025, month, NEW_YORK)}

        assert mapped == set(utc_hours_of_local_month(2025, month, NEW_YORK))


def test_only_one_utc_hour_receives_two_local_hours_in_march():
    counts = Counter(m.utc_hour for m in local_hour_mapping(2025, 3, NEW_YORK))

    assert [hour for hour, n in counts.items() if n > 1] == [datetime(2025, 3, 9, 7)]


@pytest.mark.parametrize("count", [0, 1, 2, 3, 7, 10])
def test_split_count_preserves_the_total(count):
    parts = [split_count(count, share, 2) for share in range(2)]

    assert sum(parts) == count
    assert max(parts) - min(parts) <= 1
    assert parts[0] >= parts[1]
