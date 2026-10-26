from datetime import date

import pandas as pd
import pytest

from taxi_forecast.features.calendar import build_calendar, holidays_in, main, next_month
from taxi_forecast.paths import calendar_file

NEW_YORK = "America/New_York"


@pytest.fixture(scope="module")
def year_2025():
    return build_calendar("2025-01", "2025-12", NEW_YORK)


def day(calendar, value):
    return calendar[calendar["local_date"] == date.fromisoformat(value)]


def test_one_row_per_utc_hour_without_gaps(year_2025):
    hours = year_2025["hour_utc"]

    assert len(year_2025) == 8760
    assert hours.is_unique
    expected = pd.date_range("2025-01-01 05:00", periods=8760, freq="h", tz="UTC")
    assert (pd.DatetimeIndex(hours) == expected).all()


def test_local_fields(year_2025):
    first = year_2025.iloc[0]

    assert (first["local_date"], first["local_hour"]) == (date(2025, 1, 1), 0)
    assert first["day_of_week"] == 2  # a Wednesday
    assert bool(day(year_2025, "2025-11-01")["is_weekend"].all())  # a Saturday
    assert not day(year_2025, "2025-11-03")["is_weekend"].any()


def test_holidays_and_their_neighbours(year_2025):
    thanksgiving = day(year_2025, "2025-11-27")

    assert thanksgiving["holiday"].eq("Thanksgiving Day").all()
    assert day(year_2025, "2025-11-26")["is_day_before_holiday"].all()
    assert day(year_2025, "2025-11-28")["is_day_after_holiday"].all()
    assert not day(year_2025, "2025-11-28")["is_holiday"].any()
    assert len(holidays_in(year_2025)) == 11


def test_observed_holiday_and_state_holidays():
    july_2026 = build_calendar("2026-07", "2026-07", NEW_YORK)
    november = build_calendar("2025-11", "2025-11", NEW_YORK)

    assert day(july_2026, "2026-07-03")["holiday"].eq("Independence Day (observed)").all()
    election_day = day(november, "2025-11-04")
    assert election_day["is_ny_state_holiday"].all()
    assert not election_day["is_holiday"].any()


def test_clock_change_days(year_2025):
    spring, fall = day(year_2025, "2025-03-09"), day(year_2025, "2025-11-02")

    assert (len(spring), len(fall)) == (23, 25)
    assert spring["is_clock_change_day"].all()
    assert fall["is_clock_change_day"].all()
    assert not day(year_2025, "2025-03-10")["is_clock_change_day"].any()
    assert fall["local_hour"].tolist().count(1) == 2  # 01:00 happens twice
    assert set(year_2025["utc_offset_hours"]) == {-5, -4}
    assert day(year_2025, "2025-07-01")["utc_offset_hours"].eq(-4).all()


def test_next_month():
    assert next_month("2026-08") == "2026-09"
    assert next_month("2026-12") == "2027-01"


def test_command_line_writes_the_table(tmp_path, capsys):
    main(["--start", "2025-12", "--end", "2026-01", "--data-dir", str(tmp_path)])

    calendar = pd.read_parquet(calendar_file(tmp_path))
    assert len(calendar) == 31 * 24 * 2
    # Christmas, New Year's Day and Martin Luther King Jr. Day
    assert "3 federal holiday days" in capsys.readouterr().out
