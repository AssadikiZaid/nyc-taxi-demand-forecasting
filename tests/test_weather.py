from datetime import UTC, datetime

import pandas as pd
import pytest
import requests

from taxi_forecast.config import load_config
from taxi_forecast.ingestion.weather import (
    WeatherNotAvailableError,
    download_months,
    expected_hours,
    fetch_month,
)
from taxi_forecast.paths import raw_weather_file

CONFIG = load_config()
NEW_YORK = CONFIG.data.timezone


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.content = b"x"
        self.text = str(payload)

    def json(self):
        return self.payload


class FakeArchive:
    """Answers like the Open-Meteo archive: every hour of the requested UTC dates."""

    def __init__(self, missing_after=None, status_code=200):
        self.missing_after = missing_after
        self.status_code = status_code
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        if self.status_code != 200:
            return FakeResponse({"error": True, "reason": "Parameter is wrong"}, 400)
        days = pd.date_range(params["start_date"], params["end_date"], freq="D")
        times = pd.date_range(params["start_date"], periods=24 * len(days), freq="h", tz="UTC")
        hourly = {"time": [int(t.timestamp()) for t in times]}
        for i, name in enumerate(params["hourly"].split(",")):
            values = [float(i + k % 24) for k in range(len(times))]
            if self.missing_after is not None:
                pairs = zip(times, values, strict=True)
                values = [None if t >= self.missing_after else v for t, v in pairs]
            hourly[name] = values
        return FakeResponse({"hourly": hourly})


def test_month_holds_the_utc_hours_of_the_new_york_month():
    frame = fetch_month(FakeArchive(), CONFIG.weather, 2025, 11, NEW_YORK)

    assert len(frame) == 721  # one more hour: clocks fall back on 2 November
    assert frame["hour_utc"].iloc[0] == pd.Timestamp("2025-11-01 04:00", tz="UTC")  # 00:00 EDT
    assert frame["hour_utc"].iloc[-1] == pd.Timestamp("2025-12-01 04:00", tz="UTC")  # 23:00 EST
    assert list(frame.columns) == ["hour_utc", *CONFIG.weather.hourly_variables]
    assert frame["weather_code"].dtype == "int16"


def test_request_asks_for_utc_unix_times():
    archive = FakeArchive()
    fetch_month(archive, CONFIG.weather, 2025, 1, NEW_YORK)

    params = archive.calls[0]
    assert (params["start_date"], params["end_date"]) == ("2025-01-01", "2025-02-01")
    assert (params["timezone"], params["timeformat"]) == ("GMT", "unixtime")
    assert params["latitude"] == CONFIG.weather.latitude


def test_month_not_yet_in_the_archive_is_refused():
    archive = FakeArchive(missing_after=pd.Timestamp("2026-08-28", tz="UTC"))

    with pytest.raises(WeatherNotAvailableError, match="archive lags"):
        fetch_month(archive, CONFIG.weather, 2026, 8, NEW_YORK)


def test_api_error_shows_the_reason():
    with pytest.raises(requests.HTTPError, match="Parameter is wrong"):
        fetch_month(FakeArchive(status_code=400), CONFIG.weather, 2025, 1, NEW_YORK)


def test_download_is_idempotent_and_completes_partial_files(tmp_path):
    archive = FakeArchive()

    first = download_months([(2025, 1), (2025, 2)], CONFIG, tmp_path, archive)
    second = download_months([(2025, 1), (2025, 2)], CONFIG, tmp_path, archive)

    assert [r.status for r in first] == ["downloaded", "downloaded"]
    assert [r.status for r in second] == ["skipped", "skipped"]
    assert len(archive.calls) == 2

    path = raw_weather_file(tmp_path, 2025, 2)
    pd.read_parquet(path).iloc[:-5].to_parquet(path, index=False)  # an interrupted month
    third = download_months([(2025, 2)], CONFIG, tmp_path, archive)
    assert third[0].status == "downloaded"
    assert len(pd.read_parquet(path)) == 672


def test_expected_hours_are_timezone_aware():
    hours = expected_hours(2025, 3, NEW_YORK)

    assert len(hours) == 743
    assert hours[0] == pd.Timestamp(datetime(2025, 3, 1, 5, tzinfo=UTC))
