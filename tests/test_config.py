from pathlib import Path

import pytest
import yaml

from taxi_forecast.config import DEFAULT_CONFIG_PATH, ProjectConfig, load_config


@pytest.fixture
def raw_config() -> dict:
    with DEFAULT_CONFIG_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_default_config_loads():
    config = load_config()

    assert config.forecast.horizon_hours == 24
    assert config.forecast.quantiles == (0.1, 0.5, 0.9)
    assert config.data.excluded_zone_ids == (264, 265)


def test_months_cover_the_whole_period_in_order():
    config = load_config()

    months = config.data.months()

    assert months[0] == (2023, 1)
    assert months[-1] == (2026, 8)
    assert len(months) == 44
    assert months == sorted(months)
    assert (2023, 12) in months
    assert (2024, 1) in months


def test_tlc_url_is_zero_padded():
    config = load_config()

    assert config.data.tlc_url(2026, 8) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-08.parquet"
    )


@pytest.mark.parametrize(
    "quantiles",
    [
        [0.9, 0.5, 0.1],  # not increasing
        [0.1, 0.9],  # no median
        [0.0, 0.5, 1.0],  # outside (0, 1)
        [0.1, 0.5, 0.5, 0.9],  # duplicate
    ],
)
def test_invalid_quantiles_are_rejected(raw_config, quantiles):
    raw_config["forecast"]["quantiles"] = quantiles

    with pytest.raises(ValueError, match="quantiles"):
        ProjectConfig.from_dict(raw_config)


@pytest.mark.parametrize("month", ["2023-13", "23-01", "2023-1", "January 2023"])
def test_malformed_month_is_rejected(raw_config, month):
    raw_config["data"]["start_month"] = month

    with pytest.raises(ValueError, match="YYYY-MM"):
        ProjectConfig.from_dict(raw_config)


def test_period_must_not_be_reversed(raw_config):
    raw_config["data"]["start_month"] = "2026-09"

    with pytest.raises(ValueError, match="after end_month"):
        ProjectConfig.from_dict(raw_config)


def test_load_config_accepts_an_explicit_path(tmp_path: Path, raw_config):
    raw_config["forecast"]["horizon_hours"] = 168
    custom = tmp_path / "custom.yaml"
    custom.write_text(yaml.safe_dump(raw_config), encoding="utf-8")

    assert load_config(custom).forecast.horizon_hours == 168
