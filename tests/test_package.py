import importlib

import pytest

import taxi_forecast

SUBPACKAGES = [
    "ingestion",
    "pipeline",
    "features",
    "models",
    "evaluation",
    "monitoring",
    "serving",
]


def test_package_exposes_a_version():
    assert isinstance(taxi_forecast.__version__, str)
    assert taxi_forecast.__version__ != ""


@pytest.mark.parametrize("name", SUBPACKAGES)
def test_subpackages_are_importable(name):
    module = importlib.import_module(f"taxi_forecast.{name}")

    assert module.__doc__, f"taxi_forecast.{name} should describe its role in a docstring"
