"""Shared fixtures."""

import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    """Small local Spark session shared by the whole test run."""
    if shutil.which("java") is None:
        pytest.skip("Java is not installed: Spark tests need Java 17 or later")
    from taxi_forecast.pipeline.spark import get_spark

    session = get_spark("tests", master="local[2]", driver_memory="1g", shuffle_partitions=2)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
