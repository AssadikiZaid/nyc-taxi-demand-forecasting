import pytest

from taxi_forecast.pipeline import spark as spark_module


@pytest.mark.parametrize(
    ("total_mb", "expected"),
    [
        (4096, "1638m"),  # WSL2 on an 8 GB laptop gets about 4 GB
        (6144, "2457m"),
        (2048, "1024m"),  # never below 1 GB
        (32768, "3072m"),  # never above 3 GB
        (None, "2g"),  # memory unknown
    ],
)
def test_driver_memory_follows_the_machine(monkeypatch, total_mb, expected):
    monkeypatch.delenv("SPARK_DRIVER_MEMORY", raising=False)
    monkeypatch.setattr(spark_module, "total_memory_mb", lambda: total_mb)

    assert spark_module.default_driver_memory() == expected


def test_environment_variable_wins(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_MEMORY", "5g")
    monkeypatch.setattr(spark_module, "total_memory_mb", lambda: 4096)

    assert spark_module.default_driver_memory() == "5g"


def test_total_memory_is_detected_on_linux():
    assert spark_module.total_memory_mb() > 0
