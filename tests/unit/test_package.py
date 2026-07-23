"""Tests that do not require a live Spark Connect service."""

from spark_connect_tck import __version__


def test_version_is_defined() -> None:
    assert __version__ == "0.1.0"
