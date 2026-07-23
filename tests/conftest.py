"""Pytest configuration and fixtures shared by Spark Connect TCK cases."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

from spark_connect_tck.spec import CASES_BY_ID

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("spark-connect-tck")
    group.addoption(
        "--spark-connect-url",
        action="store",
        default=os.environ.get("SPARK_CONNECT_URL"),
        metavar="URL",
        help="Spark Connect URL; defaults to the SPARK_CONNECT_URL environment variable.",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "tck_case(case_id): trace a TCK test to a registered SC-1.0-P1 starter case",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Reject untraceable live-target cases before a target is contacted."""
    del config
    for item in items:
        if "tck" not in item.keywords:
            continue
        marker = item.get_closest_marker("tck_case")
        if marker is None or len(marker.args) != 1 or marker.kwargs:
            raise pytest.UsageError(
                f"{item.nodeid} is a TCK case and must use @pytest.mark.tck_case('TCK-AREA-NNN')"
            )
        case_id = marker.args[0]
        if not isinstance(case_id, str) or case_id not in CASES_BY_ID:
            raise pytest.UsageError(
                f"{item.nodeid} cites unknown TCK case {case_id!r}; "
                "add it to spark_connect_tck.spec"
            )


@pytest.fixture(scope="session")
def spark_connect_url(pytestconfig: pytest.Config) -> str:
    """Return the explicit target URL, skipping TCK cases when none was supplied."""
    url = pytestconfig.getoption("spark_connect_url")
    if not url:
        pytest.skip(
            "No target supplied. Set SPARK_CONNECT_URL or pass --spark-connect-url "
            "to run Spark Connect TCK cases."
        )
    return url


@pytest.fixture(scope="session")
def spark(spark_connect_url: str) -> Iterator[SparkSession]:
    """Create one remote client session for the test target and close it afterward."""
    from pyspark.sql import SparkSession

    session = SparkSession.builder.remote(spark_connect_url).getOrCreate()
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def isolated_spark(spark: SparkSession) -> Iterator[SparkSession]:
    """Return a short-lived independent session for stateful conformance cases."""
    session = spark.newSession()
    try:
        yield session
    finally:
        session.stop()
