"""Pytest configuration and fixtures shared by Spark Connect TCK cases."""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from spark_connect_tck.deployment import DeploymentContractError, DeploymentDescriptor
from spark_connect_tck.spec import CASES_BY_ID

if TYPE_CHECKING:
    from typing import Any

    from pyspark.sql.connect.session import SparkSession


@dataclass(frozen=True)
class RawSparkConnectSession:
    """Direct gRPC access to one isolated Spark Connect server session."""

    proto: Any
    stub: Any
    session_id: str
    user_context: Any


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("spark-connect-tck")
    group.addoption(
        "--spark-connect-url",
        action="store",
        default=os.environ.get("SPARK_CONNECT_URL"),
        metavar="URL",
        help="Spark Connect URL; defaults to the SPARK_CONNECT_URL environment variable.",
    )
    group.addoption(
        "--deployment-descriptor",
        action="store",
        default=os.environ.get("SPARK_CONNECT_TCK_DEPLOYMENT_DESCRIPTOR"),
        metavar="PATH",
        help="SC-TCK-DEPLOYMENT-1 descriptor for controlled lifecycle cases.",
    )
    group.addoption(
        "--deployment-adapter",
        action="store",
        default=os.environ.get("SPARK_CONNECT_TCK_DEPLOYMENT_ADAPTER"),
        metavar="PATH",
        help="SC-TCK-ADAPTER-1 executable for controlled lifecycle actions.",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "tck_case(case_id): trace a core TCK test to a registered SC-1.0-P1 draft case",
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
def deployment_descriptor(pytestconfig: pytest.Config) -> DeploymentDescriptor | None:
    """Load and validate the optional run-specific deployment descriptor."""
    path = pytestconfig.getoption("deployment_descriptor")
    if not path:
        return None
    try:
        return DeploymentDescriptor.from_path(path)
    except (DeploymentContractError, OSError) as error:
        raise pytest.UsageError(f"Invalid deployment descriptor {path!r}: {error}") from error


@pytest.fixture(scope="session")
def deployment_adapter_path(pytestconfig: pytest.Config) -> Path | None:
    """Return the local adapter executable supplied for controlled lifecycle cases."""
    path = pytestconfig.getoption("deployment_adapter")
    return Path(path) if path else None


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
    session = spark.cloneSession()
    # Spark 4.2 cloneSession bypasses __init__ and omits this stop() attribute.
    session.release_session_on_close = True
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def raw_spark_connect(spark_connect_url: str) -> Iterator[RawSparkConnectSession]:
    """Open a generated gRPC stub without constructing a SparkSession client.

    The URL channel builder is used only to preserve Spark Connect endpoint, TLS,
    and token handling.  TCK tests using this fixture construct every protobuf
    request and invoke the generated service stub themselves.
    """
    from pyspark.sql.connect.client.core import DefaultChannelBuilder
    from pyspark.sql.connect.proto import base_pb2, base_pb2_grpc

    channel = DefaultChannelBuilder(spark_connect_url).toChannel()
    try:
        yield RawSparkConnectSession(
            proto=base_pb2,
            stub=base_pb2_grpc.SparkConnectServiceStub(channel),
            session_id=str(uuid4()),
            user_context=base_pb2.UserContext(user_id="spark-connect-tck-wire"),
        )
    finally:
        channel.close()
