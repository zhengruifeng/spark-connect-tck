"""Unit tests for specification traceability metadata."""

from __future__ import annotations

import pytest

from spark_connect_tck.spec import (
    CASES,
    CASES_BY_ID,
    IMPLEMENTED_WIRE_RPCS,
    REFERENCE_SPARK_VERSION,
    REQUIRED_WIRE_RPCS,
    get_case,
)


def test_starter_cases_have_unique_ids_and_manifest_rows() -> None:
    assert len(CASES) == len(CASES_BY_ID)
    assert {case.manifest for case in CASES} == {"SC-1.0-P1-WIRE", "SC-1.0-P1-SQL"}
    assert all(case.rows for case in CASES)


def test_get_case_returns_registered_case() -> None:
    assert get_case("TCK-SQL-001").rows[0] == "SQL-QRY-WITH"
    assert REFERENCE_SPARK_VERSION == "4.2.0"


def test_get_case_rejects_unknown_id() -> None:
    with pytest.raises(KeyError, match="Unknown Spark Connect TCK case"):
        get_case("TCK-NOT-REAL")


def test_direct_wire_cases_cover_the_required_service_request_inventory() -> None:
    assert IMPLEMENTED_WIRE_RPCS == REQUIRED_WIRE_RPCS
