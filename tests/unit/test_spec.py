"""Unit tests for specification traceability metadata."""

from __future__ import annotations

import pytest

from spark_connect_tck.spec import (
    CASES,
    CASES_BY_ID,
    DEFERRED_WIRE_RPCS,
    IMPLEMENTED_WIRE_RPCS,
    OPTIONAL_WIRE_RPCS,
    REFERENCE_SPARK_VERSION,
    REQUIRED_WIRE_RPCS,
    SPECIFICATION_VERSION,
    TckCase,
    get_case,
)


def test_starter_cases_have_unique_ids_and_manifest_rows() -> None:
    assert len(CASES) == len(CASES_BY_ID)
    assert {case.manifest for case in CASES} == {
        "SC-1.0-P1-ARROW",
        "SC-1.0-P1-EXPRESSION-SYNTAX",
        "SC-1.0-P1-FUNCTIONS",
        "SC-1.0-P1-PORTABLE-SQL",
        "SC-1.0-P1-WIRE",
    }
    assert all(case.rows for case in CASES)


def test_get_case_returns_registered_case() -> None:
    assert get_case("TCK-WIRE-002").rows[0] == "gRPC RPCs / AnalyzePlan"
    assert SPECIFICATION_VERSION == "1.0 draft v0.37"
    assert REFERENCE_SPARK_VERSION == "4.2.0"


def test_get_case_rejects_unknown_id() -> None:
    with pytest.raises(KeyError, match="Unknown Spark Connect TCK case"):
        get_case("TCK-NOT-REAL")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"case_id": "not-a-case"}, "Invalid TCK case ID"),
        ({"manifest": "SC-2.0-P1-WIRE"}, "must cite an SC-1.0-P1 manifest"),
        ({"rows": ()}, "must cite at least one manifest row"),
        ({"rpc_methods": ("NotAnRpc",)}, "cites unknown RPCs"),
    ],
)
def test_tck_case_rejects_invalid_traceability_metadata(
    overrides: dict[str, object], message: str
) -> None:
    values: dict[str, object] = {
        "case_id": "TCK-UNIT-001",
        "title": "Valid case used to exercise validation.",
        "manifest": "SC-1.0-P1-WIRE",
        "rows": ("Rows / Valid",),
        "rpc_methods": (),
    }

    with pytest.raises(ValueError, match=message):
        TckCase(**(values | overrides))


def test_direct_wire_cases_cover_the_required_service_request_inventory() -> None:
    assert IMPLEMENTED_WIRE_RPCS == REQUIRED_WIRE_RPCS
    assert REQUIRED_WIRE_RPCS.isdisjoint(OPTIONAL_WIRE_RPCS | DEFERRED_WIRE_RPCS)
