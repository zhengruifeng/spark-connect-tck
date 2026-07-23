"""Wire-level TCK cases that send generated protobuf requests directly."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest

if TYPE_CHECKING:
    from tests.conftest import RawSparkConnectSession


pytestmark = pytest.mark.tck


def _range_plan(proto: Any) -> Any:
    """Build a minimal Relation.Range plan without using a DataFrame client."""
    plan = proto.Plan()
    plan.root.range.start = 0
    plan.root.range.end = 2
    plan.root.range.step = 1
    plan.root.range.num_partitions = 1
    return plan


def _assert_response_identity(responses: Iterable[Any], session_id: str, operation_id: str) -> None:
    """Check the protocol identity fields carried by every stream response."""
    for response in responses:
        assert response.session_id == session_id
        assert response.operation_id == operation_id
        assert response.server_side_session_id
        UUID(response.server_side_session_id)
        UUID(response.response_id)


def _decode_arrow_rows(arrow_responses: Iterable[Any]) -> list[int]:
    """Decode Arrow IPC data and validate optional response chunking metadata."""
    import pyarrow as pa

    rows: list[int] = []
    chunks: list[bytes] = []
    expected_chunks: int | None = None

    for response in arrow_responses:
        arrow_batch = response.arrow_batch
        chunk_index = arrow_batch.chunk_index
        if chunks:
            assert chunk_index == len(chunks)
        else:
            assert chunk_index == 0

        chunks.append(arrow_batch.data)
        if arrow_batch.HasField("num_chunks_in_batch"):
            assert arrow_batch.num_chunks_in_batch > 0
            if expected_chunks is None:
                expected_chunks = arrow_batch.num_chunks_in_batch
            assert arrow_batch.num_chunks_in_batch == expected_chunks

        complete = expected_chunks is None or len(chunks) == expected_chunks
        if not complete:
            continue

        with pa.ipc.open_stream(b"".join(chunks)) as reader:
            batches = list(reader)
        assert sum(batch.num_rows for batch in batches) == arrow_batch.row_count
        for batch in batches:
            assert batch.schema.names == ["id"]
            rows.extend(batch.column("id").to_pylist())
        chunks.clear()
        expected_chunks = None

    assert not chunks
    return rows


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-001")
def test_tck_wire_001_execute_plan_range_returns_arrow(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """ExecutePlan receives a hand-built Range plan, not a client-generated plan."""
    proto = raw_spark_connect.proto
    operation_id = str(uuid4())
    request = proto.ExecutePlanRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        operation_id=operation_id,
        client_type="spark-connect-tck",
        plan=_range_plan(proto),
    )

    responses = list(raw_spark_connect.stub.ExecutePlan(request, timeout=30))

    assert responses
    _assert_response_identity(responses, raw_spark_connect.session_id, operation_id)

    schema_responses = [response for response in responses if response.HasField("schema")]
    assert schema_responses
    schema = schema_responses[-1].schema
    assert schema.WhichOneof("kind") == "struct"
    assert [(field.name, field.data_type.WhichOneof("kind")) for field in schema.struct.fields] == [
        ("id", "long")
    ]

    arrow_responses = [response for response in responses if response.HasField("arrow_batch")]
    assert arrow_responses
    assert _decode_arrow_rows(arrow_responses) == [0, 1]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-002")
def test_tck_wire_002_analyze_plan_schema_is_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """AnalyzePlan receives a hand-built Schema request and returns a Struct schema."""
    proto = raw_spark_connect.proto
    request = proto.AnalyzePlanRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        client_type="spark-connect-tck",
    )
    request.schema.plan.CopyFrom(_range_plan(proto))

    response = raw_spark_connect.stub.AnalyzePlan(request, timeout=30)

    assert response.session_id == raw_spark_connect.session_id
    assert response.server_side_session_id
    UUID(response.server_side_session_id)
    assert response.WhichOneof("result") == "schema"
    schema = response.schema.schema
    assert schema.WhichOneof("kind") == "struct"
    assert [(field.name, field.data_type.WhichOneof("kind")) for field in schema.struct.fields] == [
        ("id", "long")
    ]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-003")
def test_tck_wire_003_config_set_and_get_share_the_raw_session(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Config uses direct Set and Get requests against the same protocol session."""
    proto = raw_spark_connect.proto
    key = "spark.sql.session.timeZone"
    set_request = proto.ConfigRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        client_type="spark-connect-tck",
        operation=proto.ConfigRequest.Operation(
            set=proto.ConfigRequest.Set(pairs=[proto.KeyValue(key=key, value="UTC")])
        ),
    )

    set_response = raw_spark_connect.stub.Config(set_request, timeout=30)
    get_request = proto.ConfigRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        client_type="spark-connect-tck",
        client_observed_server_side_session_id=set_response.server_side_session_id,
        operation=proto.ConfigRequest.Operation(get=proto.ConfigRequest.Get(keys=[key])),
    )
    get_response = raw_spark_connect.stub.Config(get_request, timeout=30)

    for response in (set_response, get_response):
        assert response.session_id == raw_spark_connect.session_id
        assert response.server_side_session_id
        UUID(response.server_side_session_id)
    assert get_response.server_side_session_id == set_response.server_side_session_id
    assert [(pair.key, pair.value) for pair in get_response.pairs] == [(key, "UTC")]
