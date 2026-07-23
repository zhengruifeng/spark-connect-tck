"""Wire-level TCK cases that send generated protobuf requests directly."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4
from zlib import crc32

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


def _decode_arrow_batches(arrow_responses: Iterable[Any]) -> list[Any]:
    """Decode Arrow IPC data and validate optional response chunking metadata."""
    import pyarrow as pa

    decoded_batches: list[Any] = []
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
        decoded_batches.extend(batches)
        chunks.clear()
        expected_chunks = None

    assert not chunks
    return decoded_batches


def _decode_arrow_rows(arrow_responses: Iterable[Any]) -> list[int]:
    """Decode the single ``id`` column returned by a direct Range plan."""
    rows: list[int] = []
    for batch in _decode_arrow_batches(arrow_responses):
        assert batch.schema.names == ["id"]
        rows.extend(batch.column("id").to_pylist())
    return rows


def _decode_arrow_tuples(
    arrow_responses: Iterable[Any], expected_columns: list[str]
) -> list[tuple[Any, ...]]:
    """Decode named Arrow columns into deterministic Python row tuples."""
    rows: list[tuple[Any, ...]] = []
    for batch in _decode_arrow_batches(arrow_responses):
        assert batch.schema.names == expected_columns
        rows.extend(zip(*(batch.column(name).to_pylist() for name in expected_columns)))
    return rows


def _attribute(expressions: Any, name: str) -> Any:
    """Build an unresolved attribute expression without using a Column client."""
    expression = expressions.Expression()
    expression.unresolved_attribute.unparsed_identifier = name
    return expression


def _long_literal(expressions: Any, value: int) -> Any:
    """Build an integer literal expression without using a Column client."""
    expression = expressions.Expression()
    expression.literal.long = value
    return expression


def _function(expressions: Any, name: str, *arguments: Any) -> Any:
    """Build a non-UDF unresolved function expression directly in protobuf."""
    expression = expressions.Expression()
    expression.unresolved_function.function_name = name
    expression.unresolved_function.arguments.extend(arguments)
    return expression


def _alias(expressions: Any, expression: Any, name: str) -> Any:
    """Build an aliased expression directly in protobuf."""
    alias = expressions.Expression()
    alias.alias.expr.CopyFrom(expression)
    alias.alias.name.append(name)
    return alias


def _range_relation(relations: Any, end: int) -> Any:
    """Build a Range relation that can be nested in a larger raw plan."""
    relation = relations.Relation()
    relation.range.start = 0
    relation.range.end = end
    relation.range.step = 1
    relation.range.num_partitions = 1
    return relation


def _execute_relation(raw_spark_connect: RawSparkConnectSession, relation: Any) -> list[Any]:
    """Execute one direct Relation plan and return the complete response stream."""
    proto = raw_spark_connect.proto
    operation_id = str(uuid4())
    request = proto.ExecutePlanRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        operation_id=operation_id,
        client_type="spark-connect-tck",
        plan=proto.Plan(root=relation),
    )
    responses = list(raw_spark_connect.stub.ExecutePlan(request, timeout=30))

    assert responses
    _assert_response_identity(responses, raw_spark_connect.session_id, operation_id)
    return responses


def _get_time_zone(raw_spark_connect: RawSparkConnectSession) -> Any:
    """Create the raw session, returning its Config response and server-side ID."""
    proto = raw_spark_connect.proto
    return raw_spark_connect.stub.Config(
        proto.ConfigRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            operation=proto.ConfigRequest.Operation(
                get=proto.ConfigRequest.Get(keys=["spark.sql.session.timeZone"])
            ),
        ),
        timeout=30,
    )


def _assert_unary_response_identity(
    response: Any, raw_spark_connect: RawSparkConnectSession
) -> None:
    """Check identity fields shared by all unary service responses."""
    assert response.session_id == raw_spark_connect.session_id
    assert response.server_side_session_id
    UUID(response.server_side_session_id)


def _execute_reattachable_range(
    raw_spark_connect: RawSparkConnectSession,
) -> tuple[str, list[Any]]:
    """Start and consume a reattachable Range operation through the raw stub."""
    proto = raw_spark_connect.proto
    operation_id = str(uuid4())
    request = proto.ExecutePlanRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        operation_id=operation_id,
        client_type="spark-connect-tck",
        plan=_range_plan(proto),
        request_options=[
            proto.ExecutePlanRequest.RequestOption(
                reattach_options=proto.ReattachOptions(reattachable=True)
            )
        ],
    )
    responses = list(raw_spark_connect.stub.ExecutePlan(request, timeout=30))

    assert responses
    _assert_response_identity(responses, raw_spark_connect.session_id, operation_id)
    assert any(response.HasField("result_complete") for response in responses)
    assert _decode_arrow_rows(
        response for response in responses if response.HasField("arrow_batch")
    ) == [0, 1]
    return operation_id, responses


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


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-004")
def test_tck_wire_004_artifact_upload_and_status_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """AddArtifacts and ArtifactStatus send and verify a hand-built artifact request."""
    proto = raw_spark_connect.proto
    artifact_name = f"cache/{uuid4().hex}"
    data = b"spark-connect-tck"
    add_request = proto.AddArtifactsRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        client_type="spark-connect-tck",
        batch=proto.AddArtifactsRequest.Batch(
            artifacts=[
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name=artifact_name,
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=crc32(data)),
                )
            ]
        ),
    )

    add_response = raw_spark_connect.stub.AddArtifacts(iter([add_request]), timeout=30)
    status_response = raw_spark_connect.stub.ArtifactStatus(
        proto.ArtifactStatusesRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            client_observed_server_side_session_id=add_response.server_side_session_id,
            names=[artifact_name],
        ),
        timeout=30,
    )

    _assert_unary_response_identity(add_response, raw_spark_connect)
    _assert_unary_response_identity(status_response, raw_spark_connect)
    assert add_response.server_side_session_id == status_response.server_side_session_id
    assert [(artifact.name, artifact.is_crc_successful) for artifact in add_response.artifacts] == [
        (artifact_name, True)
    ]
    assert status_response.statuses[artifact_name].exists


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-005")
def test_tck_wire_005_interrupt_and_get_status_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Interrupt and GetStatus use direct requests against an established idle session."""
    proto = raw_spark_connect.proto
    session_response = _get_time_zone(raw_spark_connect)
    interrupt_response = raw_spark_connect.stub.Interrupt(
        proto.InterruptRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            client_observed_server_side_session_id=session_response.server_side_session_id,
            interrupt_type=proto.InterruptRequest.INTERRUPT_TYPE_ALL,
        ),
        timeout=30,
    )
    status_response = raw_spark_connect.stub.GetStatus(
        proto.GetStatusRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            client_observed_server_side_session_id=interrupt_response.server_side_session_id,
            operation_status=proto.GetStatusRequest.OperationStatusRequest(),
        ),
        timeout=30,
    )

    _assert_unary_response_identity(interrupt_response, raw_spark_connect)
    _assert_unary_response_identity(status_response, raw_spark_connect)
    assert interrupt_response.interrupted_ids == []
    assert list(status_response.operation_statuses) == []


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-006")
def test_tck_wire_006_reattach_and_release_execute_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """ReattachExecute repeats buffered results and ReleaseExecute frees the operation."""
    proto = raw_spark_connect.proto
    operation_id, initial_responses = _execute_reattachable_range(raw_spark_connect)
    reattach_responses = list(
        raw_spark_connect.stub.ReattachExecute(
            proto.ReattachExecuteRequest(
                session_id=raw_spark_connect.session_id,
                user_context=raw_spark_connect.user_context,
                client_type="spark-connect-tck",
                client_observed_server_side_session_id=initial_responses[-1].server_side_session_id,
                operation_id=operation_id,
            ),
            timeout=30,
        )
    )
    release_response = raw_spark_connect.stub.ReleaseExecute(
        proto.ReleaseExecuteRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            client_observed_server_side_session_id=initial_responses[-1].server_side_session_id,
            operation_id=operation_id,
            release_all=proto.ReleaseExecuteRequest.ReleaseAll(),
        ),
        timeout=30,
    )

    assert reattach_responses
    _assert_response_identity(reattach_responses, raw_spark_connect.session_id, operation_id)
    assert _decode_arrow_rows(
        response for response in reattach_responses if response.HasField("arrow_batch")
    ) == [0, 1]
    _assert_unary_response_identity(release_response, raw_spark_connect)
    assert release_response.operation_id == operation_id


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-007")
def test_tck_wire_007_release_session_is_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """ReleaseSession directly releases a session that this test has established."""
    proto = raw_spark_connect.proto
    session_response = _get_time_zone(raw_spark_connect)
    release_response = raw_spark_connect.stub.ReleaseSession(
        proto.ReleaseSessionRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
        ),
        timeout=30,
    )

    _assert_unary_response_identity(session_response, raw_spark_connect)
    _assert_unary_response_identity(release_response, raw_spark_connect)
    assert release_response.server_side_session_id == session_response.server_side_session_id


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-008")
def test_tck_wire_008_fetch_unknown_error_details_is_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """FetchErrorDetails responds precisely when a valid but unknown ID is requested."""
    proto = raw_spark_connect.proto
    session_response = _get_time_zone(raw_spark_connect)
    response = raw_spark_connect.stub.FetchErrorDetails(
        proto.FetchErrorDetailsRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            client_observed_server_side_session_id=session_response.server_side_session_id,
            error_id=str(uuid4()),
        ),
        timeout=30,
    )

    # The service returns the default protobuf response when the ID is absent;
    # neither the session identity nor an error envelope is populated.
    assert response.session_id == ""
    assert response.server_side_session_id == ""
    assert not response.HasField("root_error_idx")
    assert list(response.errors) == []


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-009")
def test_tck_wire_009_clone_session_is_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """CloneSession directly copies configuration into a caller-supplied session ID."""
    proto = raw_spark_connect.proto
    key = "spark.sql.session.timeZone"
    set_response = raw_spark_connect.stub.Config(
        proto.ConfigRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            operation=proto.ConfigRequest.Operation(
                set=proto.ConfigRequest.Set(pairs=[proto.KeyValue(key=key, value="UTC")])
            ),
        ),
        timeout=30,
    )
    cloned_session_id = str(uuid4())
    clone_response = raw_spark_connect.stub.CloneSession(
        proto.CloneSessionRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            client_observed_server_side_session_id=set_response.server_side_session_id,
            new_session_id=cloned_session_id,
        ),
        timeout=30,
    )
    clone_config_response = raw_spark_connect.stub.Config(
        proto.ConfigRequest(
            session_id=cloned_session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            client_observed_server_side_session_id=clone_response.new_server_side_session_id,
            operation=proto.ConfigRequest.Operation(get=proto.ConfigRequest.Get(keys=[key])),
        ),
        timeout=30,
    )

    _assert_unary_response_identity(set_response, raw_spark_connect)
    _assert_unary_response_identity(clone_response, raw_spark_connect)
    assert clone_response.new_session_id == cloned_session_id
    assert clone_response.new_server_side_session_id
    UUID(clone_response.new_server_side_session_id)
    assert clone_config_response.session_id == cloned_session_id
    assert clone_config_response.server_side_session_id == clone_response.new_server_side_session_id
    assert [(pair.key, pair.value) for pair in clone_config_response.pairs] == [(key, "UTC")]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-010")
def test_tck_wire_010_basic_relations_and_expressions_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Build Range, Filter, Project, Sort, and Limit relation messages by hand."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    filtered = relations_pb2.Relation()
    filtered.filter.input.CopyFrom(_range_relation(relations_pb2, end=8))
    filtered.filter.condition.CopyFrom(
        _function(
            expressions_pb2,
            ">",
            _attribute(expressions_pb2, "id"),
            _long_literal(expressions_pb2, 1),
        )
    )

    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(filtered)
    projected.project.expressions.extend(
        [
            _attribute(expressions_pb2, "id"),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "+",
                    _attribute(expressions_pb2, "id"),
                    _long_literal(expressions_pb2, 10),
                ),
                "value",
            ),
        ]
    )

    sorted_relation = relations_pb2.Relation()
    sorted_relation.sort.input.CopyFrom(projected)
    sorted_relation.sort.order.append(
        expressions_pb2.Expression.SortOrder(
            child=_attribute(expressions_pb2, "id"),
            direction=expressions_pb2.Expression.SortOrder.SORT_DIRECTION_DESCENDING,
            null_ordering=expressions_pb2.Expression.SortOrder.SORT_NULLS_LAST,
        )
    )
    sorted_relation.sort.is_global = True

    limited = relations_pb2.Relation()
    limited.limit.input.CopyFrom(sorted_relation)
    limited.limit.limit = 3

    responses = _execute_relation(raw_spark_connect, limited)

    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")), ["id", "value"]
    ) == [(7, 17), (6, 16), (5, 15)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-011")
def test_tck_wire_011_basic_aggregate_expressions_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Build Range, Project, Aggregate, and Sort relation messages by hand."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(_range_relation(relations_pb2, end=6))
    projected.project.expressions.extend(
        [
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "%",
                    _attribute(expressions_pb2, "id"),
                    _long_literal(expressions_pb2, 2),
                ),
                "bucket",
            ),
            _attribute(expressions_pb2, "id"),
        ]
    )

    aggregated = relations_pb2.Relation()
    aggregated.aggregate.input.CopyFrom(projected)
    aggregated.aggregate.group_type = relations_pb2.Aggregate.GROUP_TYPE_GROUPBY
    aggregated.aggregate.grouping_expressions.append(_attribute(expressions_pb2, "bucket"))
    aggregated.aggregate.aggregate_expressions.extend(
        [
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "sum", _attribute(expressions_pb2, "id")),
                "total",
            ),
        ]
    )

    sorted_relation = relations_pb2.Relation()
    sorted_relation.sort.input.CopyFrom(aggregated)
    sorted_relation.sort.order.append(
        expressions_pb2.Expression.SortOrder(
            child=_attribute(expressions_pb2, "bucket"),
            direction=expressions_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING,
            null_ordering=expressions_pb2.Expression.SortOrder.SORT_NULLS_LAST,
        )
    )
    sorted_relation.sort.is_global = True

    responses = _execute_relation(raw_spark_connect, sorted_relation)

    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        ["bucket", "total"],
    ) == [(0, 6), (1, 9)]
