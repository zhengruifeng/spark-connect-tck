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


def _long_literal_value(expressions: Any, value: int) -> Any:
    """Build an integer literal value for relation fields that require one."""
    literal = expressions.Expression.Literal()
    literal.long = value
    return literal


def _string_literal_value(expressions: Any, value: str) -> Any:
    """Build a string literal value for relation fields that require one."""
    literal = expressions.Expression.Literal()
    literal.string = value
    return literal


def _function(expressions: Any, name: str, *arguments: Any) -> Any:
    """Build a non-UDF unresolved function expression directly in protobuf."""
    expression = expressions.Expression()
    expression.unresolved_function.function_name = name
    expression.unresolved_function.arguments.extend(arguments)
    return expression


def _alias(expressions: Any, expression: Any, name: str) -> Any:
    """Build an aliased expression directly in protobuf."""
    result = expressions.Expression()
    result.alias.CopyFrom(_alias_value(expressions, expression, name))
    return result


def _alias_value(expressions: Any, expression: Any, name: str) -> Any:
    """Build an alias value for relation fields that require an Alias message."""
    alias = expressions.Expression.Alias()
    alias.expr.CopyFrom(expression)
    alias.name.append(name)
    return alias


def _range_relation(relations: Any, end: int, start: int = 0) -> Any:
    """Build a Range relation that can be nested in a larger raw plan."""
    relation = relations.Relation()
    relation.range.start = start
    relation.range.end = end
    relation.range.step = 1
    relation.range.num_partitions = 1
    return relation


def _sorted_relation(
    relations: Any, expressions: Any, input_relation: Any, columns: list[str]
) -> Any:
    """Sort a relation by named columns so result assertions do not rely on physical order."""
    relation = relations.Relation()
    relation.sort.input.CopyFrom(input_relation)
    relation.sort.order.extend(
        expressions.Expression.SortOrder(
            child=_attribute(expressions, column),
            direction=expressions.Expression.SortOrder.SORT_DIRECTION_ASCENDING,
            null_ordering=expressions.Expression.SortOrder.SORT_NULLS_LAST,
        )
        for column in columns
    )
    relation.sort.is_global = True
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


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-012")
def test_tck_wire_012_join_and_set_operations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Build Join and SetOperation relation messages without DataFrame APIs."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    joined = relations_pb2.Relation()
    joined.join.left.CopyFrom(_range_relation(relations_pb2, end=3))
    joined.join.right.CopyFrom(_range_relation(relations_pb2, start=1, end=4))
    joined.join.using_columns.append("id")
    joined.join.join_type = relations_pb2.Join.JOIN_TYPE_INNER

    sorted_join = relations_pb2.Relation()
    sorted_join.sort.input.CopyFrom(joined)
    sorted_join.sort.order.append(
        expressions_pb2.Expression.SortOrder(
            child=_attribute(expressions_pb2, "id"),
            direction=expressions_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING,
            null_ordering=expressions_pb2.Expression.SortOrder.SORT_NULLS_LAST,
        )
    )
    sorted_join.sort.is_global = True

    unioned = relations_pb2.Relation()
    unioned.set_op.left_input.CopyFrom(_range_relation(relations_pb2, end=3))
    unioned.set_op.right_input.CopyFrom(_range_relation(relations_pb2, start=2, end=5))
    unioned.set_op.set_op_type = relations_pb2.SetOperation.SET_OP_TYPE_UNION
    unioned.set_op.is_all = False

    sorted_union = relations_pb2.Relation()
    sorted_union.sort.input.CopyFrom(unioned)
    sorted_union.sort.order.append(
        expressions_pb2.Expression.SortOrder(
            child=_attribute(expressions_pb2, "id"),
            direction=expressions_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING,
            null_ordering=expressions_pb2.Expression.SortOrder.SORT_NULLS_LAST,
        )
    )
    sorted_union.sort.is_global = True

    join_responses = _execute_relation(raw_spark_connect, sorted_join)
    union_responses = _execute_relation(raw_spark_connect, sorted_union)

    assert _decode_arrow_rows(
        response for response in join_responses if response.HasField("arrow_batch")
    ) == [1, 2]
    assert _decode_arrow_rows(
        response for response in union_responses if response.HasField("arrow_batch")
    ) == [0, 1, 2, 3, 4]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-013")
def test_tck_wire_013_offset_and_tail_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Build ordered Offset and Tail relation messages without DataFrame APIs."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    sorted_range = relations_pb2.Relation()
    sorted_range.sort.input.CopyFrom(_range_relation(relations_pb2, end=6))
    sorted_range.sort.order.append(
        expressions_pb2.Expression.SortOrder(
            child=_attribute(expressions_pb2, "id"),
            direction=expressions_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING,
            null_ordering=expressions_pb2.Expression.SortOrder.SORT_NULLS_LAST,
        )
    )
    sorted_range.sort.is_global = True

    offset = relations_pb2.Relation()
    offset.offset.input.CopyFrom(sorted_range)
    offset.offset.offset = 2

    tail = relations_pb2.Relation()
    tail.tail.input.CopyFrom(offset)
    tail.tail.limit = 2

    responses = _execute_relation(raw_spark_connect, tail)

    assert _decode_arrow_rows(
        response for response in responses if response.HasField("arrow_batch")
    ) == [4, 5]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-014")
def test_tck_wire_014_conditional_and_cast_expressions_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Build boolean, conditional, and cast expressions without Column APIs."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    filtered = relations_pb2.Relation()
    filtered.filter.input.CopyFrom(_range_relation(relations_pb2, end=3))
    filtered.filter.condition.CopyFrom(
        _function(
            expressions_pb2,
            "and",
            _function(
                expressions_pb2,
                ">=",
                _attribute(expressions_pb2, "id"),
                _long_literal(expressions_pb2, 0),
            ),
            _function(
                expressions_pb2,
                "<",
                _attribute(expressions_pb2, "id"),
                _long_literal(expressions_pb2, 3),
            ),
        )
    )

    cast_to_string = expressions_pb2.Expression()
    cast_to_string.cast.expr.CopyFrom(_attribute(expressions_pb2, "id"))
    cast_to_string.cast.type_str = "STRING"

    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(filtered)
    projected.project.expressions.extend(
        [
            _attribute(expressions_pb2, "id"),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "when",
                    _function(
                        expressions_pb2,
                        "==",
                        _attribute(expressions_pb2, "id"),
                        _long_literal(expressions_pb2, 1),
                    ),
                    _long_literal(expressions_pb2, 100),
                    _long_literal(expressions_pb2, -1),
                ),
                "marked",
            ),
            _alias(expressions_pb2, cast_to_string, "as_text"),
        ]
    )

    responses = _execute_relation(raw_spark_connect, projected)

    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        ["id", "marked", "as_text"],
    ) == [(0, -1, "0"), (1, 100, "1"), (2, -1, "2")]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-015")
def test_tck_wire_015_local_relation_and_na_relations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Send Arrow-backed LocalRelation, fill, drop, and replace plans directly."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    table = pa.table(
        {
            "label": pa.array([None, "keep", None]),
            "score": pa.array([None, 2, 2], type=pa.int64()),
        }
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    local = relations_pb2.Relation()
    local.local_relation.data = sink.getvalue().to_pybytes()

    filled = relations_pb2.Relation()
    filled.fill_na.input.CopyFrom(local)
    filled.fill_na.cols.extend(["label", "score"])
    filled.fill_na.values.extend(
        [
            _string_literal_value(expressions_pb2, "missing"),
            _long_literal_value(expressions_pb2, 0),
        ]
    )

    replaced = relations_pb2.Relation()
    replaced.replace.input.CopyFrom(filled)
    replaced.replace.cols.append("label")
    replacement = replaced.replace.replacements.add()
    replacement.old_value.CopyFrom(_string_literal_value(expressions_pb2, "missing"))
    replacement.new_value.CopyFrom(_string_literal_value(expressions_pb2, "replaced"))

    dropped = relations_pb2.Relation()
    dropped.drop_na.input.CopyFrom(local)
    dropped.drop_na.cols.append("label")

    filled_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, filled, ["label", "score"]),
    )
    replaced_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, replaced, ["label", "score"]),
    )
    dropped_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, dropped, ["label", "score"]),
    )

    assert _decode_arrow_tuples(
        (response for response in filled_responses if response.HasField("arrow_batch")),
        ["label", "score"],
    ) == [("keep", 2), ("missing", 0), ("missing", 2)]
    assert _decode_arrow_tuples(
        (response for response in replaced_responses if response.HasField("arrow_batch")),
        ["label", "score"],
    ) == [("keep", 2), ("replaced", 0), ("replaced", 2)]
    assert _decode_arrow_tuples(
        (response for response in dropped_responses if response.HasField("arrow_batch")),
        ["label", "score"],
    ) == [("keep", 2)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-016")
def test_tck_wire_016_column_mutation_and_deduplication_relations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Send WithColumns, rename, drop, and deduplication plans directly."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    table = pa.table(
        {
            "label": pa.array(["a", "a", "a", "b"]),
            "score": pa.array([1, 1, 3, 2], type=pa.int64()),
        }
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    local = relations_pb2.Relation()
    local.local_relation.data = sink.getvalue().to_pybytes()

    with_columns = relations_pb2.Relation()
    with_columns.with_columns.input.CopyFrom(local)
    with_columns.with_columns.aliases.extend(
        [
            _alias_value(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "+",
                    _attribute(expressions_pb2, "score"),
                    _long_literal(expressions_pb2, 10),
                ),
                "score",
            ),
            _alias_value(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "*",
                    _attribute(expressions_pb2, "score"),
                    _long_literal(expressions_pb2, 2),
                ),
                "double_score",
            ),
        ]
    )

    renamed = relations_pb2.Relation()
    renamed.with_columns_renamed.input.CopyFrom(with_columns)
    label_rename = renamed.with_columns_renamed.renames.add()
    label_rename.col_name = "label"
    label_rename.new_col_name = "kind"
    score_rename = renamed.with_columns_renamed.renames.add()
    score_rename.col_name = "score"
    score_rename.new_col_name = "score_plus"

    dropped = relations_pb2.Relation()
    dropped.drop.input.CopyFrom(renamed)
    dropped.drop.column_names.append("double_score")

    deduplicated_by_name = relations_pb2.Relation()
    deduplicated_by_name.deduplicate.input.CopyFrom(dropped)
    deduplicated_by_name.deduplicate.column_names.append("kind")

    deduplicated_by_row = relations_pb2.Relation()
    deduplicated_by_row.deduplicate.input.CopyFrom(renamed)
    deduplicated_by_row.deduplicate.all_columns_as_keys = True

    dropped_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, dropped, ["kind", "score_plus"]),
    )
    by_name_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, deduplicated_by_name, ["kind"]),
    )
    by_row_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(
            relations_pb2,
            expressions_pb2,
            deduplicated_by_row,
            ["kind", "score_plus"],
        ),
    )

    assert _decode_arrow_tuples(
        (response for response in dropped_responses if response.HasField("arrow_batch")),
        ["kind", "score_plus"],
    ) == [("a", 11), ("a", 11), ("a", 13), ("b", 12)]
    rows_by_name = _decode_arrow_tuples(
        (response for response in by_name_responses if response.HasField("arrow_batch")),
        ["kind", "score_plus"],
    )
    assert len(rows_by_name) == 2
    assert {kind for kind, _ in rows_by_name} == {"a", "b"}
    assert _decode_arrow_tuples(
        (response for response in by_row_responses if response.HasField("arrow_batch")),
        ["kind", "score_plus", "double_score"],
    ) == [("a", 11, 2), ("a", 13, 6), ("b", 12, 4)]
