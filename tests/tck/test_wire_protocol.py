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


def _result_schema(responses: Iterable[Any]) -> Any:
    """Return the final logical result schema carried by an ExecutePlan stream."""
    schemas = [response.schema for response in responses if response.HasField("schema")]
    assert schemas
    schema = schemas[-1]
    assert schema.WhichOneof("kind") == "struct"
    return schema.struct


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


def _int_literal(expressions: Any, value: int) -> Any:
    """Build a 32-bit integer literal expression without using a Column client."""
    expression = expressions.Expression()
    expression.literal.integer = value
    return expression


def _string_literal(expressions: Any, value: str) -> Any:
    """Build a string literal expression without using a Column client."""
    expression = expressions.Expression()
    expression.literal.string = value
    return expression


def _expression_string(expressions: Any, text: str) -> Any:
    """Build an ExpressionString node without using a Column expression parser."""
    expression = expressions.Expression()
    expression.expression_string.expression = text
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


def _local_relation(relations: Any, table: Any) -> Any:
    """Encode an Arrow table as a LocalRelation without a DataFrame client."""
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    relation = relations.Relation()
    relation.local_relation.data = sink.getvalue().to_pybytes()
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


def _assert_relation_rejected(raw_spark_connect: RawSparkConnectSession, relation: Any) -> None:
    """Require a direct relation to fail through the gRPC error transport."""
    import grpc

    with pytest.raises(grpc.RpcError) as caught:
        _execute_relation(raw_spark_connect, relation)

    assert caught.value.code() != grpc.StatusCode.OK
    assert caught.value.details()


def _execute_command(raw_spark_connect: RawSparkConnectSession, command: Any) -> list[Any]:
    """Execute one direct Command plan and return the complete response stream."""
    proto = raw_spark_connect.proto
    operation_id = str(uuid4())
    request = proto.ExecutePlanRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        operation_id=operation_id,
        client_type="spark-connect-tck",
        plan=proto.Plan(command=command),
    )
    responses = list(raw_spark_connect.stub.ExecutePlan(request, timeout=30))

    if responses:
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
def test_tck_wire_002_required_analyze_plan_operations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Exercise every AnalyzePlan operation required by the v0.20 wire profile."""
    proto = raw_spark_connect.proto
    request_fields = (
        "schema",
        "explain",
        "tree_string",
        "is_local",
        "is_streaming",
        "input_files",
    )
    requests: dict[str, Any] = {}
    for field in request_fields:
        request = proto.AnalyzePlanRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
        )
        getattr(request, field).plan.CopyFrom(_range_plan(proto))
        requests[field] = request
    requests["explain"].explain.explain_mode = proto.AnalyzePlanRequest.Explain.EXPLAIN_MODE_SIMPLE

    requests["spark_version"] = proto.AnalyzePlanRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        client_type="spark-connect-tck",
        spark_version=proto.AnalyzePlanRequest.SparkVersion(),
    )
    requests["ddl_parse"] = proto.AnalyzePlanRequest(
        session_id=raw_spark_connect.session_id,
        user_context=raw_spark_connect.user_context,
        client_type="spark-connect-tck",
        ddl_parse=proto.AnalyzePlanRequest.DDLParse(ddl_string="id BIGINT"),
    )

    responses = {
        field: raw_spark_connect.stub.AnalyzePlan(request, timeout=30)
        for field, request in requests.items()
    }
    server_session_ids = set()
    for field, response in responses.items():
        assert response.session_id == raw_spark_connect.session_id
        assert response.server_side_session_id
        UUID(response.server_side_session_id)
        server_session_ids.add(response.server_side_session_id)
        assert response.WhichOneof("result") == field
    assert len(server_session_ids) == 1

    schema = responses["schema"].schema.schema
    assert schema.WhichOneof("kind") == "struct"
    assert [(field.name, field.data_type.WhichOneof("kind")) for field in schema.struct.fields] == [
        ("id", "long")
    ]
    assert responses["explain"].explain.explain_string
    assert responses["tree_string"].tree_string.tree_string
    assert not responses["is_local"].is_local.is_local
    assert not responses["is_streaming"].is_streaming.is_streaming
    assert list(responses["input_files"].input_files.files) == []
    assert responses["spark_version"].spark_version.version

    parsed = responses["ddl_parse"].ddl_parse.parsed
    assert parsed.WhichOneof("kind") == "struct"
    assert [(field.name, field.data_type.WhichOneof("kind")) for field in parsed.struct.fields] == [
        ("id", "long")
    ]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-003")
def test_tck_wire_003_required_config_operations_share_the_raw_session(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Exercise Set, Get, default, option, listing, mutability, and Unset directly."""
    proto = raw_spark_connect.proto
    key = "spark.sql.session.timeZone"
    missing_key = f"spark.connect.tck.missing.{uuid4().hex}"
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

    def config(operation: Any) -> Any:
        return raw_spark_connect.stub.Config(
            proto.ConfigRequest(
                session_id=raw_spark_connect.session_id,
                user_context=raw_spark_connect.user_context,
                client_type="spark-connect-tck",
                client_observed_server_side_session_id=set_response.server_side_session_id,
                operation=operation,
            ),
            timeout=30,
        )

    default_response = config(
        proto.ConfigRequest.Operation(
            get_with_default=proto.ConfigRequest.GetWithDefault(
                pairs=[proto.KeyValue(key=missing_key, value="fallback")]
            )
        )
    )
    option_response = config(
        proto.ConfigRequest.Operation(get_option=proto.ConfigRequest.GetOption(keys=[missing_key]))
    )
    all_response = config(
        proto.ConfigRequest.Operation(
            get_all=proto.ConfigRequest.GetAll(prefix="spark.sql.session.")
        )
    )
    modifiable_response = config(
        proto.ConfigRequest.Operation(is_modifiable=proto.ConfigRequest.IsModifiable(keys=[key]))
    )
    unset_response = config(
        proto.ConfigRequest.Operation(unset=proto.ConfigRequest.Unset(keys=[key]))
    )

    for response in (
        set_response,
        get_response,
        default_response,
        option_response,
        all_response,
        modifiable_response,
        unset_response,
    ):
        _assert_unary_response_identity(response, raw_spark_connect)
        assert response.server_side_session_id == set_response.server_side_session_id

    assert set_response.pairs == []
    assert [(pair.key, pair.value) for pair in get_response.pairs] == [(key, "UTC")]
    assert [(pair.key, pair.value) for pair in default_response.pairs] == [
        (missing_key, "fallback")
    ]
    assert len(option_response.pairs) == 1
    assert option_response.pairs[0].key == missing_key
    assert not option_response.pairs[0].HasField("value")
    assert ("timeZone", "UTC") in [(pair.key, pair.value) for pair in all_response.pairs]
    assert [(pair.key, pair.value) for pair in modifiable_response.pairs] == [(key, "true")]
    assert unset_response.pairs == []


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
                _function(expressions_pb2, "count", _attribute(expressions_pb2, "id")),
                "count",
            ),
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "sum", _attribute(expressions_pb2, "id")),
                "total",
            ),
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "avg", _attribute(expressions_pb2, "id")),
                "average",
            ),
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "min", _attribute(expressions_pb2, "id")),
                "minimum",
            ),
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "max", _attribute(expressions_pb2, "id")),
                "maximum",
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
        ["bucket", "count", "total", "average", "minimum", "maximum"],
    ) == [(0, 3, 6, 2.0, 0, 4), (1, 3, 9, 3.0, 1, 5)]


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

    conditional = expressions_pb2.Expression()
    conditional.expression_string.expression = "CASE WHEN id = 1 THEN 100 ELSE -1 END"

    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(filtered)
    projected.project.expressions.extend(
        [
            _attribute(expressions_pb2, "id"),
            _alias(
                expressions_pb2,
                conditional,
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


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-017")
def test_tck_wire_017_partition_alias_rename_and_sample_relations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Send partitioning, aliasing, renaming, and deterministic sample plans directly."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    repartitioned = relations_pb2.Relation()
    repartitioned.repartition.input.CopyFrom(_range_relation(relations_pb2, end=4))
    repartitioned.repartition.num_partitions = 1
    repartitioned.repartition.shuffle = True

    aliased = relations_pb2.Relation()
    aliased.subquery_alias.input.CopyFrom(repartitioned)
    aliased.subquery_alias.alias = "numbers"
    aliased.subquery_alias.qualifier.append("tck")

    renamed = relations_pb2.Relation()
    renamed.to_df.input.CopyFrom(aliased)
    renamed.to_df.column_names.append("value")

    partitioned_by_value = relations_pb2.Relation()
    partitioned_by_value.repartition_by_expression.input.CopyFrom(renamed)
    partitioned_by_value.repartition_by_expression.partition_exprs.append(
        _attribute(expressions_pb2, "value")
    )
    partitioned_by_value.repartition_by_expression.num_partitions = 1

    sampled = relations_pb2.Relation()
    sampled.sample.input.CopyFrom(partitioned_by_value)
    sampled.sample.lower_bound = 0.0
    sampled.sample.upper_bound = 1.0
    sampled.sample.with_replacement = False
    sampled.sample.seed = 17
    sampled.sample.deterministic_order = True

    responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, sampled, ["value"]),
    )

    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")), ["value"]
    ) == [(0,), (1,), (2,), (3,)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-018")
def test_tck_wire_018_to_schema_and_unpivot_relations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Send an explicit DataType schema and an Unpivot relation directly."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2, types_pb2

    table = pa.table(
        {
            "category": pa.array(["a", "b"]),
            "first": pa.array([1, 3], type=pa.int32()),
            "second": pa.array([2, 4], type=pa.int32()),
        }
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    local = relations_pb2.Relation()
    local.local_relation.data = sink.getvalue().to_pybytes()

    schema = types_pb2.DataType()
    kind = schema.struct.fields.add()
    kind.name = "category"
    kind.data_type.string.SetInParent()
    kind.nullable = True
    left = schema.struct.fields.add()
    left.name = "first"
    left.data_type.long.SetInParent()
    left.nullable = True
    right = schema.struct.fields.add()
    right.name = "second"
    right.data_type.long.SetInParent()
    right.nullable = True

    to_schema = relations_pb2.Relation()
    to_schema.to_schema.input.CopyFrom(local)
    to_schema.to_schema.schema.CopyFrom(schema)

    unpivoted = relations_pb2.Relation()
    unpivoted.unpivot.input.CopyFrom(to_schema)
    unpivoted.unpivot.ids.append(_attribute(expressions_pb2, "category"))
    unpivoted.unpivot.values.values.extend(
        [
            _attribute(expressions_pb2, "first"),
            _attribute(expressions_pb2, "second"),
        ]
    )
    unpivoted.unpivot.variable_column_name = "metric"
    unpivoted.unpivot.value_column_name = "amount"

    responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, unpivoted, ["category", "metric"]),
    )

    arrow_responses = [response for response in responses if response.HasField("arrow_batch")]
    batches = _decode_arrow_batches(arrow_responses)
    assert batches[0].schema.field("amount").type == pa.int64()
    assert _decode_arrow_tuples(arrow_responses, ["category", "metric", "amount"]) == [
        ("a", "first", 1),
        ("a", "second", 2),
        ("b", "first", 3),
        ("b", "second", 4),
    ]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-020")
def test_tck_wire_020_hint_and_transpose_relations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Execute direct optimizer-hint and transpose relation messages."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    hinted = relations_pb2.Relation()
    hinted.hint.input.CopyFrom(_range_relation(relations_pb2, end=4))
    hinted.hint.name = "COALESCE"
    hinted.hint.parameters.append(_int_literal(expressions_pb2, 1))

    local = _local_relation(
        relations_pb2,
        pa.table(
            {
                "label": pa.array(["a", "b"]),
                "first": pa.array([1, 3], type=pa.int64()),
                "second": pa.array([2, 4], type=pa.int64()),
            }
        ),
    )
    transposed = relations_pb2.Relation()
    transposed.transpose.input.CopyFrom(local)
    transposed.transpose.index_columns.append(_attribute(expressions_pb2, "label"))

    hint_responses = _execute_relation(raw_spark_connect, hinted)
    transpose_responses = _execute_relation(raw_spark_connect, transposed)

    assert _decode_arrow_rows(
        response for response in hint_responses if response.HasField("arrow_batch")
    ) == [0, 1, 2, 3]
    assert _decode_arrow_tuples(
        (response for response in transpose_responses if response.HasField("arrow_batch")),
        ["key", "a", "b"],
    ) == [("first", 1, 3), ("second", 2, 4)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-021")
def test_tck_wire_021_statistics_relations_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Execute deterministic summary, correlation, quantile, and stratified-sample plans."""
    import math

    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    numeric = relations_pb2.Relation()
    numeric.project.input.CopyFrom(_range_relation(relations_pb2, end=4))
    numeric.project.expressions.extend(
        [
            _alias(expressions_pb2, _attribute(expressions_pb2, "id"), "value"),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "*",
                    _attribute(expressions_pb2, "id"),
                    _long_literal(expressions_pb2, 2),
                ),
                "doubled",
            ),
        ]
    )

    summary = relations_pb2.Relation()
    summary.summary.input.CopyFrom(numeric)
    summary.summary.statistics.extend(["count", "min", "max"])

    covariance = relations_pb2.Relation()
    covariance.cov.input.CopyFrom(numeric)
    covariance.cov.col1 = "value"
    covariance.cov.col2 = "doubled"

    correlation = relations_pb2.Relation()
    correlation.corr.input.CopyFrom(numeric)
    correlation.corr.col1 = "value"
    correlation.corr.col2 = "doubled"
    correlation.corr.method = "pearson"

    quantiles = relations_pb2.Relation()
    quantiles.approx_quantile.input.CopyFrom(numeric)
    quantiles.approx_quantile.cols.extend(["value", "doubled"])
    quantiles.approx_quantile.probabilities.extend([0.0, 1.0])
    quantiles.approx_quantile.relative_error = 0.0

    sample = relations_pb2.Relation()
    sample.sample_by.input.CopyFrom(numeric)
    sample.sample_by.col.CopyFrom(_attribute(expressions_pb2, "value"))
    sample.sample_by.seed = 17
    for value in range(4):
        fraction = sample.sample_by.fractions.add()
        fraction.stratum.CopyFrom(_long_literal_value(expressions_pb2, value))
        fraction.fraction = 1.0

    summary_responses = _execute_relation(raw_spark_connect, summary)
    covariance_responses = _execute_relation(raw_spark_connect, covariance)
    correlation_responses = _execute_relation(raw_spark_connect, correlation)
    quantile_responses = _execute_relation(raw_spark_connect, quantiles)
    sample_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, sample, ["value"]),
    )

    assert _decode_arrow_tuples(
        (response for response in summary_responses if response.HasField("arrow_batch")),
        ["summary", "value", "doubled"],
    ) == [("count", "4", "4"), ("min", "0", "0"), ("max", "3", "6")]
    covariance_value = _decode_arrow_tuples(
        (response for response in covariance_responses if response.HasField("arrow_batch")), ["cov"]
    )[0][0]
    correlation_value = _decode_arrow_tuples(
        (response for response in correlation_responses if response.HasField("arrow_batch")),
        ["corr"],
    )[0][0]
    assert math.isclose(covariance_value, 10 / 3)
    assert math.isclose(correlation_value, 1.0)
    assert _decode_arrow_tuples(
        (response for response in quantile_responses if response.HasField("arrow_batch")),
        ["approx_quantile"],
    ) == [([[0.0, 3.0], [0.0, 6.0]],)]
    assert _decode_arrow_tuples(
        (response for response in sample_responses if response.HasField("arrow_batch")),
        ["value", "doubled"],
    ) == [(0, 0), (1, 2), (2, 4), (3, 6)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-022")
def test_tck_wire_022_view_command_named_table_and_catalog_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Create, inspect, read, and remove one session temp view through direct protobuf plans."""
    from pyspark.sql.connect.proto import commands_pb2, relations_pb2

    view_name = f"spark_connect_tck_{uuid4().hex}"
    create_view = commands_pb2.Command()
    create_view.create_dataframe_view.input.CopyFrom(_range_relation(relations_pb2, end=3))
    create_view.create_dataframe_view.name = view_name
    create_view.create_dataframe_view.replace = True

    view_created = False
    try:
        create_responses = _execute_command(raw_spark_connect, create_view)
        view_created = True
        assert not create_responses or any(
            response.HasField("result_complete") for response in create_responses
        )

        exists = relations_pb2.Relation()
        exists.catalog.table_exists.table_name = view_name
        exists_responses = _execute_relation(raw_spark_connect, exists)
        exists_batches = _decode_arrow_batches(
            response for response in exists_responses if response.HasField("arrow_batch")
        )
        exists_values = [value for batch in exists_batches for value in batch.column(0).to_pylist()]
        assert exists_values == [True]

        columns = relations_pb2.Relation()
        columns.catalog.list_columns.table_name = view_name
        column_responses = _execute_relation(raw_spark_connect, columns)
        column_batches = _decode_arrow_batches(
            response for response in column_responses if response.HasField("arrow_batch")
        )
        column_names = {value for batch in column_batches for value in batch.column(0).to_pylist()}
        assert column_names == {"id"}

        named_table = relations_pb2.Relation()
        named_table.read.named_table.unparsed_identifier = view_name
        read_responses = _execute_relation(raw_spark_connect, named_table)
        assert _decode_arrow_rows(
            response for response in read_responses if response.HasField("arrow_batch")
        ) == [0, 1, 2]
    finally:
        if view_created:
            dropped = relations_pb2.Relation()
            dropped.catalog.drop_temp_view.view_name = view_name
            drop_responses = _execute_relation(raw_spark_connect, dropped)
            drop_batches = _decode_arrow_batches(
                response for response in drop_responses if response.HasField("arrow_batch")
            )
            assert [value for batch in drop_batches for value in batch.column(0).to_pylist()] == [
                True
            ]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-024")
def test_tck_wire_024_required_scalar_and_lambda_functions_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Exercise the required scalar kernel and both lambda value/null handling directly."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    source = _local_relation(
        relations_pb2,
        pa.table(
            {
                "number": pa.array([-3], type=pa.int64()),
                "missing": pa.array([None], type=pa.int64()),
                "text": pa.array([" AbC "]),
                "items": pa.array([[1, None, 3]], type=pa.list_(pa.int64())),
            }
        ),
    )

    variable = expressions_pb2.Expression.UnresolvedNamedLambdaVariable(name_parts=["x"])
    variable_reference = expressions_pb2.Expression(unresolved_named_lambda_variable=variable)
    lambda_function = expressions_pb2.Expression()
    lambda_function.lambda_function.function.CopyFrom(
        _function(
            expressions_pb2,
            "+",
            _function(
                expressions_pb2,
                "coalesce",
                variable_reference,
                _long_literal(expressions_pb2, 0),
            ),
            _long_literal(expressions_pb2, 1),
        )
    )
    lambda_function.lambda_function.arguments.append(variable)

    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(source)
    projected.project.expressions.extend(
        [
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "abs", _attribute(expressions_pb2, "number")),
                "absolute",
            ),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "coalesce",
                    _attribute(expressions_pb2, "missing"),
                    _long_literal(expressions_pb2, 7),
                ),
                "coalesced",
            ),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "nullif",
                    _attribute(expressions_pb2, "number"),
                    _attribute(expressions_pb2, "number"),
                ),
                "nullified",
            ),
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "lower", _attribute(expressions_pb2, "text")),
                "lowered",
            ),
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "upper", _attribute(expressions_pb2, "text")),
                "uppered",
            ),
            _alias(
                expressions_pb2,
                _function(expressions_pb2, "length", _attribute(expressions_pb2, "text")),
                "length",
            ),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "substring",
                    _attribute(expressions_pb2, "text"),
                    _long_literal(expressions_pb2, 2),
                    _long_literal(expressions_pb2, 3),
                ),
                "substring",
            ),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "substr",
                    _attribute(expressions_pb2, "text"),
                    _long_literal(expressions_pb2, 2),
                    _long_literal(expressions_pb2, 3),
                ),
                "substr",
            ),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "concat",
                    _function(expressions_pb2, "trim", _attribute(expressions_pb2, "text")),
                    _string_literal(expressions_pb2, "!"),
                ),
                "concatenated",
            ),
            _alias(
                expressions_pb2,
                _function(
                    expressions_pb2,
                    "transform",
                    _attribute(expressions_pb2, "items"),
                    lambda_function,
                ),
                "transformed",
            ),
        ]
    )

    responses = _execute_relation(raw_spark_connect, projected)

    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        [
            "absolute",
            "coalesced",
            "nullified",
            "lowered",
            "uppered",
            "length",
            "substring",
            "substr",
            "concatenated",
            "transformed",
        ],
    ) == [(3, 7, None, " abc ", " ABC ", 5, "AbC", "AbC", "AbC!", [2, 1, 4])]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-025")
def test_tck_wire_025_parse_star_and_window_expressions_are_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Parse JSON and execute star and framed-window expressions through hand-built plans."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2, types_pb2

    json_source = _local_relation(
        relations_pb2,
        pa.table({"value": pa.array(['{"id":1,"name":"a"}', '{"id":2,"name":"b"}'])}),
    )
    schema = types_pb2.DataType()
    id_field = schema.struct.fields.add()
    id_field.name = "id"
    id_field.data_type.long.SetInParent()
    id_field.nullable = True
    name_field = schema.struct.fields.add()
    name_field.name = "name"
    name_field.data_type.string.SetInParent()
    name_field.nullable = True

    parsed = relations_pb2.Relation()
    parsed.parse.input.CopyFrom(json_source)
    parsed.parse.format = relations_pb2.Parse.PARSE_FORMAT_JSON
    parsed.parse.schema.CopyFrom(schema)

    star = expressions_pb2.Expression()
    star.unresolved_star.SetInParent()
    selected = relations_pb2.Relation()
    selected.project.input.CopyFrom(parsed)
    selected.project.expressions.append(star)

    window = expressions_pb2.Expression()
    window.window.window_function.CopyFrom(
        _function(expressions_pb2, "sum", _attribute(expressions_pb2, "id"))
    )
    window.window.order_spec.append(
        expressions_pb2.Expression.SortOrder(
            child=_attribute(expressions_pb2, "id"),
            direction=expressions_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING,
            null_ordering=expressions_pb2.Expression.SortOrder.SORT_NULLS_LAST,
        )
    )
    window.window.frame_spec.frame_type = (
        expressions_pb2.Expression.Window.WindowFrame.FRAME_TYPE_ROW
    )
    window.window.frame_spec.lower.unbounded = True
    window.window.frame_spec.upper.current_row = True

    running = relations_pb2.Relation()
    running.project.input.CopyFrom(_range_relation(relations_pb2, end=4))
    running.project.expressions.extend(
        [
            _attribute(expressions_pb2, "id"),
            _alias(expressions_pb2, window, "running_total"),
        ]
    )

    parsed_responses = _execute_relation(raw_spark_connect, selected)
    window_responses = _execute_relation(
        raw_spark_connect,
        _sorted_relation(relations_pb2, expressions_pb2, running, ["id"]),
    )

    assert _decode_arrow_tuples(
        (response for response in parsed_responses if response.HasField("arrow_batch")),
        ["id", "name"],
    ) == [(1, "a"), (2, "b")]
    assert _decode_arrow_tuples(
        (response for response in window_responses if response.HasField("arrow_batch")),
        ["id", "running_total"],
    ) == [(0, 0), (1, 1), (2, 3), (3, 6)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-WIRE-026")
@pytest.mark.parametrize(
    ("config_value", "use_large_types"),
    [
        pytest.param(None, False, id="default-false"),
        pytest.param("false", False, id="explicit-false"),
        pytest.param("true", True, id="explicit-true"),
    ],
)
def test_tck_wire_026_arrow_variable_width_types_round_trip_recursively(
    raw_spark_connect: RawSparkConnectSession,
    config_value: str | None,
    use_large_types: bool,
) -> None:
    """Round-trip narrow and large String/Binary values at every required nesting level."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import relations_pb2

    if config_value is not None:
        proto = raw_spark_connect.proto
        config_response = raw_spark_connect.stub.Config(
            proto.ConfigRequest(
                session_id=raw_spark_connect.session_id,
                user_context=raw_spark_connect.user_context,
                client_type="spark-connect-tck",
                operation=proto.ConfigRequest.Operation(
                    set=proto.ConfigRequest.Set(
                        pairs=[
                            proto.KeyValue(
                                key="spark.sql.execution.arrow.useLargeVarTypes",
                                value=config_value,
                            )
                        ]
                    )
                ),
            ),
            timeout=30,
        )
        _assert_unary_response_identity(config_response, raw_spark_connect)

    string_type = pa.large_string() if use_large_types else pa.string()
    binary_type = pa.large_binary() if use_large_types else pa.binary()
    array_type = pa.list_(string_type)
    map_type = pa.map_(string_type, binary_type)
    struct_type = pa.struct(
        [
            pa.field("text", string_type),
            pa.field("payload", binary_type),
        ]
    )
    table = pa.table(
        {
            "text": pa.array(["", "spark", None], type=string_type),
            "payload": pa.array([b"", b"\x00\xff", None], type=binary_type),
            "texts": pa.array([[], ["", None, "spark"], None], type=array_type),
            "attributes": pa.array(
                [[], [("key", b"\x01"), ("empty", b"")], None],
                type=map_type,
            ),
            "record": pa.array(
                [
                    {"text": "", "payload": b""},
                    {"text": None, "payload": b"\x02"},
                    None,
                ],
                type=struct_type,
            ),
            "all_null_text": pa.array([None, None, None], type=string_type),
        }
    )

    responses = _execute_relation(raw_spark_connect, _local_relation(relations_pb2, table))
    batches = _decode_arrow_batches(
        response for response in responses if response.HasField("arrow_batch")
    )
    result = pa.Table.from_batches(batches)

    assert result.schema.field("text").type == string_type
    assert result.schema.field("payload").type == binary_type
    assert result.schema.field("texts").type == array_type
    assert result.schema.field("attributes").type == map_type
    assert result.schema.field("record").type == struct_type
    assert result.schema.field("all_null_text").type == string_type
    assert result.to_pylist() == table.to_pylist()


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-001")
def test_tck_sql_001_portable_relation_sql_is_direct(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Exercise the v0.20 Portable SQL Core through direct Relation.SQL plans."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    grouped = relations_pb2.Relation()
    grouped.sql.query = """
        SELECT left_values.category AS category,
               count(*) AS row_count,
               sum(right_values.amount) AS total,
               avg(right_values.amount) AS average,
               min(right_values.amount) AS minimum,
               max(right_values.amount) AS maximum
        FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS left_values(id, category)
        INNER JOIN (VALUES (1, 10), (2, 20), (3, 5)) AS right_values(id, amount)
          ON left_values.id = right_values.id
        WHERE right_values.amount >= :minimum_amount
        GROUP BY left_values.category
        HAVING sum(right_values.amount) >= 15
        ORDER BY category ASC NULLS LAST
        LIMIT 2 OFFSET 0
    """
    grouped.sql.named_arguments["minimum_amount"].CopyFrom(_long_literal(expressions_pb2, 5))

    scalar = relations_pb2.Relation()
    scalar.sql.query = """
        SELECT DISTINCT values_table.value AS value,
               upper(trim(values_table.label)) AS normalized,
               length(values_table.label) AS original_length
        FROM (
          VALUES (?, ' a '), (?, ' a '), (CAST(NULL AS BIGINT), 'ignored')
        ) AS values_table(value, label)
        WHERE values_table.value IS NOT NULL
        UNION ALL
        SELECT CAST(abs(-3) AS BIGINT) AS value,
               CASE WHEN nullif(lower('X'), 'x') IS NULL
                    THEN substring(concat(' extra', ' '), 2, 5)
                    ELSE coalesce(NULL, 'bad')
               END AS normalized,
               CAST(7 AS INTEGER) AS original_length
        ORDER BY value ASC NULLS LAST
        LIMIT 3 OFFSET 0
    """
    scalar.sql.pos_arguments.extend(
        [_long_literal(expressions_pb2, 1), _long_literal(expressions_pb2, 2)]
    )

    left_join = relations_pb2.Relation()
    left_join.sql.query = """
        SELECT left_values.id AS id, right_values.label AS label
        FROM (VALUES (1), (2)) AS left_values(id)
        LEFT OUTER JOIN (VALUES (2, 'matched')) AS right_values(id, label)
          ON left_values.id = right_values.id
        ORDER BY id ASC NULLS LAST
    """

    cross_join = relations_pb2.Relation()
    cross_join.sql.query = """
        SELECT left_values.id AS left_id, right_values.id AS right_id
        FROM (VALUES (1), (2)) AS left_values(id)
        CROSS JOIN (VALUES (3), (4)) AS right_values(id)
        ORDER BY left_id ASC NULLS LAST, right_id DESC NULLS LAST
    """

    grouped_responses = _execute_relation(raw_spark_connect, grouped)
    scalar_responses = _execute_relation(raw_spark_connect, scalar)
    left_join_responses = _execute_relation(raw_spark_connect, left_join)
    cross_join_responses = _execute_relation(raw_spark_connect, cross_join)

    assert _decode_arrow_tuples(
        (response for response in grouped_responses if response.HasField("arrow_batch")),
        ["category", "row_count", "total", "average", "minimum", "maximum"],
    ) == [("a", 2, 30, 15.0, 10, 20)]
    assert _decode_arrow_tuples(
        (response for response in scalar_responses if response.HasField("arrow_batch")),
        ["value", "normalized", "original_length"],
    ) == [(1, "A", 3), (2, "A", 3), (3, "extra", 7)]
    assert _decode_arrow_tuples(
        (response for response in left_join_responses if response.HasField("arrow_batch")),
        ["id", "label"],
    ) == [(1, None), (2, "matched")]
    assert _decode_arrow_tuples(
        (response for response in cross_join_responses if response.HasField("arrow_batch")),
        ["left_id", "right_id"],
    ) == [(1, 4), (1, 3), (2, 4), (2, 3)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-002")
def test_tck_sql_002_portable_sql_command_returns_relation(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Execute a portable query through SqlCommand.input, then execute its returned relation."""
    from pyspark.sql.connect.proto import commands_pb2, expressions_pb2, relations_pb2

    query = relations_pb2.Relation()
    query.sql.query = "SELECT :value + 1 AS incremented"
    query.sql.named_arguments["value"].CopyFrom(_long_literal(expressions_pb2, 7))

    command = commands_pb2.Command()
    command.sql_command.input.CopyFrom(query)
    command_responses = _execute_command(raw_spark_connect, command)
    command_results = [
        response.sql_command_result
        for response in command_responses
        if response.HasField("sql_command_result")
    ]

    assert len(command_results) == 1
    assert command_results[0].HasField("relation")

    direct_responses = _execute_relation(raw_spark_connect, query)
    returned_responses = _execute_relation(raw_spark_connect, command_results[0].relation)
    expected = [(8,)]
    assert (
        _decode_arrow_tuples(
            (response for response in direct_responses if response.HasField("arrow_batch")),
            ["incremented"],
        )
        == expected
    )
    assert (
        _decode_arrow_tuples(
            (response for response in returned_responses if response.HasField("arrow_batch")),
            ["incremented"],
        )
        == expected
    )


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-003")
def test_tck_sql_003_portable_casts_have_exact_logical_types(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Assert the portable aliases independently of bare VARCHAR."""
    from decimal import Decimal

    from pyspark.sql.connect.proto import relations_pb2

    query = relations_pb2.Relation()
    query.sql.query = """
        SELECT CAST(TRUE AS BOOLEAN) AS boolean_value,
               CAST(32767 AS SMALLINT) AS small_value,
               CAST(2147483647 AS INTEGER) AS integer_value,
               CAST(9223372036854775807 AS BIGINT) AS big_value,
               CAST(1.25 AS REAL) AS real_value,
               CAST(2.5 AS DOUBLE) AS double_value,
               CAST(12.34 AS DECIMAL(4, 2)) AS decimal_value,
               CAST('2024-01-02' AS DATE) AS date_value,
               CAST('2024-01-02 03:04:05' AS TIMESTAMP) AS timestamp_value
    """

    responses = _execute_relation(raw_spark_connect, query)
    schema = _result_schema(responses)
    fields = {field.name: field.data_type for field in schema.fields}

    assert {name: data_type.WhichOneof("kind") for name, data_type in fields.items()} == {
        "boolean_value": "boolean",
        "small_value": "short",
        "integer_value": "integer",
        "big_value": "long",
        "real_value": "float",
        "double_value": "double",
        "decimal_value": "decimal",
        "date_value": "date",
        "timestamp_value": "timestamp",
    }
    assert (fields["decimal_value"].decimal.precision, fields["decimal_value"].decimal.scale) == (
        4,
        2,
    )
    rows = _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        list(fields),
    )
    assert len(rows) == 1
    assert rows[0][:-2] == (
        True,
        32767,
        2147483647,
        9223372036854775807,
        1.25,
        2.5,
        Decimal("12.34"),
    )
    assert rows[0][-2].isoformat() == "2024-01-02"
    assert rows[0][-1].replace(tzinfo=None).isoformat() == "2024-01-02T03:04:05"


@pytest.mark.smoke
@pytest.mark.reference_gap
@pytest.mark.tck_case("TCK-SQL-004")
def test_tck_sql_004_bare_varchar_is_binary_string(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Require bare VARCHAR to produce String rather than optional VarChar or a parse error."""
    from pyspark.sql.connect.proto import relations_pb2

    query = relations_pb2.Relation()
    query.sql.query = "SELECT CAST('connect' AS VARCHAR) AS varchar_value"
    responses = _execute_relation(raw_spark_connect, query)
    schema = _result_schema(responses)

    assert len(schema.fields) == 1
    varchar_type = schema.fields[0].data_type
    assert varchar_type.WhichOneof("kind") == "string"
    assert varchar_type.string.collation in ("", "UTF8_BINARY")
    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        ["varchar_value"],
    ) == [("connect",)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-005")
@pytest.mark.parametrize(
    ("session_zone", "timestamp_default", "expected_utc_hour"),
    [
        pytest.param(
            "UTC",
            "TIMESTAMP_NTZ",
            3,
            id="utc-ntz-default",
            marks=pytest.mark.reference_gap,
        ),
        pytest.param("UTC", "TIMESTAMP_LTZ", 3, id="utc-ltz-default"),
        pytest.param(
            "America/Los_Angeles",
            "TIMESTAMP_NTZ",
            11,
            id="los-angeles-ntz-default",
            marks=pytest.mark.reference_gap,
        ),
        pytest.param("America/Los_Angeles", "TIMESTAMP_LTZ", 11, id="los-angeles-ltz-default"),
    ],
)
def test_tck_sql_005_portable_timestamp_ignores_non_core_default(
    raw_spark_connect: RawSparkConnectSession,
    session_zone: str,
    timestamp_default: str,
    expected_utc_hour: int,
) -> None:
    """Keep TIMESTAMP zoned under UTC/non-UTC and both Spark timestamp defaults."""
    from datetime import timezone

    from pyspark.sql.connect.proto import relations_pb2

    proto = raw_spark_connect.proto
    config_response = raw_spark_connect.stub.Config(
        proto.ConfigRequest(
            session_id=raw_spark_connect.session_id,
            user_context=raw_spark_connect.user_context,
            client_type="spark-connect-tck",
            operation=proto.ConfigRequest.Operation(
                set=proto.ConfigRequest.Set(
                    pairs=[
                        proto.KeyValue(key="spark.sql.session.timeZone", value=session_zone),
                        proto.KeyValue(key="spark.sql.timestampType", value=timestamp_default),
                    ]
                )
            ),
        ),
        timeout=30,
    )
    _assert_unary_response_identity(config_response, raw_spark_connect)

    query = relations_pb2.Relation()
    query.sql.query = "SELECT CAST('2024-01-02 03:04:05.123456' AS TIMESTAMP) AS timestamp_value"
    responses = _execute_relation(raw_spark_connect, query)
    schema = _result_schema(responses)
    assert len(schema.fields) == 1
    assert schema.fields[0].data_type.WhichOneof("kind") == "timestamp"

    batches = _decode_arrow_batches(
        response for response in responses if response.HasField("arrow_batch")
    )
    assert len(batches) == 1
    arrow_type = batches[0].schema.field("timestamp_value").type
    assert arrow_type.tz == session_zone
    value = batches[0].column("timestamp_value").to_pylist()[0]
    assert value.replace(tzinfo=None).isoformat() == "2024-01-02T03:04:05.123456"
    assert value.astimezone(timezone.utc).hour == expected_utc_hour


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-006")
def test_tck_sql_006_portable_precedence_and_associativity(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Exercise every Portable SQL precedence row with discriminating values."""
    from pyspark.sql.connect.proto import relations_pb2

    query = relations_pb2.Relation()
    query.sql.query = """
        SELECT (1 + 2) * 3 AS parenthesized,
               - - 2 AS unary_value,
               16 / 4 / 2 AS division_value,
               8 - 3 - 2 AS subtraction_value,
               1 + 2 * 3 AS mixed_arithmetic,
               NOT 1 = 1 AS not_predicate,
               TRUE OR TRUE AND FALSE AS boolean_precedence
    """

    responses = _execute_relation(raw_spark_connect, query)
    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        [
            "parenthesized",
            "unary_value",
            "division_value",
            "subtraction_value",
            "mixed_arithmetic",
            "not_predicate",
            "boolean_precedence",
        ],
    ) == [(9, 2, 2.0, 3, 7, False, True)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-007")
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param(
            "1 = 1 = TRUE",
            id="comparison-chain",
            marks=pytest.mark.reference_gap,
        ),
        pytest.param("1 IS NULL IS NULL", id="null-test-chain"),
    ],
)
def test_tck_sql_007_portable_rejects_chained_predicates(
    raw_spark_connect: RawSparkConnectSession,
    expression: str,
) -> None:
    """Reject comparison and null-test chains instead of importing backend precedence."""
    from pyspark.sql.connect.proto import relations_pb2

    query = relations_pb2.Relation()
    query.sql.query = f"SELECT {expression} AS chained_predicate"
    _assert_relation_rejected(raw_spark_connect, query)


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-008")
def test_tck_sql_008_portable_lexical_productions(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Exercise the complete v0.20 Portable SQL lexer with discriminating tokens."""
    from pyspark.sql.connect.proto import relations_pb2

    query = relations_pb2.Relation()
    query.sql.query = (
        "sElEcT\t"
        "cAsT(0007 aS BIGINT) AS _integer1,\r\n"
        "CAST(12.50e+1 AS DOUBLE) AS decimal_exponent, "
        "CAST(2E-1 AS DOUBLE) AS exponent_only,\n"
        "'Spark ''Connect'' Ω 😀' AS unicode_text, "
        "TRUE AS boolean_value, CAST(NULL AS INTEGER) AS null_value,\r"
        "1 <= 1 AS less_equal, 2 >= 1 AS greater_equal, "
        "1 <> 2 AS not_equal_angle, 1 != 2 AS not_equal_bang"
    )

    responses = _execute_relation(raw_spark_connect, query)
    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        [
            "_integer1",
            "decimal_exponent",
            "exponent_only",
            "unicode_text",
            "boolean_value",
            "null_value",
            "less_equal",
            "greater_equal",
            "not_equal_angle",
            "not_equal_bang",
        ],
    ) == [(7, 125.0, 0.2, "Spark 'Connect' Ω 😀", True, None, True, True, True, True)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-009")
def test_tck_sql_009_function_names_are_contextual(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Resolve ABS as an identifier except when the next token opens a call."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import commands_pb2, relations_pb2

    create_view = commands_pb2.Command()
    create_view.create_dataframe_view.input.CopyFrom(
        _local_relation(relations_pb2, pa.table({"abs": [-3]}))
    )
    create_view.create_dataframe_view.name = "abs"
    create_view.create_dataframe_view.replace = True

    view_created = False
    try:
        _execute_command(raw_spark_connect, create_view)
        view_created = True

        queries = [
            ("SELECT abs FROM abs", "abs", -3),
            ("SELECT abs(abs) AS result FROM abs", "result", 3),
            ("SELECT aBs FROM aBs", "aBs", -3),
            ("SELECT AbS(aBs) AS result FROM aBs", "result", 3),
        ]
        for sql, column, expected in queries:
            query = relations_pb2.Relation()
            query.sql.query = sql
            responses = _execute_relation(raw_spark_connect, query)
            assert _decode_arrow_tuples(
                (response for response in responses if response.HasField("arrow_batch")),
                [column],
            ) == [(expected,)]
    finally:
        if view_created:
            dropped = relations_pb2.Relation()
            dropped.catalog.drop_temp_view.view_name = "abs"
            _execute_relation(raw_spark_connect, dropped)


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-EXPR-001")
def test_tck_expr_001_expression_string_precedence_and_associativity(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Exercise every ExpressionString precedence row in a direct plan."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    expressions = [
        ("parenthesized", "(1 + 2) * 3"),
        ("unary_value", "- - 2"),
        ("division_value", "16 / 4 / 2"),
        ("subtraction_value", "8 - 3 - 2"),
        ("mixed_arithmetic", "1 + 2 * 3"),
        ("not_equality", "NOT 1 == 1"),
        ("not_null_safe_equality", "NOT 1 <=> 2"),
        ("boolean_precedence", "TRUE OR TRUE AND FALSE"),
    ]
    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(_range_relation(relations_pb2, end=1))
    projected.project.expressions.extend(
        _alias(expressions_pb2, _expression_string(expressions_pb2, text), name)
        for name, text in expressions
    )

    responses = _execute_relation(raw_spark_connect, projected)
    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        [name for name, _ in expressions],
    ) == [(9, 2, 2.0, 3, 7, False, True, True)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-EXPR-002")
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param(
            "1 == 1 == TRUE",
            id="equality-chain",
            marks=pytest.mark.reference_gap,
        ),
        pytest.param(
            "1 <=> 1 <=> TRUE",
            id="null-safe-equality-chain",
            marks=pytest.mark.reference_gap,
        ),
        pytest.param("1 IS NULL IS NULL", id="null-test-chain"),
    ],
)
def test_tck_expr_002_expression_string_rejects_chained_predicates(
    raw_spark_connect: RawSparkConnectSession,
    expression: str,
) -> None:
    """Reject ExpressionString predicate chains, including expression-only operators."""
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(_range_relation(relations_pb2, end=1))
    projected.project.expressions.append(_expression_string(expressions_pb2, expression))
    _assert_relation_rejected(raw_spark_connect, projected)


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-EXPR-003")
def test_tck_expr_003_expression_string_lexical_productions(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Resolve v0.20 unquoted, quoted, escaped, Unicode, and multipart attribute tokens."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    struct_type = pa.struct([pa.field("inner.dot", pa.int64())])
    source = _local_relation(
        relations_pb2,
        pa.table(
            {
                "plain_1": pa.array([3], type=pa.int64()),
                "part.with.dot": pa.array(["quoted"]),
                "tick`name": pa.array([5], type=pa.int64()),
                "naïve name": pa.array([6], type=pa.int64()),
                "outer": pa.array([{"inner.dot": 7}], type=struct_type),
            }
        ),
    )
    expressions = [
        ("plain", "plain_1"),
        ("dotted_name", "`part.with.dot`"),
        ("escaped_backtick", "`tick``name`"),
        ("unicode_name", "`naïve name`"),
        ("multipart", "outer.`inner.dot`"),
        ("unicode_text", "'Spark ''Connect'' Ω 😀'"),
        ("exponent", "CAST(2E-1 AS DOUBLE)"),
    ]
    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(source)
    projected.project.expressions.extend(
        _alias(expressions_pb2, _expression_string(expressions_pb2, text), name)
        for name, text in expressions
    )

    responses = _execute_relation(raw_spark_connect, projected)
    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        [name for name, _ in expressions],
    ) == [(3, "quoted", 5, 6, 7, "Spark 'Connect' Ω 😀", 0.2)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-EXPR-004")
def test_tck_expr_004_function_names_are_contextual(
    raw_spark_connect: RawSparkConnectSession,
) -> None:
    """Resolve ABS as an attribute except when the next token opens a call."""
    import pyarrow as pa
    from pyspark.sql.connect.proto import expressions_pb2, relations_pb2

    source = _local_relation(relations_pb2, pa.table({"abs": [-3]}))
    expressions = [
        ("identifier", "abs"),
        ("lowercase_call", "abs(abs)"),
        ("mixed_case_identifier", "aBs"),
        ("mixed_case_call", "AbS(aBs)"),
    ]
    projected = relations_pb2.Relation()
    projected.project.input.CopyFrom(source)
    projected.project.expressions.extend(
        _alias(expressions_pb2, _expression_string(expressions_pb2, text), name)
        for name, text in expressions
    )

    responses = _execute_relation(raw_spark_connect, projected)
    assert _decode_arrow_tuples(
        (response for response in responses if response.HasField("arrow_batch")),
        [name for name, _ in expressions],
    ) == [(-3, 3, -3, 3)]
