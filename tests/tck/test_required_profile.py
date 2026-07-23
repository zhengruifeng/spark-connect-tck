"""Starter conformance cases selected from the SC-1.0-P1 required profile."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession


pytestmark = pytest.mark.tck


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-ANALYZE-001")
def test_tck_analyze_001_required_relation_analysis(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE AnalyzePlan: required relation properties are available."""
    result = spark.range(2)

    assert result.schema.fieldNames() == ["id"]
    assert result.schema["id"].dataType.simpleString() == "bigint"
    assert not result.isLocal()
    assert not result.isStreaming
    assert result.inputFiles() == []


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-EXEC-001")
def test_tck_exec_001_empty_result_preserves_schema(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE ExecutePlan: zero rows are a successful typed result."""
    result = spark.sql("SELECT CAST(1 AS INT) AS value WHERE FALSE")

    assert result.schema.fieldNames() == ["value"]
    assert result.schema["value"].dataType.simpleString() == "int"
    assert result.collect() == []


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-EXEC-002")
def test_tck_exec_002_sql_named_parameters_are_typed(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE SqlCommand: named values bind as typed expressions."""
    row = spark.sql(
        "SELECT :number + 1 AS incremented, :label AS label",
        args={"number": 7, "label": "connect"},
    ).first()

    assert row.asDict() == {"incremented": 8, "label": "connect"}


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SESSION-001")
def test_tck_session_001_sessions_isolate_config_and_temp_views(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE: session-scoped state cannot leak to another session."""
    first = spark.newSession()
    second = spark.newSession()
    view_name = f"spark_connect_tck_{uuid4().hex}"
    try:
        first.conf.set("spark.sql.session.timeZone", "UTC")
        second.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")

        first_zone = first.sql("SELECT current_timezone() AS zone").first()["zone"]
        second_zone = second.sql("SELECT current_timezone() AS zone").first()["zone"]
        assert first_zone == "UTC"
        assert second_zone == "America/Los_Angeles"

        first.range(1).createOrReplaceTempView(view_name)
        assert first.catalog.tableExists(view_name)
        assert not second.catalog.tableExists(view_name)
    finally:
        first.catalog.dropTempView(view_name)
        first.stop()
        second.stop()


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-CONFIG-001")
def test_tck_config_001_time_zone_is_set_read_and_observed(
    isolated_spark: SparkSession,
) -> None:
    """SC-1.0-P1-WIRE Config: session time-zone changes affect later SQL."""
    key = "spark.sql.session.timeZone"
    original_value = isolated_spark.conf.get(key)
    try:
        isolated_spark.conf.set(key, "UTC")
        assert isolated_spark.conf.get(key) == "UTC"
        assert isolated_spark.sql("SELECT current_timezone() AS zone").first()["zone"] == "UTC"
    finally:
        isolated_spark.conf.set(key, original_value)


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-CONFIG-002")
def test_tck_config_002_sql_set_and_reset_share_runtime_config(
    isolated_spark: SparkSession,
) -> None:
    """SC-1.0-P1-SQL SQL-CONFIG: SQL changes are visible through RuntimeConfig."""
    key = "spark.sql.session.timeZone"
    original_value = isolated_spark.conf.get(key)
    try:
        isolated_spark.sql(f"SET {key}=UTC")
        assert isolated_spark.conf.get(key) == "UTC"

        isolated_spark.sql(f"RESET {key}")
        reset_value = isolated_spark.conf.get(key)
        observed_zone = isolated_spark.sql("SELECT current_timezone() AS zone").first()["zone"]
        assert observed_zone == reset_value
    finally:
        isolated_spark.conf.set(key, original_value)


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-CATALOG-001")
def test_tck_catalog_001_temporary_view_lifecycle(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Catalog: temporary views are queryable, listed, and removable."""
    view_name = f"spark_connect_tck_{uuid4().hex}"
    spark.range(5).createOrReplaceTempView(view_name)
    try:
        assert spark.catalog.tableExists(view_name)
        tables = spark.catalog.listTables()
        assert any(table.name == view_name and table.isTemporary for table in tables)
        assert spark.sql(f"SELECT sum(id) AS total FROM {view_name}").first()["total"] == 10
    finally:
        assert spark.catalog.dropTempView(view_name)
        assert not spark.catalog.tableExists(view_name)


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-CATALOG-002")
def test_tck_catalog_002_current_database_is_listed(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Catalog: the current database appears in visible databases."""
    current_database = spark.catalog.currentDatabase()
    databases = spark.catalog.listDatabases()

    assert current_database
    assert any(database.name == current_database for database in databases)


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-REL-001")
def test_tck_rel_001_dataframe_relations_preserve_schema_and_rows(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Relations: standard DataFrame plans preserve their result."""
    result = (
        spark.range(10)
        .where("id % 2 = 0")
        .selectExpr("id % 3 AS bucket", "id * id AS squared")
        .groupBy("bucket")
        .sum("squared")
        .withColumnRenamed("sum(squared)", "total")
        .orderBy("bucket")
    )

    assert result.schema.fieldNames() == ["bucket", "total"]
    actual_rows = [(row["bucket"], row["total"]) for row in result.collect()]
    assert actual_rows == [(0, 36), (1, 16), (2, 68)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-REL-002")
def test_tck_rel_002_joins_and_set_operations_preserve_rows(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Relations: joins and set operations retain their semantics."""
    left = spark.range(3).selectExpr("id", "concat('left-', id) AS label")
    right = spark.range(1, 4).selectExpr("id", "id * 10 AS amount")
    joined = left.join(right, on="id").orderBy("id")
    unioned = left.select("id").unionByName(right.select("id")).distinct().orderBy("id")

    assert [(row["id"], row["label"], row["amount"]) for row in joined.collect()] == [
        (1, "left-1", 10),
        (2, "left-2", 20),
    ]
    assert [row["id"] for row in unioned.collect()] == [0, 1, 2, 3]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-REL-003")
def test_tck_rel_003_na_relations_preserve_null_semantics(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Relations: NA fill, drop, and replace are deterministic."""
    source = spark.sql(
        """
        SELECT CAST(NULL AS STRING) AS label, CAST(NULL AS INT) AS score
        UNION ALL
        SELECT 'keep' AS label, 2 AS score
        """
    )
    filled = source.na.fill({"label": "missing", "score": 0}).orderBy("label")
    replaced = filled.na.replace({"missing": "replaced"}, subset=["label"]).orderBy("label")
    dropped = source.na.drop(subset=["label"]).orderBy("label")

    assert [(row["label"], row["score"]) for row in filled.collect()] == [
        ("keep", 2),
        ("missing", 0),
    ]
    assert [(row["label"], row["score"]) for row in replaced.collect()] == [
        ("keep", 2),
        ("replaced", 0),
    ]
    assert [(row["label"], row["score"]) for row in dropped.collect()] == [("keep", 2)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-TYPE-001")
def test_tck_type_001_required_sql_types_round_trip(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Data types: required types retain their logical schemas."""
    result = spark.sql(
        """
        SELECT
          CAST(127 AS TINYINT) AS tiny,
          CAST(32767 AS SMALLINT) AS small,
          CAST(2147483647 AS INT) AS integer,
          CAST(9223372036854775807 AS BIGINT) AS big,
          CAST(1.25 AS FLOAT) AS float_value,
          CAST(2.5 AS DOUBLE) AS double_value,
          CAST('12.34' AS DECIMAL(4, 2)) AS decimal_value,
          TRUE AS boolean_value,
          'connect' AS string_value,
          X'00FF' AS binary_value,
          DATE '2024-01-02' AS date_value,
          TIMESTAMP '2024-01-02 03:04:05' AS timestamp_value,
          TIMESTAMP_NTZ '2024-01-02 03:04:05' AS timestamp_ntz_value,
          array(1, CAST(NULL AS INT)) AS array_value,
          map('one', 1) AS map_value,
          named_struct('name', 'connect', 'count', 2) AS struct_value
        """
    )

    types = {field.name: field.dataType.simpleString() for field in result.schema}
    assert types == {
        "tiny": "tinyint",
        "small": "smallint",
        "integer": "int",
        "big": "bigint",
        "float_value": "float",
        "double_value": "double",
        "decimal_value": "decimal(4,2)",
        "boolean_value": "boolean",
        "string_value": "string",
        "binary_value": "binary",
        "date_value": "date",
        "timestamp_value": "timestamp",
        "timestamp_ntz_value": "timestamp_ntz",
        "array_value": "array<int>",
        "map_value": "map<string,int>",
        "struct_value": "struct<name:string,count:int>",
    }

    row = result.first()
    assert (row["tiny"], row["small"], row["integer"], row["big"]) == (
        127,
        32767,
        2147483647,
        9223372036854775807,
    )
    assert row["decimal_value"] == Decimal("12.34")
    assert bytes(row["binary_value"]) == b"\x00\xff"
    assert row["array_value"] == [1, None]
    assert row["map_value"] == {"one": 1}
    assert row["struct_value"].asDict() == {"name": "connect", "count": 2}


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-TYPE-002")
def test_tck_type_002_null_and_empty_values_remain_distinct(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Data types: null and empty are never silently conflated."""
    row = spark.sql(
        """
        SELECT
          CAST(NULL AS STRING) AS null_string,
          '' AS empty_string,
          array() AS empty_array,
          array(CAST(NULL AS INT)) AS array_with_null
        """
    ).first()

    assert row["null_string"] is None
    assert row["empty_string"] == ""
    assert row["empty_array"] == []
    assert row["array_with_null"] == [None]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-TYPE-003")
def test_tck_type_003_floating_point_special_values(spark: SparkSession) -> None:
    """SC-1.0-P1-WIRE Data types: IEEE 754 special values round-trip."""
    import math

    row = spark.sql(
        """
        SELECT
          CAST('NaN' AS DOUBLE) AS nan_value,
          CAST('Infinity' AS DOUBLE) AS positive_infinity,
          CAST('-Infinity' AS DOUBLE) AS negative_infinity,
          CAST('-0.0' AS DOUBLE) AS negative_zero
        """
    ).first()

    assert math.isnan(row["nan_value"])
    assert row["positive_infinity"] == math.inf
    assert row["negative_infinity"] == -math.inf
    assert math.copysign(1.0, row["negative_zero"]) == -1.0


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-001")
def test_tck_sql_001_required_query_productions(spark: SparkSession) -> None:
    """SC-1.0-P1-SQL: CTE, VALUES, UNION ALL, ORDER BY, and LIMIT interoperate."""
    rows = spark.sql(
        """
        WITH values_cte(value) AS (VALUES (3), (1), (2))
        SELECT value FROM values_cte WHERE value > 1
        UNION ALL
        SELECT 0 AS value
        ORDER BY value
        LIMIT 3
        """
    ).collect()

    assert [row["value"] for row in rows] == [0, 2, 3]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-002")
def test_tck_sql_002_intersect_and_except_all(spark: SparkSession) -> None:
    """SC-1.0-P1-SQL SQL-QRY-SET: intersection and multiset difference are required."""
    intersected = spark.sql(
        """
        SELECT value FROM VALUES (1), (2) AS left_values(value)
        INTERSECT
        SELECT value FROM VALUES (2), (3) AS right_values(value)
        ORDER BY value
        """
    ).collect()
    excepted = spark.sql(
        """
        SELECT value FROM VALUES (1), (2), (2) AS left_values(value)
        EXCEPT ALL
        SELECT value FROM VALUES (2) AS right_values(value)
        ORDER BY value
        """
    ).collect()

    assert [row["value"] for row in intersected] == [2]
    assert [row["value"] for row in excepted] == [1, 2]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-SQL-003")
def test_tck_sql_003_join_group_and_having(spark: SparkSession) -> None:
    """SC-1.0-P1-SQL SQL-QRY-SELECT: joins, grouping, and HAVING interoperate."""
    rows = spark.sql(
        """
        SELECT left_values.category, sum(right_values.amount) AS total
        FROM VALUES (1, 'a'), (2, 'a'), (3, 'b') AS left_values(id, category)
        JOIN VALUES (1, 10), (2, 20), (3, 5) AS right_values(id, amount)
          ON left_values.id = right_values.id
        GROUP BY left_values.category
        HAVING sum(right_values.amount) >= 15
        ORDER BY category
        """
    ).collect()

    assert [(row["category"], row["total"]) for row in rows] == [("a", 30)]


@pytest.mark.smoke
@pytest.mark.tck_case("TCK-PRESENTATION-001")
def test_tck_presentation_001_show_renders_table(
    spark: SparkSession,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SC-1.0-P1-WIRE ShowString: the default table rendering is exact."""
    spark.range(2).show()

    assert capsys.readouterr().out == "+---+\n| id|\n+---+\n|  0|\n|  1|\n+---+\n\n"
