# Spark Connect TCK

A Python implementation of a Technology Compatibility Kit (TCK) for Spark
Connect servers. The suite exercises public PySpark Connect APIs and selected
generated gRPC/protobuf requests against a separately running target server.

The intended implementation requirements are recorded in the [Spark Connect
1.0 draft](https://docs.google.com/document/d/1FFBrD__93Pdznj4roy2UrDpoRzMQnhjvfxrtChzXPpg/edit?tab=t.0#heading=h.lweyg2cvmawz).
This repository implements a deliberately small, traceable starter slice of
its `SC-1.0-P1` required profile, pinned to Apache Spark 4.2.0. It is not yet a
complete conformance suite and must not be used to make a Spark Connect 1.0
compatibility claim.

## Scope

The initial suite establishes a small, portable baseline. Every live-target
test is registered with its normative manifest and rows; collection fails if a
test has no registered `TCK-<AREA>-<NUMBER>` reference.

| Case ID | Area | Behavior verified |
| --- | --- | --- |
| `TCK-EXEC-001` | Execution | A typed zero-row query completes successfully. |
| `TCK-EXEC-002` | Execution | Named SQL parameters bind as typed expressions. |
| `TCK-ANALYZE-001` | Analysis | Required relation schema and properties are available. |
| `TCK-WIRE-001` | Wire protocol | A direct `ExecutePlan` Range request returns typed Arrow results. |
| `TCK-WIRE-002` | Wire protocol | A direct `AnalyzePlan` Schema request returns the Range schema. |
| `TCK-WIRE-003` | Wire protocol | Direct `Config` Set/Get requests preserve session state. |
| `TCK-WIRE-004` | Wire protocol | Direct `AddArtifacts` and `ArtifactStatus` requests round-trip an artifact. |
| `TCK-WIRE-005` | Wire protocol | Direct `Interrupt` and `GetStatus` requests report an idle session. |
| `TCK-WIRE-006` | Wire protocol | A reattachable request supports direct `ReattachExecute` and `ReleaseExecute`. |
| `TCK-WIRE-007` | Wire protocol | A direct `ReleaseSession` request releases an established session. |
| `TCK-WIRE-008` | Wire protocol | A direct `FetchErrorDetails` request returns the defined unknown-ID response. |
| `TCK-WIRE-009` | Wire protocol | A direct `CloneSession` request copies configuration into the clone. |
| `TCK-WIRE-010` | Relations/expressions | A direct Range/Filter/Project/Sort/Limit plan returns its expected rows. |
| `TCK-WIRE-011` | Relations/expressions | A direct Range/Project/Aggregate/Sort plan returns grouped totals. |
| `TCK-WIRE-012` | Relations/expressions | Direct Join and SetOperation plans preserve their expected row sets. |
| `TCK-WIRE-013` | Relations/expressions | A direct ordered Offset/Tail plan returns its final rows. |
| `TCK-WIRE-014` | Relations/expressions | Direct boolean, conditional, and cast expressions retain typed values. |
| `TCK-SESSION-001` | Sessions | Runtime configuration and temporary views are session-isolated. |
| `TCK-CONFIG-001` | Configuration | `spark.sql.session.timeZone` is settable, readable, and observable. |
| `TCK-CONFIG-002` | Configuration | SQL `SET`/`RESET` and `RuntimeConfig` share session state. |
| `TCK-CATALOG-001` | Catalog | A temporary view is queryable, listed, and removable. |
| `TCK-CATALOG-002` | Catalog | The current database is visible through catalog metadata. |
| `TCK-REL-001` | Relations | Range, filter, projection, aggregation, and ordering preserve results. |
| `TCK-REL-002` | Relations | Joins and set operations preserve their rows. |
| `TCK-REL-003` | Relations | NA fill, drop, and replace preserve null semantics. |
| `TCK-TYPE-001` | Data types | Required primitive, temporal, and complex types preserve schemas and values. |
| `TCK-TYPE-002` | Data types | Nulls, empty strings, empty arrays, and null elements stay distinct. |
| `TCK-TYPE-003` | Data types | NaN, infinities, and signed zero round-trip. |
| `TCK-SQL-001` | SQL | CTE, `VALUES`, `UNION ALL`, ordering, and limit forms interoperate. |
| `TCK-SQL-002` | SQL | `INTERSECT` and `EXCEPT ALL` follow set semantics. |
| `TCK-SQL-003` | SQL | Joins, grouping, and `HAVING` interoperate. |
| `TCK-PRESENTATION-001` | Presentation | `DataFrame.show()` produces the required table rendering. |

Cases use public PySpark Connect APIs, SQL rows, and a small direct generated
protobuf/gRPC slice in the draft's required profile. The wire cases construct
their `Plan` and RPC requests themselves; they do not route those calls through
`SparkSession`. The TCK does not start, configure, or manage the server being
tested.

The direct cases cover every request in the baseline Spark Connect service
inventory: `ExecutePlan`, `AnalyzePlan`, `Config`, `AddArtifacts`,
`ArtifactStatus`, `Interrupt`, `ReattachExecute`, `ReleaseExecute`,
`ReleaseSession`, `FetchErrorDetails`, `CloneSession`, and `GetStatus`. A unit
test enforces this inventory-to-case mapping. Each case verifies a successful
or specified empty response; it does not make all optional request fields or
extension messages mandatory.

## Run it

Use Python 3.10 or later and point the suite at a running Spark Connect
endpoint:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
pytest tests/unit
pytest tests/tck --spark-connect-url sc://localhost:15002
```

The target can also be supplied with `SPARK_CONNECT_URL`:

```bash
SPARK_CONNECT_URL=sc://spark-connect.example:15002 pytest tests/tck
```

When no target is supplied, TCK cases are skipped rather than silently testing
an in-process Spark session. Run only the quick baseline with:

```bash
pytest -m smoke --spark-connect-url sc://localhost:15002
```

## Adding a case

1. Add a `TckCase` with a stable ID, `SC-1.0-P1` manifest, and exact manifest
   rows in `src/spark_connect_tck/spec.py`.
2. Put the test in `tests/tck/`, mark it `tck_case("TCK-AREA-NNN")`, and add
   `smoke` only when it is deterministic and fast.
3. Isolate all server state with a unique name and clean it up in `finally`.
4. Use only behavior required by the cited rows. Do not turn an optional or
   extension surface into a positive conformance requirement.

## Development

```bash
python -m pip install -e '.[test,lint]'
ruff check .
pytest
```

GitHub Actions runs the unit suite and linting without requiring a live Spark
Connect service. Target-server conformance runs are intentionally explicit.
