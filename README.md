# Spark Connect TCK

A Python implementation of a Technology Compatibility Kit (TCK) for Spark
Connect servers. Core cases construct generated gRPC/protobuf requests directly
and run them against a separately managed target server.

The intended implementation requirements are recorded in the [Spark Connect
1.0 draft](https://docs.google.com/document/d/1FFBrD__93Pdznj4roy2UrDpoRzMQnhjvfxrtChzXPpg/edit?tab=t.0#heading=h.lweyg2cvmawz).
This repository tracks working draft v0.20 and implements a deliberately small,
traceable slice of its `SC-1.0-P1` required profile, pinned to Apache Spark
4.2.0. It is not a complete conformance suite and must not be used to make a
Spark Connect 1.0 compatibility claim.

## Scope

The core suite establishes a portable protocol-first baseline. Every core
live-target test is registered with its draft manifest references; collection
fails if a test has no registered `TCK-<AREA>-<NUMBER>` reference.

| Case ID | Area | Behavior verified |
| --- | --- | --- |
| `TCK-WIRE-001` | Wire protocol | A direct `ExecutePlan` Range request returns typed Arrow results. |
| `TCK-WIRE-002` | Wire protocol | Direct requests exercise all eight required `AnalyzePlan` operations. |
| `TCK-WIRE-003` | Wire protocol | Direct requests exercise all seven `Config` operations. |
| `TCK-WIRE-005` | Wire protocol | Direct `Interrupt` and `GetStatus` requests report an idle session. |
| `TCK-WIRE-007` | Wire protocol | A direct `ReleaseSession` request releases an established session. |
| `TCK-WIRE-008` | Wire protocol | A direct `FetchErrorDetails` request returns the defined unknown-ID response. |
| `TCK-WIRE-009` | Wire protocol | A direct `CloneSession` request copies configuration into the clone. |
| `TCK-WIRE-010` | Relations/expressions | A direct Range/Filter/Project/Sort/Limit plan returns its expected rows. |
| `TCK-WIRE-011` | Relations/expressions | Direct aggregate plans cover count, sum, average, minimum, and maximum. |
| `TCK-WIRE-012` | Relations/expressions | Direct Join and SetOperation plans preserve their expected row sets. |
| `TCK-WIRE-013` | Relations/expressions | A direct ordered Offset/Tail plan returns its final rows. |
| `TCK-WIRE-014` | Relations/expressions | Direct Boolean, ExpressionString CASE, and cast expressions retain typed values. |
| `TCK-WIRE-015` | Relations/expressions | Direct Arrow-backed LocalRelation and NA plans preserve null semantics. |
| `TCK-WIRE-016` | Relations/expressions | Direct column mutation and deduplication plans preserve rows and schemas. |
| `TCK-WIRE-017` | Relations/expressions | Direct partitioning, aliasing, renaming, and sample plans preserve rows. |
| `TCK-WIRE-018` | Relations/expressions | Direct schema replacement and unpivot plans preserve typed values. |
| `TCK-WIRE-020` | Relations/expressions | Direct hint and transpose plans preserve deterministic results. |
| `TCK-WIRE-021` | Relations/expressions | Direct statistics plans return exact summary, correlation, quantile, and sample data. |
| `TCK-WIRE-022` | Commands/catalog | Direct temp-view creation, named-table reads, and catalog relations share session state. |
| `TCK-WIRE-024` | Functions/expressions | Direct plans cover the required scalar kernel and transform lambda binding. |
| `TCK-WIRE-025` | Relations/expressions | Direct Parse, star, and framed-window plans preserve structured results. |
| `TCK-WIRE-026` | Arrow mappings | Narrow/large String and Binary values round-trip recursively through nested types. |
| `TCK-SQL-001` | Portable SQL | Direct `Relation.SQL` plans cover required queries, joins, clauses, functions, and typed parameters. |
| `TCK-SQL-002` | Portable SQL | A direct `SqlCommand.input` returns an equivalent executable relation. |
| `TCK-SQL-003` | Portable SQL types | Direct casts assert exact Boolean, numeric, decimal, date, and timestamp schemas. |
| `TCK-SQL-004` | Portable SQL types | Bare `VARCHAR` must produce an unbounded `UTF8_BINARY` String. |
| `TCK-SQL-005` | Portable SQL types | `TIMESTAMP` remains zoned under UTC/non-UTC and both Spark timestamp defaults. |
| `TCK-SQL-006` | Portable SQL expressions | Direct SQL verifies all precedence levels and arithmetic associativity. |
| `TCK-SQL-007` | Portable SQL expressions | Comparison and null-test predicates must not chain. |
| `TCK-SQL-008` | Portable SQL lexer | ASCII whitespace, identifiers, literals, and longest-match operators preserve their meanings. |
| `TCK-SQL-009` | Portable SQL contextual names | Required function spellings remain identifiers outside call position, including mixed case. |
| `TCK-EXPR-001` | ExpressionString | Direct expressions verify all precedence levels and arithmetic associativity. |
| `TCK-EXPR-002` | ExpressionString | Comparison, null-safe comparison, and null-test predicates must not chain. |
| `TCK-EXPR-003` | ExpressionString lexer | Unquoted, multipart, Unicode, and escaped backtick attribute tokens resolve exactly. |
| `TCK-EXPR-004` | ExpressionString contextual names | Required function spellings remain attributes outside call position, including mixed case. |

The direct cases cover all v0.20 required service requests: `ExecutePlan`,
`AnalyzePlan`, `Config`, `Interrupt`, `ReleaseSession`, `FetchErrorDetails`,
`CloneSession`, and `GetStatus`. A unit test enforces this mapping.
`ReattachExecute` and `ReleaseExecute` are optional in v0.20; `AddArtifacts`
and `ArtifactStatus` are deferred. The required Portable SQL Core has fixed
logical aliases and casts; complete Spark SQL semantics, DDL/DML, and
presentation relations remain outside the core profile.

Draft v0.15 exposes two type-related reference gaps in Apache Spark 4.2.0: bare
`VARCHAR` is rejected as missing a length, and `spark.sql.timestampType` can
change portable `TIMESTAMP` into `TimestampNTZ`. Draft v0.16 additionally
requires chained predicates to be rejected, while Spark accepts chained `=`,
`==`, and `<=>` comparisons. The corresponding required cases remain active,
so a full live run reports those mismatches. Run the currently
reference-compatible slice with `-m "not reference_gap"`.

The draft's canonical JSON bundle, publication commit, and SHA-256 are not yet
published. Case references are therefore section-level draft references rather
than final canonical row IDs. This is an explicit release blocker, not a gap
the TCK fills by inventing IDs.

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

Historical PySpark/Spark SQL 4.2 smoke checks are retained as non-conformance
probes and can be run explicitly:

```bash
pytest tests/optional --spark-connect-url sc://localhost:15002
```

## Adding a case

1. Add a `TckCase` with a stable ID, `SC-1.0-P1` manifest, and the narrowest
   available draft references in `src/spark_connect_tck/spec.py`. Replace them
   with canonical row IDs once the v0.20 bundle is published.
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
