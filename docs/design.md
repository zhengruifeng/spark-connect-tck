# Design traceability

The source design is the Spark Connect specification document:

<https://docs.google.com/document/d/1FFBrD__93Pdznj4roy2UrDpoRzMQnhjvfxrtChzXPpg/edit?tab=t.0#heading=h.lweyg2cvmawz>

## Current implementation status

The source document is working draft v0.14, updated 2026-08-07. It defines
`SC-1.0-P1` as the sole required profile and pins the reference implementation
to Apache Spark 4.2.0 at commit
`32f7299601108917fb01920a54e084595b7b3bf8`.

The v0.14 scope differs materially from the earlier v0.8 draft. The required
RPCs are `ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt`, `ReleaseSession`,
`FetchErrorDetails`, `CloneSession`, and `GetStatus`. Reattachment RPCs are
optional, artifact RPCs are deferred, presentation relations are excluded, and
complete SQL dialects remain optional. Unlike v0.13, v0.14 requires a small
Portable SQL Core for `SELECT` and `VALUES` queries through `Relation.SQL` and
the query-returning `SqlCommand.input` path.

Final profile membership will be controlled by
`docs/spark-connect/spec/manifests/sc-1.0-p1.json` at an immutable Apache Spark
publication commit. That bundle, its commit, and its whole-file SHA-256 are not
published yet. The references in `spark_connect_tck.spec` therefore identify
draft sections rather than final canonical row IDs. This repository cannot be
a conformance release until those pins exist and every test cites them.

The current core slice constructs protobuf requests directly and covers:

1. All eight required service RPCs.
2. All eight required `AnalyzePlan` operations: Schema, Explain, TreeString,
   IsLocal, IsStreaming, InputFiles, SparkVersion, and DDLParse.
3. All seven `Config` operations and required time-zone key behavior.
4. Range, LocalRelation, Project, Filter, Sort, Limit, Aggregate, Join,
   SetOperation, Offset, Tail, NA, column-shape, deduplication, repartitioning,
   Sample, ToSchema, Unpivot, Hint, Transpose, Read, Parse, and selected Catalog
   relations.
5. CreateDataFrameViewCommand with named-table and Catalog visibility.
6. Direct Arrow result decoding, schema checks, and response identity checks.
7. Required aggregate functions and the v0.14 scalar kernel, including a
   worker-free `transform` lambda.
8. Direct ExpressionString CASE, cast, star, and framed-window expressions.
9. Summary, covariance, correlation, exact quantile, and stratified-sample
   statistical relations.
10. A direct positive Portable SQL slice covering `SELECT`, `VALUES`, required
    joins and clauses, `UNION ALL`, scalar and aggregate functions, named and
    positional typed parameters, and `SqlCommandResult.relation`.

Historical PySpark and Spark SQL 4.2 checks live under `tests/optional`. They
remain useful implementation probes but do not contribute to an SC-1.0-P1
result.

This is still a starter slice. Major missing areas include the canonical
client overload rows, providers and writes, the remaining relation and
expression variants and field-domain branches, full function,
ExpressionString, and Portable SQL positive/negative corpora, cast modes,
Arrow mappings, structured error envelopes, authorization, configuration-key
effects, complete Catalog operations/result schemas, and positive/negative
command coverage.

## Test contract

Each core case has a stable `TCK-<AREA>-<NUMBER>` identifier, a concise
contract, an `SC-1.0-P1` member, and at least one draft reference. Core relation
and expression cases construct protobuf plans directly. Tests must not require
a shared client/server filesystem, a particular catalog implementation, a
complete Spark SQL dialect, presentation formatting, or vendor-specific
extensions.

Once the canonical v0.14 bundle is published, draft references must be replaced
with exact row IDs and the repository must record the bundle publication
commit, path, and whole-file digest.
