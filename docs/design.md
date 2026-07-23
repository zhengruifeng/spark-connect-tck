# Design traceability

The source design is the Spark Connect TCK document:

<https://docs.google.com/document/d/1FFBrD__93Pdznj4roy2UrDpoRzMQnhjvfxrtChzXPpg/edit?tab=t.0#heading=h.lweyg2cvmawz>

## Current implementation status

The source document is accessible to the TCK environment. Its v0.8 draft
defines `SC-1.0-P1` as the sole normative required-profile manifest, pinned to
Apache Spark 4.2.0 at commit
`32f7299601108917fb01920a54e084595b7b3bf8`. The profile combines wire,
client, provider, function, and SQL sub-manifests. A passing target must pass
the complete required profile; per-area results are diagnostic only.

This repository contains a basic executable slice of the wire and SQL
sub-manifests. Its cases are registered in `spark_connect_tck.spec`, where
each one records its `SC-1.0-P1` manifest and rows. Pytest collection rejects
any live-target test that is not registered there.

The starter slice covers:

1. `ExecutePlan` user-visible zero-row completion and schema preservation.
2. Required `AnalyzePlan` schema, local, streaming, and input-files operations.
3. Typed named SQL parameters.
4. Session isolation for runtime configuration and temporary views.
5. RuntimeConfig and SQL `SET`/`RESET` time-zone behavior.
6. Temporary-view lifecycle and visible current-database metadata.
7. Selected required Range, Filter, Project, Aggregate, Sort, Join,
   SetOperation, NAFill, NADrop, and NAReplace relations.
8. Required scalar, temporal, complex, null, empty-value, and IEEE 754
   special-value behavior.
9. Selected `SQL-QRY-*` rows: WITH, VALUES, SELECT, set operations, joins,
   grouping, HAVING, ordering, and limit.
10. Default `ShowString` rendering through the public `DataFrame.show()` API.

This is not a conformance release. In particular, it does not yet implement
the linked `SC-1.0-P1-CLIENT` overload manifest, the exhaustive function and
provider manifests, raw gRPC/error-envelope assertions, Arrow framing,
authorization harness, full catalog/configuration coverage, or the complete
relation/expression/command matrix.

## Test contract

Each test case must have a stable `TCK-<AREA>-<NUMBER>` identifier, a concise
normative contract, an `SC-1.0-P1` manifest, and at least one exact manifest
row. Cases use the public PySpark Connect API where it exposes the specified
wire behavior. Cases must not assume that the target has a local filesystem, a
particular catalog, or Databricks-specific SQL extensions.
