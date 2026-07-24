"""Traceability metadata for the Spark Connect 1.0 draft TCK.

The specification is the normative source.  This module intentionally records
only the starter cases implemented by this repository; it is not a replacement
for the complete SC-1.0-P1 manifest.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

SPECIFICATION_VERSION = "1.0 draft v0.8"
REFERENCE_SPARK_VERSION = "4.2.0"
REFERENCE_SPARK_COMMIT = "32f7299601108917fb01920a54e084595b7b3bf8"
SPECIFICATION_URL = (
    "https://docs.google.com/document/d/1FFBrD__93Pdznj4roy2UrDpoRzMQnhjvfxrtChzXPpg/edit"
)

_CASE_ID = re.compile(r"TCK-[A-Z]+-\d{3}$")

# The Spark Connect 4.2 service request inventory.  These are the RPCs that a
# required-profile wire implementation must expose; extension RPCs are out of
# scope until they appear in the specification manifest.
REQUIRED_WIRE_RPCS = frozenset(
    {
        "AddArtifacts",
        "AnalyzePlan",
        "ArtifactStatus",
        "CloneSession",
        "Config",
        "ExecutePlan",
        "FetchErrorDetails",
        "GetStatus",
        "Interrupt",
        "ReattachExecute",
        "ReleaseExecute",
        "ReleaseSession",
    }
)


@dataclass(frozen=True)
class TckCase:
    """A runnable case and its normative SC-1.0-P1 traceability references."""

    case_id: str
    title: str
    manifest: str
    rows: tuple[str, ...]
    rpc_methods: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _CASE_ID.fullmatch(self.case_id):
            raise ValueError(f"Invalid TCK case ID: {self.case_id}")
        if not self.manifest.startswith("SC-1.0-P1"):
            raise ValueError(f"Case {self.case_id} must cite an SC-1.0-P1 manifest")
        if not self.rows:
            raise ValueError(f"Case {self.case_id} must cite at least one manifest row")
        unknown_rpcs = set(self.rpc_methods) - REQUIRED_WIRE_RPCS
        if unknown_rpcs:
            raise ValueError(f"Case {self.case_id} cites unknown RPCs: {sorted(unknown_rpcs)}")


CASES = (
    TckCase(
        "TCK-ANALYZE-001",
        "Required AnalyzePlan operations expose schema and relation properties.",
        "SC-1.0-P1-WIRE",
        (
            "AnalyzePlan / Schema",
            "AnalyzePlan / IsLocal",
            "AnalyzePlan / IsStreaming",
            "AnalyzePlan / InputFiles",
        ),
    ),
    TckCase(
        "TCK-WIRE-001",
        "A direct ExecutePlan protobuf Range request returns typed Arrow results.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / ExecutePlan", "Relations / Range", "Result delivery / Arrow IPC batches"),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-002",
        "A direct AnalyzePlan protobuf Schema request returns the Range schema.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / AnalyzePlan", "AnalyzePlan / Schema", "Relations / Range"),
        ("AnalyzePlan",),
    ),
    TckCase(
        "TCK-WIRE-003",
        "Direct Config Set and Get protobuf requests preserve session state.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / Config", "Configuration / spark.sql.session.timeZone"),
        ("Config",),
    ),
    TckCase(
        "TCK-WIRE-004",
        "Direct artifact upload and status protobuf requests round-trip a session artifact.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / AddArtifacts", "gRPC RPCs / ArtifactStatus"),
        ("AddArtifacts", "ArtifactStatus"),
    ),
    TckCase(
        "TCK-WIRE-005",
        "Direct interrupt and operation-status protobuf requests report an idle session.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / Interrupt", "gRPC RPCs / GetStatus"),
        ("Interrupt", "GetStatus"),
    ),
    TckCase(
        "TCK-WIRE-006",
        "A reattachable execution can be reattached to and explicitly released.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / ReattachExecute", "gRPC RPCs / ReleaseExecute"),
        ("ReattachExecute", "ReleaseExecute"),
    ),
    TckCase(
        "TCK-WIRE-007",
        "A direct release-session protobuf request releases an established session.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / ReleaseSession",),
        ("ReleaseSession",),
    ),
    TckCase(
        "TCK-WIRE-008",
        "A direct fetch-error-details protobuf request reports an unknown error ID precisely.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / FetchErrorDetails",),
        ("FetchErrorDetails",),
    ),
    TckCase(
        "TCK-WIRE-009",
        "A direct clone-session protobuf request copies session configuration to the clone.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / CloneSession",),
        ("CloneSession",),
    ),
    TckCase(
        "TCK-WIRE-010",
        "A direct plan composes Range, Filter, Project, Sort, and Limit relations.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Range",
            "Relations / Filter",
            "Relations / Project",
            "Relations / Sort",
            "Relations / Limit",
            "Expressions / Literal",
            "Expressions / UnresolvedAttribute",
            "Expressions / UnresolvedFunction",
            "Expressions / Alias",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-011",
        "A direct plan groups and aggregates expressions before sorting its result.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Range",
            "Relations / Project",
            "Relations / Aggregate",
            "Relations / Sort",
            "Expressions / UnresolvedAttribute",
            "Expressions / UnresolvedFunction",
            "Expressions / Alias",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-012",
        "Direct Join and SetOperation plans preserve deterministic row sets.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Range",
            "Relations / Join",
            "Relations / SetOperation",
            "Relations / Sort",
            "Expressions / UnresolvedAttribute",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-013",
        "A direct plan applies Offset and Tail after a deterministic sort.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Range",
            "Relations / Sort",
            "Relations / Offset",
            "Relations / Tail",
            "Expressions / UnresolvedAttribute",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-014",
        "Direct boolean, conditional, and cast expressions retain typed values.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Range",
            "Relations / Filter",
            "Relations / Project",
            "Expressions / Literal",
            "Expressions / UnresolvedAttribute",
            "Expressions / UnresolvedFunction",
            "Expressions / Alias",
            "Expressions / Cast",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-015",
        "Direct Arrow-backed local and NA-relation plans preserve null semantics.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / LocalRelation",
            "Relations / NAFill",
            "Relations / NADrop",
            "Relations / NAReplace",
            "Relations / Sort",
            "Expressions / Literal",
            "Expressions / UnresolvedAttribute",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-016",
        "Direct column-mutation and deduplication relation plans preserve rows and schema.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / LocalRelation",
            "Relations / WithColumns",
            "Relations / WithColumnsRenamed",
            "Relations / Drop",
            "Relations / Deduplicate",
            "Relations / Sort",
            "Expressions / Literal",
            "Expressions / UnresolvedAttribute",
            "Expressions / UnresolvedFunction",
            "Expressions / Alias",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-EXEC-001",
        "Empty tabular execution preserves its schema and completes successfully.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / ExecutePlan", "Result delivery / Arrow IPC batches"),
    ),
    TckCase(
        "TCK-EXEC-002",
        "SQL named parameters are bound as typed expressions rather than text.",
        "SC-1.0-P1-WIRE",
        ("Commands / SqlCommand", "Execution / SQL execution"),
    ),
    TckCase(
        "TCK-SESSION-001",
        "Session configuration and temporary views are isolated between sessions.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / CloneSession", "Configuration", "Catalog relations"),
    ),
    TckCase(
        "TCK-CONFIG-001",
        "The portable session time-zone key is settable, readable, and observable.",
        "SC-1.0-P1-WIRE",
        ("Configuration / spark.sql.session.timeZone",),
    ),
    TckCase(
        "TCK-CONFIG-002",
        "SQL SET and RESET share session state with RuntimeConfig.",
        "SC-1.0-P1-SQL",
        ("SQL-CONFIG",),
    ),
    TckCase(
        "TCK-CATALOG-001",
        "A session temporary view is queryable, listed, and removable.",
        "SC-1.0-P1-WIRE",
        ("Catalog relations / ListTables", "Catalog relations / DropTempView"),
    ),
    TckCase(
        "TCK-CATALOG-002",
        "Current database and database listings use the session catalog state.",
        "SC-1.0-P1-WIRE",
        ("Catalog relations / CurrentDatabase", "Catalog relations / ListDatabases"),
    ),
    TckCase(
        "TCK-REL-001",
        "Range, filter, projection, aggregation, ordering, and collection preserve rows.",
        "SC-1.0-P1-WIRE",
        ("Relations / Range", "Relations / Filter", "Relations / Project", "Relations / Aggregate"),
    ),
    TckCase(
        "TCK-REL-002",
        "Join and set-operation relations preserve the expected row set.",
        "SC-1.0-P1-WIRE",
        ("Relations / Join", "Relations / SetOperation", "Relations / Sort"),
    ),
    TckCase(
        "TCK-REL-003",
        "NA fill, drop, and replace preserve Spark null and type semantics.",
        "SC-1.0-P1-WIRE",
        ("Relations / NAFill", "Relations / NADrop", "Relations / NAReplace"),
    ),
    TckCase(
        "TCK-TYPE-001",
        "Required scalar and complex SQL types are preserved in result schemas and values.",
        "SC-1.0-P1-WIRE",
        ("Data types", "Result delivery / schema", "Result delivery / Arrow IPC batches"),
    ),
    TckCase(
        "TCK-TYPE-002",
        "Null, empty strings, empty arrays, and null array elements remain distinct.",
        "SC-1.0-P1-WIRE",
        ("Data types / Null", "Data types / String", "Data types / Array"),
    ),
    TckCase(
        "TCK-TYPE-003",
        "Double values preserve NaN, infinities, and signed zero.",
        "SC-1.0-P1-WIRE",
        ("Data types / Float", "Data types / Double"),
    ),
    TckCase(
        "TCK-SQL-001",
        "Core CTE, VALUES, set, ordering, and limit SQL forms produce deterministic rows.",
        "SC-1.0-P1-SQL",
        (
            "SQL-QRY-WITH",
            "SQL-QRY-VALUES",
            "SQL-QRY-SELECT",
            "SQL-QRY-SET",
            "SQL-QRY-ORDER",
            "SQL-QRY-LIMIT",
        ),
    ),
    TckCase(
        "TCK-SQL-002",
        "INTERSECT and EXCEPT ALL follow the required set-operation semantics.",
        "SC-1.0-P1-SQL",
        ("SQL-QRY-SET", "SQL-QRY-ORDER"),
    ),
    TckCase(
        "TCK-SQL-003",
        "SELECT supports joins, grouping, and HAVING over required expressions.",
        "SC-1.0-P1-SQL",
        ("SQL-QRY-SELECT",),
    ),
    TckCase(
        "TCK-PRESENTATION-001",
        "DataFrame.show renders the required ShowString table output.",
        "SC-1.0-P1-WIRE",
        ("Relations / ShowString",),
    ),
)

CASES_BY_ID = {case.case_id: case for case in CASES}
IMPLEMENTED_WIRE_RPCS = frozenset(rpc_method for case in CASES for rpc_method in case.rpc_methods)


def get_case(case_id: str) -> TckCase:
    """Return one registered starter case, failing clearly for an unknown ID."""
    try:
        return CASES_BY_ID[case_id]
    except KeyError as error:
        raise KeyError(f"Unknown Spark Connect TCK case: {case_id}") from error
