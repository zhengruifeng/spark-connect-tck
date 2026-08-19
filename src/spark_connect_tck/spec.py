"""Traceability metadata for the Spark Connect 1.0 draft TCK.

The specification is the normative source.  This module intentionally records
only the starter cases implemented by this repository; it is not a replacement
for the complete SC-1.0-P1 manifest.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

SPECIFICATION_VERSION = "1.0 draft v0.20"
REFERENCE_SPARK_VERSION = "4.2.0"
REFERENCE_SPARK_COMMIT = "32f7299601108917fb01920a54e084595b7b3bf8"
SPECIFICATION_URL = (
    "https://docs.google.com/document/d/1FFBrD__93Pdznj4roy2UrDpoRzMQnhjvfxrtChzXPpg/edit"
)

_CASE_ID = re.compile(r"TCK-[A-Z]+-\d{3}$")

# The v0.20 SC-1.0-P1 service request inventory. Optional and deferred RPCs
# intentionally do not contribute to a core conformance result.
REQUIRED_WIRE_RPCS = frozenset(
    {
        "AnalyzePlan",
        "CloneSession",
        "Config",
        "ExecutePlan",
        "FetchErrorDetails",
        "GetStatus",
        "Interrupt",
        "ReleaseSession",
    }
)
OPTIONAL_WIRE_RPCS = frozenset({"ReattachExecute", "ReleaseExecute"})
DEFERRED_WIRE_RPCS = frozenset({"AddArtifacts", "ArtifactStatus"})


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
        "TCK-WIRE-001",
        "A direct ExecutePlan protobuf Range request returns typed Arrow results.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / ExecutePlan", "Relations / Range", "Result delivery / Arrow IPC batches"),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-002",
        "Direct requests exercise every AnalyzePlan operation required by v0.20.",
        "SC-1.0-P1-WIRE",
        (
            "gRPC RPCs / AnalyzePlan",
            "AnalyzePlan / Schema",
            "AnalyzePlan / Explain",
            "AnalyzePlan / TreeString",
            "AnalyzePlan / IsLocal",
            "AnalyzePlan / IsStreaming",
            "AnalyzePlan / InputFiles",
            "AnalyzePlan / SparkVersion",
            "AnalyzePlan / DDLParse",
            "Relations / Range",
        ),
        ("AnalyzePlan",),
    ),
    TckCase(
        "TCK-WIRE-003",
        "Direct requests exercise every Config operation in one session.",
        "SC-1.0-P1-WIRE",
        (
            "gRPC RPCs / Config",
            "Configuration / Set",
            "Configuration / Get",
            "Configuration / GetWithDefault",
            "Configuration / GetOption",
            "Configuration / GetAll",
            "Configuration / Unset",
            "Configuration / IsModifiable",
            "Configuration / spark.sql.session.timeZone",
        ),
        ("Config",),
    ),
    TckCase(
        "TCK-WIRE-005",
        "Direct interrupt and operation-status protobuf requests report an idle session.",
        "SC-1.0-P1-WIRE",
        ("gRPC RPCs / Interrupt", "gRPC RPCs / GetStatus"),
        ("Interrupt", "GetStatus"),
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
            "Expressions / ExpressionString",
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
        "TCK-WIRE-017",
        "Direct partitioning, aliasing, renaming, and sample relation plans preserve rows.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Range",
            "Relations / Repartition",
            "Relations / SubqueryAlias",
            "Relations / ToDF",
            "Relations / RepartitionByExpression",
            "Relations / Sample",
            "Relations / Sort",
            "Expressions / UnresolvedAttribute",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-018",
        "Direct schema replacement and unpivot relation plans preserve typed values.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / LocalRelation",
            "Relations / ToSchema",
            "Relations / Unpivot",
            "Relations / Sort",
            "Data types",
            "Expressions / UnresolvedAttribute",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-020",
        "Direct hint and transpose relation plans preserve their deterministic results.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Hint",
            "Relations / Transpose",
            "Relations / LocalRelation",
            "Relations / Range",
            "Expressions / Literal",
            "Expressions / UnresolvedAttribute",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-021",
        "Direct statistics relations return exact summary, correlation, quantile, and sample data.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Project",
            "Relations / Sort",
            "Relations / StatSummary",
            "Relations / StatCov",
            "Relations / StatCorr",
            "Relations / StatApproxQuantile",
            "Relations / StatSampleBy",
            "Expressions / Literal",
            "Expressions / UnresolvedAttribute",
            "Expressions / UnresolvedFunction",
            "Expressions / Alias",
            "Expressions / SortOrder",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-022",
        "A direct view command, named-table read, and catalog relations share session state.",
        "SC-1.0-P1-WIRE",
        (
            "Commands / CreateDataFrameViewCommand",
            "Relations / Read",
            "Relations / Range",
            "Catalog relations / TableExists",
            "Catalog relations / ListColumns",
            "Catalog relations / DropTempView",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-024",
        "Direct scalar and transform expressions cover the closed required function kernel.",
        "SC-1.0-P1-FUNCTIONS",
        (
            "Functions / abs",
            "Functions / coalesce",
            "Functions / nullif",
            "Functions / lower",
            "Functions / upper",
            "Functions / length",
            "Functions / substring",
            "Functions / substr",
            "Functions / concat",
            "Functions / trim",
            "Functions / transform",
            "Expressions / LambdaFunction",
            "Expressions / UnresolvedNamedLambdaVariable",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-025",
        "Direct Parse, star, and framed-window plans preserve structured results.",
        "SC-1.0-P1-WIRE",
        (
            "Relations / Parse",
            "Expressions / UnresolvedStar",
            "Expressions / Window",
            "Expressions / SortOrder",
            "Functions / sum",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-WIRE-026",
        "Arrow variable-width selection applies recursively to input and result values.",
        "SC-1.0-P1-ARROW",
        (
            "Configuration / spark.sql.execution.arrow.useLargeVarTypes",
            "ARROW-STRING",
            "ARROW-BINARY",
            "ARROW-ARRAY",
            "ARROW-MAP",
            "ARROW-STRUCT",
            "Result delivery / LocalRelation input",
            "Result delivery / ExecutePlan Arrow results",
            "Appendix B.1 / variable-width type selection",
        ),
        ("Config", "ExecutePlan"),
    ),
    TckCase(
        "TCK-SQL-001",
        "Direct Relation.SQL plans cover the Portable SQL Core query and parameter families.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "Relations / SQL",
            "Appendix I.2 / Lexical elements",
            "Appendix I.3 / SELECT",
            "Appendix I.3 / VALUES",
            "Appendix I.3 / Joins",
            "Appendix I.3 / WHERE",
            "Appendix I.3 / GROUP BY",
            "Appendix I.3 / HAVING",
            "Appendix I.3 / UNION ALL",
            "Appendix I.3 / ORDER BY",
            "Appendix I.3 / LIMIT and OFFSET",
            "Appendix I.4 / Operators and functions",
            "Appendix I.5 / Query semantics",
            "Appendix I.6 / Named parameters",
            "Appendix I.6 / Positional parameters",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-SQL-002",
        "A portable query sent through SqlCommand returns an equivalent lazy relation.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "Commands / SqlCommand",
            "Relations / SQL",
            "Appendix I.6 / Named parameters",
            "Appendix I.6 / SqlCommandResult.relation",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-SQL-003",
        "Portable SQL casts produce exact scalar, decimal, date, and timestamp types.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "SQL-TYPE-BOOLEAN / CAST-SQL-BOOLEAN",
            "SQL-TYPE-SMALLINT / CAST-SQL-SMALLINT",
            "SQL-TYPE-INTEGER / CAST-SQL-INTEGER",
            "SQL-TYPE-BIGINT / CAST-SQL-BIGINT",
            "SQL-TYPE-REAL / CAST-SQL-REAL",
            "SQL-TYPE-DOUBLE / CAST-SQL-DOUBLE",
            "SQL-TYPE-DECIMAL / CAST-SQL-DECIMAL",
            "SQL-TYPE-DATE / CAST-SQL-DATE",
            "SQL-TYPE-TIMESTAMP / CAST-SQL-TIMESTAMP",
            "Appendix I.4 / Portable type aliases",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-SQL-004",
        "Portable SQL bare VARCHAR maps to an unbounded UTF8_BINARY String.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "SQL-TYPE-VARCHAR",
            "CAST-SQL-VARCHAR",
            "Appendix I.4 / VARCHAR is String with UTF8_BINARY",
            "Appendix I.9 / bare VARCHAR coverage",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-SQL-005",
        "Portable SQL TIMESTAMP remains Timestamp across time zones and timestamp defaults.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "SQL-TYPE-TIMESTAMP",
            "CAST-SQL-TIMESTAMP",
            "Configuration / spark.sql.session.timeZone",
            "Appendix I.4 / TIMESTAMP independence from spark.sql.timestampType",
            "Appendix I.9 / UTC and non-UTC TIMESTAMP coverage",
        ),
        ("Config", "ExecutePlan"),
    ),
    TckCase(
        "TCK-SQL-006",
        "Portable SQL expressions obey the required precedence and associativity rules.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "SQL-PREC-PRIMARY",
            "SQL-PREC-UNARY",
            "SQL-PREC-MULTIPLICATIVE",
            "SQL-PREC-ADDITIVE",
            "SQL-PREC-PREDICATE",
            "SQL-PREC-NOT",
            "SQL-PREC-AND",
            "SQL-PREC-OR",
            "Appendix I.9 / precedence and associativity coverage",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-SQL-007",
        "Portable SQL rejects unparenthesized chained predicates.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "SQL-PREC-PREDICATE",
            "Appendix I.9 / chained-predicate rejection",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-SQL-008",
        "Portable SQL lexical productions preserve their exact token semantics.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "Appendix I.2 / ascii_whitespace",
            "Appendix I.2 / identifier_part",
            "Appendix I.2 / integer_literal",
            "Appendix I.2 / decimal_literal and exponent_part",
            "Appendix I.2 / string_literal and string_unit",
            "Appendix I.2 / boolean_literal and null_literal",
            "Appendix I.2 / longest-token matching",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-SQL-009",
        "Portable SQL function spellings remain identifiers outside call position.",
        "SC-1.0-P1-PORTABLE-SQL",
        (
            "Appendix I.2 / contextual function names",
            "Appendix I.4 / scalar_function_call",
            "Appendix I.9 / contextual function-name coverage",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-EXPR-001",
        "ExpressionString obeys the required precedence and associativity rules.",
        "SC-1.0-P1-EXPRESSION-SYNTAX",
        (
            "EXPR-PREC-PRIMARY",
            "EXPR-PREC-UNARY",
            "EXPR-PREC-MULTIPLICATIVE",
            "EXPR-PREC-ADDITIVE",
            "EXPR-PREC-PREDICATE",
            "EXPR-PREC-NOT",
            "EXPR-PREC-AND",
            "EXPR-PREC-OR",
            "Appendix I.9 / precedence and associativity coverage",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-EXPR-002",
        "ExpressionString rejects unparenthesized chained predicates.",
        "SC-1.0-P1-EXPRESSION-SYNTAX",
        (
            "EXPR-PREC-PREDICATE",
            "Appendix I.9 / chained-predicate rejection",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-EXPR-003",
        "ExpressionString lexical productions preserve quoted and multipart attributes.",
        "SC-1.0-P1-EXPRESSION-SYNTAX",
        (
            "Appendix I.2 / reused literal and identifier productions",
            "Appendix I.8 / expr_attribute_reference",
            "Appendix I.8 / backtick_identifier and backtick_unit",
            "Appendix I.8 / non_backtick_scalar",
            "Appendix I.8 / expr_cast_expression",
        ),
        ("ExecutePlan",),
    ),
    TckCase(
        "TCK-EXPR-004",
        "ExpressionString function spellings remain attributes outside call position.",
        "SC-1.0-P1-EXPRESSION-SYNTAX",
        (
            "Appendix I.2 / contextual function names",
            "Appendix I.8 / expr_scalar_function_call",
            "Appendix I.9 / contextual function-name coverage",
        ),
        ("ExecutePlan",),
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
