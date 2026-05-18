"""mdde-sql-process — folder of SQL in, optimised SQL + mapping metadata out.

A standalone, sqlglot-only pipeline (no DuckDB) that:

  1. Reads every ``*.sql`` file in an input folder
  2. Extracts MDDE SQL-First annotations (``-- @mdde-entity``, ``-- @pk``,
     ``-- @pii``, ``-- @derived``, ``-- @fk(...)``, etc.)
  3. Parses the SQL via sqlglot and walks the AST to build column-level
     lineage from output columns back to source columns
  4. Runs quality checks (SELECT *, CARTESIAN_JOIN, ORDER_BY_NUMBER,
     WINDOW_NO_ORDER, WHERE 1=1, HARDCODED_DATE, ...) and auto-fixes
     the safe ones; flags the rest in the report
  5. Rewrites the SQL: lifts inline subqueries into CTEs and pushes
     single-table projections + filters into ``<table>_filtered`` /
     ``<table>_prepared`` CTEs
  6. Emits ONE FOLDER PER INPUT QUERY, each containing: ``optimized.sql``
     (with original annotations re-attached), ``mapping.bfm.yaml`` and
     ``mapping.cte.yaml`` (two mapping shapes), ``annotation.entity.yaml``
     (SQL-First entity YAML), ``genie.md`` (natural-language prompt
     describing the ORIGINAL query for Databricks Genie / LLM-backed
     SQL generators), ``findings.md`` (per-query quality findings),
     and ``movement.csv`` (customer-format mapping CSV)
  7. Rolls up cross-file lineage into an OpenLineage event JSON, and
     all per-query movement rows into a run-level ``movement.csv``
  8. Writes a ``report.md`` summarising what was processed, what was
     fixed, what's still flagged, and where the mapping is incomplete

Usage::

    python sql_process.py input/ --out output/

No DuckDB. No metadata persistence between runs. Each invocation is
self-contained and idempotent — running twice on the same input
produces byte-identical output.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Self-contained imports — vendored copies of the optimizer and
# determinism checkers live as siblings (`_optimizer.py`,
# `_determinism.py`). No DuckDB, no mdde_lite parent package, no
# repo-root sys.path dance. Drop these four files into a Databricks
# Workspace folder and the script runs.
_HERE = (
    os.path.dirname(os.path.abspath(__file__))
    if "__file__" in globals() else os.getcwd()
)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import sqlglot
import yaml
from sqlglot import exp


# =============================================================================
# 0. METADATA SCHEMA (optional input — used by sqlglot's qualify() pass)
# =============================================================================


@dataclass
class MetadataSchema:
    """A registry of source tables and their column types.

    Used by sqlglot.optimizer.qualify() to:
      - resolve bare column references to their owning table
      - expand SELECT * into explicit column lists
      - validate that referenced columns actually exist

    Loaded from a YAML file in this shape::

        # _metadata.yaml
        sources:
          customers:
            customer_id: BIGINT
            email: VARCHAR
            name: VARCHAR
          orders:
            order_id: BIGINT
            customer_id: BIGINT
            total: DECIMAL

    Table names may be qualified (``catalog.db.table`` or ``db.table``).
    Column types are dialect-agnostic — sqlglot uses them to resolve
    references, not to validate types.
    """

    tables: Dict[str, Dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: Path) -> "MetadataSchema":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        sources = data.get("sources") or {}
        # Defensive: each table maps to a dict of {col: type}.
        clean: Dict[str, Dict[str, str]] = {}
        for table, cols in sources.items():
            if not isinstance(cols, dict):
                continue
            clean[str(table)] = {
                str(name): str(dtype) for name, dtype in cols.items()
            }
        return cls(tables=clean)

    def to_sqlglot_schema(self) -> Dict[str, Any]:
        """Return the dict shape sqlglot.optimizer.qualify() expects.

        sqlglot's qualify() needs a uniform nesting depth across
        every table. Mixing ``landing.crm_customers_export`` (depth 2)
        with ``raw_customers`` (depth 1) raises a SchemaError. The
        workaround: park unqualified table names under an empty-string
        key (``""``), so every entry sits at depth 2.

        Example output::

            {
                "landing": {
                    "crm_customers_export": {"customer_id": "BIGINT", ...}
                },
                "": {
                    "raw_customers": {"customer_id": "BIGINT", ...}
                }
            }
        """
        out: Dict[str, Any] = {}
        for fq_name, cols in self.tables.items():
            parts = fq_name.split(".")
            if len(parts) == 1:
                cursor = out.setdefault("", {})
                cursor[parts[0]] = dict(cols)
            else:
                cursor: Dict[str, Any] = out
                for part in parts[:-1]:
                    cursor = cursor.setdefault(part, {})
                cursor[parts[-1]] = dict(cols)
        return out

    def __bool__(self) -> bool:
        return bool(self.tables)


def load_metadata(input_dir: Path, override: Optional[Path]) -> MetadataSchema:
    """Resolve which metadata file to use, if any.

    Lookup order:
      1. Explicit ``--metadata <path>`` argument (override)
      2. ``<input_dir>/_metadata.yaml`` if present
      3. Empty schema (no qualification)
    """
    if override is not None:
        if not override.is_file():
            raise FileNotFoundError(f"metadata file not found: {override}")
        return MetadataSchema.from_yaml(override)
    auto = input_dir / "_metadata.yaml"
    if auto.is_file():
        return MetadataSchema.from_yaml(auto)
    return MetadataSchema()


# =============================================================================
# 1. ANNOTATION EXTRACTION
# =============================================================================


# Entity-level annotations live in header comments above the
# CREATE/SELECT statement.
_ENTITY_ANNOTATION_RE = re.compile(
    r"--\s*@mdde-(?P<key>entity|layer|stereotype|description|domain|order-by)\s*:\s*"
    r"(?P<value>.+?)\s*$",
    re.MULTILINE,
)

# Column-level annotations live as trailing comments after the column
# expression. We capture them in two passes — first the comment text,
# then the individual flags inside it.
_COLUMN_ANNOTATION_FLAGS = (
    "pk", "business_key", "fk", "pii", "nullable",
    "derived", "scd2_from", "scd2_to", "scd2_current",
)


@dataclass
class ColumnAnnotations:
    """Annotations attached to one output column."""
    flags: Set[str] = field(default_factory=set)
    fk_target: Optional[str] = None        # @fk(table.column) -> "table.column"
    decimal: Optional[Tuple[int, int]] = None  # @decimal(18,2) -> (18, 2)


@dataclass
class FileAnnotations:
    """All annotations parsed from one SQL file."""
    entity: Dict[str, str] = field(default_factory=dict)
    columns: Dict[str, ColumnAnnotations] = field(default_factory=dict)
    header_block: str = ""  # Original entity-level comment block (preserved)


def extract_annotations(sql: str) -> FileAnnotations:
    """Pull MDDE annotations from SQL comments."""
    result = FileAnnotations()

    # Entity-level annotations from header comments.
    for m in _ENTITY_ANNOTATION_RE.finditer(sql):
        result.entity[m.group("key")] = m.group("value").strip()

    # Header block — every leading line that's a comment, blank, or
    # whitespace, until the first non-comment statement.
    header_lines: List[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            header_lines.append(line)
        else:
            break
    result.header_block = "\n".join(header_lines).rstrip() + "\n"

    # Column-level annotations — match "<expr>,? -- @flag1 @flag2 ..."
    # We do this lexically rather than via the parser because sqlglot
    # discards inline comments at parse time.
    column_pattern = re.compile(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*(?:\([^)]*\))?(?:\s+AS\s+[A-Za-z_][A-Za-z0-9_]*)?)"
        r"[,]?\s*--\s*(@.+?)\s*$",
        re.MULTILINE | re.IGNORECASE,
    )
    for m in column_pattern.finditer(sql):
        col_expr = m.group(1).strip()
        flags_text = m.group(2)
        # Resolve column name — last identifier in "EXPR AS name" or
        # the bare column itself.
        as_match = re.search(r"AS\s+([A-Za-z_][A-Za-z0-9_]*)", col_expr,
                             re.IGNORECASE)
        if as_match:
            col_name = as_match.group(1)
        else:
            # Bare identifier or function call — strip parens and qualifiers.
            col_name = re.split(r"\s|\(", col_expr)[0].split(".")[-1]

        ann = result.columns.setdefault(col_name, ColumnAnnotations())
        for flag_match in re.finditer(r"@([a-z_]+)(?:\(([^)]*)\))?", flags_text):
            flag = flag_match.group(1)
            arg = flag_match.group(2)
            if flag == "fk" and arg:
                ann.fk_target = arg.strip()
                ann.flags.add("fk")
            elif flag == "decimal" and arg:
                parts = [p.strip() for p in arg.split(",")]
                if len(parts) == 2:
                    try:
                        ann.decimal = (int(parts[0]), int(parts[1]))
                    except ValueError:
                        pass
            elif flag in _COLUMN_ANNOTATION_FLAGS:
                ann.flags.add(flag)

    return result


# =============================================================================
# 2. SQL PARSE + LINEAGE EXTRACTION (sqlglot AST)
# =============================================================================


@dataclass
class ColumnLineage:
    """Lineage record for one output column.

    ``source_columns`` lists ``(table, column)`` pairs the output is
    derived from. For a constant or pure expression, the list is empty.
    """
    output_column: str
    source_columns: List[Tuple[str, str]]
    derivation: str  # "direct" | "rename" | "expression" | "aggregate" | "constant"
    expression: str  # The SQL fragment that produces the column


@dataclass
class ParsedFile:
    """Everything extracted from one SQL file."""
    path: Path
    raw_sql: str
    entity_name: str
    annotations: FileAnnotations
    parsed: Optional[exp.Expression] = None
    lineage: List[ColumnLineage] = field(default_factory=list)
    cte_names: List[str] = field(default_factory=list)
    source_tables: List[str] = field(default_factory=list)
    parse_error: Optional[str] = None
    qualified: bool = False                  # True iff qualify() succeeded
    qualify_error: Optional[str] = None      # Reason qualify() was skipped


def _pre_parse_union_separator(sql: str, separator: Optional[str]) -> str:
    """Replace the customer's UNION-ALL-by-comma convention with
    explicit UNION ALL before parsing.

    The customer site writes UNION ALL between multiple SELECT
    statements as a comma between SELECTs. Two shapes occur in
    practice:

        Shape A (comma alone on a line):
            SELECT a, b, c FROM t1
            ,
            SELECT a, b, c FROM t2

        Shape B (comma at end of previous line):
            SELECT a, b, c FROM t1,
            SELECT a, b, c FROM t2

    sqlglot can't parse either. This pre-pass rewrites both into
    explicit ``UNION ALL`` keywords.

    The match is anchored on a comma whose NEXT non-whitespace
    token (across newlines) is ``SELECT``. A comma followed by a
    column name / expression (the normal in-SELECT case) is left
    alone. Commas inside parentheses (subquery argument lists,
    function calls) are also left alone — when followed by SELECT
    they're a subquery, not a UNION.

    ``separator`` is the configurable token; only ``","`` is
    supported today. Returns the input unchanged when separator is
    None/empty.
    """
    if not separator or separator != ",":
        return sql

    # Strategy: scan character-by-character tracking paren depth.
    # When we see a comma at paren depth 0, look ahead for SELECT
    # (skipping whitespace + comments). If found, replace the comma.
    out: List[str] = []
    depth = 0
    in_string: Optional[str] = None  # current quote char if inside a string
    in_line_comment = False
    in_block_comment = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_line_comment:
            out.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            out.append(ch)
            if ch == "*" and i + 1 < n and sql[i + 1] == "/":
                out.append("/")
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue
        if in_string is not None:
            out.append(ch)
            if ch == in_string and (i == 0 or sql[i - 1] != "\\"):
                in_string = None
            i += 1
            continue
        # Detect entry into a string / comment.
        if ch in ('"', "'"):
            in_string = ch
            out.append(ch)
            i += 1
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            in_line_comment = True
            out.append(ch)
            i += 1
            continue
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            in_block_comment = True
            out.append(ch)
            out.append("*")
            i += 2
            continue
        if ch == "(":
            depth += 1
            out.append(ch)
            i += 1
            continue
        if ch == ")":
            if depth > 0:
                depth -= 1
            out.append(ch)
            i += 1
            continue
        if ch == "," and depth == 0:
            # Look ahead: is the next non-whitespace, non-comment
            # token the keyword SELECT?
            j = i + 1
            while j < n:
                # Skip whitespace.
                if sql[j].isspace():
                    j += 1
                    continue
                # Skip line / block comments in the gap.
                if j + 1 < n and sql[j] == "-" and sql[j + 1] == "-":
                    while j < n and sql[j] != "\n":
                        j += 1
                    continue
                if j + 1 < n and sql[j] == "/" and sql[j + 1] == "*":
                    j += 2
                    while j + 1 < n and not (sql[j] == "*" and sql[j + 1] == "/"):
                        j += 1
                    j += 2
                    continue
                break
            if j + 5 < n and sql[j:j + 6].upper() == "SELECT" and (
                j + 6 == n or not sql[j + 6].isalnum() and sql[j + 6] != "_"
            ):
                # Rewrite this comma as UNION ALL. Preserve any
                # whitespace/comments between the comma and SELECT
                # by writing them after the keyword.
                out.append("\nUNION ALL")
                # Keep the original gap (whitespace + comments) so
                # the next iteration writes it.
                i += 1
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def parse_file(
    path: Path,
    metadata: Optional[MetadataSchema] = None,
    union_separator: Optional[str] = None,
) -> ParsedFile:
    """Read a SQL file, extract annotations, parse with sqlglot.

    If ``metadata`` is provided and non-empty, run sqlglot's
    ``qualify()`` pass on the AST. That resolves bare column refs to
    their owning table, expands ``SELECT *`` into explicit columns,
    and validates that referenced columns exist. On failure (e.g.
    ambiguous columns, columns not in the schema), falls back to
    the unqualified AST and records the reason in ``qualify_error``.

    ``union_separator`` (e.g., ``","``) triggers a pre-parse step
    that replaces the customer's UNION-ALL-by-comma convention with
    explicit ``UNION ALL`` keywords. See
    ``_pre_parse_union_separator``.
    """
    raw_sql = path.read_text(encoding="utf-8")
    if union_separator:
        raw_sql = _pre_parse_union_separator(raw_sql, union_separator)
    annotations = extract_annotations(raw_sql)
    entity_name = annotations.entity.get("entity") or path.stem

    pf = ParsedFile(
        path=path,
        raw_sql=raw_sql,
        entity_name=entity_name,
        annotations=annotations,
    )

    try:
        statements = sqlglot.parse(raw_sql, read=None)
        # The CREATE VIEW / SELECT with a SELECT inside is what we
        # actually want to walk. Find the most-relevant top-level
        # statement.
        target = None
        for stmt in statements:
            if stmt is None:
                continue
            if isinstance(stmt, exp.Create) and stmt.this:
                # CREATE [OR REPLACE] VIEW foo AS <select>
                target = stmt.expression or stmt.this
                break
            if isinstance(stmt, exp.Query):
                target = stmt
                break
        if target is None and statements:
            target = statements[-1]
        pf.parsed = target

        # Qualify pass — opportunistic, fail-soft per design.
        if pf.parsed is not None and metadata:
            try:
                from sqlglot.optimizer.qualify import qualify
                pf.parsed = qualify(
                    pf.parsed,
                    schema=metadata.to_sqlglot_schema(),
                    # Don't raise on columns we can't resolve — record
                    # them and keep going. Lets a partial schema still
                    # help the columns it does cover.
                    validate_qualify_columns=False,
                    # Allow the script to qualify even when not every
                    # source table has a schema entry.
                    allow_partial_qualification=True,
                    # We still want * expanded when possible.
                    expand_stars=True,
                )
                pf.qualified = True
            except Exception as exc:  # noqa: BLE001 — fail-soft is the design
                pf.qualify_error = type(exc).__name__ + ": " + str(exc)

        if pf.parsed is not None:
            pf.cte_names = _extract_cte_names(pf.parsed)
            pf.source_tables = _extract_source_tables(pf.parsed)
            pf.lineage = _extract_column_lineage(pf.parsed)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as exc:
        # Tokenization failures (unterminated string literals, bad
        # escape sequences, ...) are just as fatal as parse errors
        # for our purposes — record the message and skip downstream
        # transforms. Without this catch ONE malformed file in the
        # corpus aborts the entire batch.
        pf.parse_error = str(exc)

    return pf


def _extract_cte_names(node: exp.Expression) -> List[str]:
    return [cte.alias_or_name for cte in node.find_all(exp.CTE)]


def _extract_source_tables(node: exp.Expression) -> List[str]:
    """Tables referenced via FROM/JOIN that are not local CTEs."""
    cte_names = set(_extract_cte_names(node))
    seen: List[str] = []
    for table in node.find_all(exp.Table):
        name = table.name
        if not name or name in cte_names:
            continue
        # Build qualified name when schema/catalog is present.
        parts = []
        if table.args.get("catalog"):
            parts.append(table.args["catalog"].name)
        if table.args.get("db"):
            parts.append(table.args["db"].name)
        parts.append(name)
        qualified = ".".join(parts)
        if qualified not in seen:
            seen.append(qualified)
    return seen


def _extract_column_lineage(node: exp.Expression) -> List[ColumnLineage]:
    """Walk the outermost SELECT and record per-column lineage.

    Best-effort — sqlglot's full lineage API is heavyweight. For the
    demo we use a structural walk that's correct for the common shapes:
    direct column refs, aliased renames, single-column expressions,
    and aggregate functions.
    """
    select = _outermost_select(node)
    if select is None:
        return []

    out: List[ColumnLineage] = []
    for projection in select.expressions:
        out_col = _projection_alias(projection)
        if out_col is None:
            continue
        sources = _collect_column_refs(projection)
        derivation = _classify_derivation(projection, sources)
        out.append(ColumnLineage(
            output_column=out_col,
            source_columns=sources,
            derivation=derivation,
            expression=projection.sql(),
        ))
    return out


def _outermost_select(node: exp.Expression) -> Optional[exp.Select]:
    """Return the top-level SELECT (after CTEs)."""
    if isinstance(node, exp.Select):
        return node
    if isinstance(node, exp.Query):
        # WITH ... SELECT ... or set operations
        inner = node.find(exp.Select)
        return inner
    select = node.find(exp.Select)
    return select


def _projection_alias(proj: exp.Expression) -> Optional[str]:
    """Output column name for a projection."""
    if isinstance(proj, exp.Alias):
        return proj.alias
    if isinstance(proj, exp.Column):
        return proj.name
    if isinstance(proj, exp.Star):
        return "*"
    return None


def _collect_column_refs(node: exp.Expression) -> List[Tuple[str, str]]:
    """All ``(table_alias_or_name, column)`` pairs the projection
    actually reads. For an expression like ``SUM(o.total)`` this
    returns ``[("o", "total")]``.
    """
    seen: List[Tuple[str, str]] = []
    for col in node.find_all(exp.Column):
        table = col.table or ""
        name = col.name
        pair = (table, name)
        if pair not in seen:
            seen.append(pair)
    return seen


def _classify_derivation(
    proj: exp.Expression,
    sources: List[Tuple[str, str]],
) -> str:
    if not sources:
        return "constant"
    has_agg = any(proj.find_all(exp.AggFunc))
    if has_agg:
        return "aggregate"
    if isinstance(proj, exp.Column):
        return "direct"
    if isinstance(proj, exp.Alias):
        inner = proj.this
        if isinstance(inner, exp.Column):
            return "rename"
    return "expression"


# =============================================================================
# 3. QUALITY CHECKS + AUTO-FIX REWRITES
# =============================================================================


@dataclass
class QualityFinding:
    rule: str
    severity: str   # "error" | "warning" | "info"
    location: str   # CTE name or "<final>"
    message: str
    auto_fixed: bool = False


# Vendored optimizer + determinism (sibling modules, no DuckDB).
import _optimizer as lite_optimizer  # noqa: E402


def run_quality_checks(
    pf: ParsedFile,
    customer_config: Optional["CustomerRuleConfig"] = None,
) -> List[QualityFinding]:
    """Run mdde_lite's optimizer rules over the file's SQL string."""
    findings: List[QualityFinding] = []
    if pf.parse_error:
        findings.append(QualityFinding(
            rule="PARSE_ERROR",
            severity="error",
            location="<file>",
            message=pf.parse_error,
        ))
        return findings

    config_dict = customer_config.to_dict() if customer_config else None
    diagnostics = lite_optimizer.analyze_sql(
        pf.raw_sql,
        include_determinism=True,
        config=config_dict,
    )
    for diag in diagnostics:
        findings.append(QualityFinding(
            rule=diag.diagnostic_type,
            severity=diag.severity,
            location=(
                f"line {diag.line_number}" if diag.line_number
                else "<file>"
            ),
            message=diag.message,
        ))

    # File-level check: source_version is the 3rd hyphen-separated
    # part of the filename stem. Emit an info finding when the
    # filename has fewer than 3 parts.
    if len(pf.path.stem.split("-")) < 3:
        findings.append(QualityFinding(
            rule="MISSING_SOURCE_VERSION",
            severity="info",
            location="<file>",
            message=(
                f"Filename '{pf.path.name}' has fewer than 3 hyphen-separated "
                f"parts; movement.csv source_version will be empty"
            ),
        ))

    return findings


# Auto-fixes are conservative: only rewrites that preserve semantics
# under all known engine behaviours. ROW_NUMBER without ORDER BY is
# NOT auto-fixed (we can't infer the right key).
_AUTOFIX_WHERE_TRUE = re.compile(
    r"WHERE\s+1\s*=\s*1\s+AND\s+", re.IGNORECASE,
)
_AUTOFIX_LEADING_WHERE_TRUE = re.compile(
    r"WHERE\s+1\s*=\s*1\s*\n", re.IGNORECASE,
)


# -----------------------------------------------------------------------------
# Subquery → CTE lifter
# -----------------------------------------------------------------------------
#
# In-scope shapes (safe to lift, semantics-preserving):
#   - Derived tables in FROM/JOIN: `FROM (SELECT ...) AS x`
#   - Scalar subqueries in SELECT projections: `SELECT (SELECT ...) AS y`
#   - FROM-wrapped set operations: `FROM ((SELECT ...) UNION ALL (SELECT ...))`
#   - WHERE IN (SELECT ...) predicates — including correlated.
#   - WHERE EXISTS (SELECT ...) predicates — including correlated.
#   - Top-level EXCEPT / EXCEPT ALL / INTERSECT — both branches lifted.
#
# Correlated handling (2026-05-12):
#   When the inner SELECT references an outer-scope column, the lifted
#   CTE additionally projects the correlation column(s) so the outer
#   predicate can re-correlate via a join-shaped reference into the
#   CTE. The outer predicate's structure is preserved (IN / EXISTS
#   remain as predicates) — only the inner SELECT's source-table
#   logic is moved into the CTE.
#
# Out-of-scope (still left inline, flagged):
#   - Comparison subqueries (`WHERE x = (SELECT MAX(y) FROM t)`) when
#     non-scalar — these are scalar already handled by the SELECT-
#     projection path when promoted; a WHERE-side scalar with multi-
#     row potential is too risky.


def _is_correlated(subq: exp.Expression, outer_scopes: List[exp.Expression]) -> bool:
    """A subquery is correlated when it references a table not defined
    inside itself. Compute the set of tables/aliases reachable from
    within the subquery, then check every Column for a table qualifier
    pointing outside that set.
    """
    return bool(_correlated_columns(subq))


def _correlated_columns(subq: exp.Expression) -> List[exp.Column]:
    """Return every column in ``subq`` whose qualifier is not defined
    by a table / CTE inside ``subq``. Those are outer-scope references —
    the columns we need to lift OUT of the inner SELECT (or expose as a
    projection in the rewritten CTE so the outer can re-correlate).
    """
    inner_names: Set[str] = set()
    for tbl in subq.find_all(exp.Table):
        if tbl.alias:
            inner_names.add(tbl.alias)
        if tbl.name:
            inner_names.add(tbl.name)
    for cte in subq.find_all(exp.CTE):
        inner_names.add(cte.alias_or_name)
    out: List[exp.Column] = []
    for col in subq.find_all(exp.Column):
        if col.table and col.table not in inner_names:
            out.append(col)
    return out


def _outer_alias_for(col: exp.Column, target: exp.Expression) -> Optional[str]:
    """Look up which outer-scope source alias ``col.table`` belongs to.
    Returns the alias unchanged when it matches a real source in the
    outer SELECT's FROM/JOIN tree; otherwise ``None``. Used to validate
    that a correlation reference is meaningful (not a typo)."""
    if not col.table:
        return None
    if not isinstance(target, exp.Select):
        return col.table  # Be permissive.
    from_clause = target.args.get("from_") or target.args.get("from")
    if from_clause is None:
        return None
    for tbl in from_clause.find_all(exp.Table):
        if (tbl.alias or tbl.name) == col.table:
            return col.table
    for join in target.args.get("joins") or []:
        for tbl in join.find_all(exp.Table):
            if (tbl.alias or tbl.name) == col.table:
                return col.table
    return None


def _subquery_in_predicate(subq: exp.Subquery) -> bool:
    """True if this subquery sits inside a WHERE/HAVING/ON predicate
    (IN, EXISTS, comparison) rather than FROM/JOIN/SELECT projection."""
    node = subq.parent
    while node is not None:
        if isinstance(node, (exp.Where, exp.Having)):
            return True
        if isinstance(node, exp.Join) and subq is node.args.get("on"):
            return True
        # If we hit a Select/From/Join slot that *contains* the subquery
        # as a table-like position, it's not a predicate.
        if isinstance(node, (exp.From, exp.Join)):
            return False
        node = node.parent
    return False


def _make_cte_name(
    subq: exp.Subquery,
    fallback_index: int,
    used: Set[str],
) -> str:
    """Pick a stable, unique CTE name.

    Prefers the subquery's existing alias (semantic). Falls back to
    ``_sub1``, ``_sub2`` when no alias is present. Always guarantees
    uniqueness against ``used``.
    """
    base = subq.alias_or_name or f"_sub{fallback_index}"
    name = base
    counter = 2
    while name in used:
        name = f"{base}_{counter}"
        counter += 1
    used.add(name)
    return name


# -----------------------------------------------------------------------------
# FULL OUTER JOIN + COALESCE → three-CTE UNION ALL pattern
# -----------------------------------------------------------------------------
#
# A common shape we see at customer sites is:
#
#     SELECT
#         COALESCE(a.key, b.key)  AS key,
#         a.value AS value_a,
#         b.value AS value_b,
#         ...
#     FROM left_t a
#     FULL OUTER JOIN right_t b ON a.key = b.key
#
# That's hard to reason about: COALESCE hides which side a row came
# from, and downstream consumers can't tell whether a NULL means
# "missing in left" or "missing in right". Rewrite into three CTEs +
# UNION ALL so each side is named and tagged:
#
#     WITH unique_rows_from_<a> AS (
#       SELECT ... 'a' AS source_ind
#       FROM left_t a LEFT JOIN right_t b ON ...
#       WHERE b.<key> IS NULL
#     ),
#     unique_rows_from_<b> AS (
#       SELECT ... 'b' AS source_ind
#       FROM right_t b LEFT JOIN left_t a ON ...
#       WHERE a.<key> IS NULL
#     ),
#     matching_rows AS (
#       SELECT ... 'both' AS source_ind
#       FROM left_t a INNER JOIN right_t b ON ...
#     )
#     SELECT * FROM unique_rows_from_<a>
#     UNION ALL SELECT * FROM unique_rows_from_<b>
#     UNION ALL SELECT * FROM matching_rows
#
# Scope:
#   - Fires only when at least one outer projection is
#     ``COALESCE(a.x, b.x)`` matching the join keys (so the COALESCE
#     was being used to merge the FULL OUTER's columns).
#   - Single FULL OUTER JOIN at the top-level only. Multi-FULL-OUTER
#     chains and FULL OUTER inside a CTE are left alone.
#   - The projection list is preserved column-for-column in each
#     branch. ``COALESCE(a.x, b.x)`` becomes ``a.x`` in the unique-a
#     branch, ``b.x`` in the unique-b branch, and ``a.x`` in
#     matching (where both sides are non-null and equal).
#   - Columns from only one side (e.g., ``a.value AS value_a``) are
#     projected as NULL in the OTHER side's branch — so the
#     UNION ALL stays column-aligned.


def _is_coalesce_of_join_columns(
    proj: exp.Expression,
    alias_a: str,
    alias_b: str,
) -> bool:
    """True if ``proj`` is `COALESCE(a.X, b.X)` (or its Alias) where
    both arguments are columns from the two aliases — i.e., merging
    the same logical column from both sides of a FULL OUTER JOIN."""
    inner = proj.this if isinstance(proj, exp.Alias) else proj
    if not isinstance(inner, exp.Coalesce):
        return False
    args = [inner.this] + (inner.expressions or [])
    if len(args) != 2:
        return False
    cols = [a for a in args if isinstance(a, exp.Column)]
    if len(cols) != 2:
        return False
    tables = {c.table for c in cols if c.table}
    return tables == {alias_a, alias_b}


def rewrite_full_outer_with_coalesce(
    sql: str,
    findings: List[QualityFinding],
    log: "TransformLog",
) -> Tuple[str, List[QualityFinding]]:
    """Rewrite ``FULL OUTER JOIN`` + ``COALESCE`` into a three-CTE
    UNION ALL pattern with explicit `source_ind` tags. Fail-soft.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    root = statements[0]
    if isinstance(root, exp.Create) and root.this:
        target = root.expression or root.this
    elif isinstance(root, exp.Select):
        target = root
    else:
        target = root.find(exp.Select) if hasattr(root, "find") else None
    if not isinstance(target, exp.Select):
        return sql, findings

    # Look for a top-level FULL OUTER JOIN. We support exactly ONE
    # such join — chains of FULL OUTERs are out of scope.
    joins = target.args.get("joins") or []
    full_outer_joins = [
        j for j in joins
        if (j.args.get("side") or "").upper() == "FULL"
        and (j.args.get("kind") or "").upper() == "OUTER"
    ]
    if len(full_outer_joins) != 1:
        return sql, findings
    if len(joins) != 1:  # Other joins in the mix — bail.
        return sql, findings
    fo_join = full_outer_joins[0]

    on_clause = fo_join.args.get("on")
    if on_clause is None:
        return sql, findings

    # Identify the two side aliases (left = FROM, right = JOIN).
    from_clause = target.args.get("from_") or target.args.get("from")
    if from_clause is None:
        return sql, findings
    left_tbl = from_clause.this if isinstance(from_clause.this, exp.Table) else None
    right_tbl = fo_join.this if isinstance(fo_join.this, exp.Table) else None
    if not isinstance(left_tbl, exp.Table) or not isinstance(right_tbl, exp.Table):
        return sql, findings
    alias_a = left_tbl.alias or left_tbl.name
    alias_b = right_tbl.alias or right_tbl.name
    if not alias_a or not alias_b:
        return sql, findings

    # Must have at least one COALESCE(a.x, b.x) projection.
    outer_projections = list(target.expressions)
    if not any(
        _is_coalesce_of_join_columns(p, alias_a, alias_b)
        for p in outer_projections
    ):
        return sql, findings

    # CTE names. Use source table NAME (without alias) so the labels
    # are stable across rewrites.
    a_name = left_tbl.name or alias_a
    b_name = right_tbl.name or alias_b
    cte_a_name = f"unique_rows_from_{a_name}"
    cte_b_name = f"unique_rows_from_{b_name}"
    cte_match_name = "matching_rows"

    def _build_branch_projections(
        side: str,  # 'a' | 'b' | 'both'
    ) -> List[exp.Expression]:
        """Re-project the outer SELECT's projection list for one of
        the three branches. COALESCE(a.x, b.x) collapses to the
        non-null side. Columns referencing only the OTHER side
        become NULL in this branch (so the UNION stays column-aligned).
        """
        new_projs: List[exp.Expression] = []
        for orig in outer_projections:
            inner = orig.this if isinstance(orig, exp.Alias) else orig
            out_name = orig.alias if isinstance(orig, exp.Alias) else None

            # COALESCE(a.x, b.x) → which side wins this branch?
            if _is_coalesce_of_join_columns(inner, alias_a, alias_b):
                args = [inner.this] + (inner.expressions or [])
                col_a = next(c for c in args if isinstance(c, exp.Column) and c.table == alias_a)
                col_b = next(c for c in args if isinstance(c, exp.Column) and c.table == alias_b)
                if side == "a":
                    chosen = col_a.copy()
                elif side == "b":
                    chosen = col_b.copy()
                else:  # both
                    chosen = col_a.copy()
                if out_name:
                    new_projs.append(exp.alias_(chosen, out_name))
                else:
                    new_projs.append(chosen)
                continue

            # Plain column ref — check which side it's from.
            referenced_aliases = {
                c.table for c in orig.find_all(exp.Column) if c.table
            }
            if side == "a":
                # Columns from b become NULL.
                if alias_b in referenced_aliases and alias_a not in referenced_aliases:
                    null_ref = exp.Null()
                    if out_name:
                        new_projs.append(exp.alias_(null_ref, out_name))
                    else:
                        new_projs.append(null_ref)
                    continue
            elif side == "b":
                if alias_a in referenced_aliases and alias_b not in referenced_aliases:
                    null_ref = exp.Null()
                    if out_name:
                        new_projs.append(exp.alias_(null_ref, out_name))
                    else:
                        new_projs.append(null_ref)
                    continue
            new_projs.append(orig.copy())

        # Append source_ind tag.
        tag_value = "both" if side == "both" else side
        new_projs.append(exp.alias_(exp.Literal.string(tag_value), "source_ind"))
        return new_projs

    def _build_branch_select(
        side: str,
        primary_tbl: exp.Table,
        other_tbl: exp.Table,
        is_inner: bool,
    ) -> exp.Select:
        sel = exp.Select(expressions=_build_branch_projections(side))
        sel.set("from_", exp.From(this=primary_tbl.copy()))
        join_kind = "INNER" if is_inner else "LEFT"
        join_node = exp.Join(
            this=other_tbl.copy(),
            kind=join_kind,
            on=on_clause.copy(),
        )
        sel.set("joins", [join_node])

        # For LEFT joins on the unique-rows side, add WHERE
        # <other-side>.<key> IS NULL using the FIRST equality leaf
        # of the ON clause (or any column from the OTHER side).
        if not is_inner:
            other_alias = other_tbl.alias or other_tbl.name
            # Pick the join's other-side column from the ON clause.
            other_col = None
            for c in on_clause.find_all(exp.Column):
                if c.table == other_alias:
                    other_col = c
                    break
            if other_col is not None:
                sel.set(
                    "where",
                    exp.Where(this=exp.Is(this=other_col.copy(), expression=exp.Null())),
                )
        return sel

    cte_a = exp.CTE(
        this=_build_branch_select("a", left_tbl, right_tbl, is_inner=False),
        alias=exp.TableAlias(this=exp.to_identifier(cte_a_name)),
    )
    cte_b = exp.CTE(
        this=_build_branch_select("b", right_tbl, left_tbl, is_inner=False),
        alias=exp.TableAlias(this=exp.to_identifier(cte_b_name)),
    )
    cte_match = exp.CTE(
        this=_build_branch_select("both", left_tbl, right_tbl, is_inner=True),
        alias=exp.TableAlias(this=exp.to_identifier(cte_match_name)),
    )

    # Build the new outer body: a 3-branch UNION ALL of SELECT *
    # FROM each new CTE. Left-associative chain.
    def _sel_star(cte_name: str) -> exp.Select:
        return exp.Select(expressions=[exp.Star()]).from_(
            exp.Table(this=exp.to_identifier(cte_name))
        )

    new_body: exp.Expression = exp.Union(
        this=_sel_star(cte_a_name),
        expression=_sel_star(cte_b_name),
        distinct=False,
    )
    new_body = exp.Union(
        this=new_body,
        expression=_sel_star(cte_match_name),
        distinct=False,
    )

    # Attach the WITH clause. Preserve any existing WITH.
    existing_with = target.args.get("with_")
    new_ctes_list = [cte_a, cte_b, cte_match]
    if existing_with:
        new_body.set(
            "with_",
            exp.With(
                expressions=list(existing_with.expressions) + new_ctes_list,
                recursive=False,
            ),
        )
    else:
        new_body.set(
            "with_",
            exp.With(expressions=new_ctes_list, recursive=False),
        )

    # Swap into the parent statement.
    if isinstance(root, exp.Create):
        root.set("expression", new_body)
        statements[0] = root
    else:
        statements[0] = new_body

    log.full_outer_rewritten = True

    try:
        rendered = "\n".join(
            s.sql(pretty=True) for s in statements if s is not None
        )
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


# -----------------------------------------------------------------------------
# DISTINCT → ROW_NUMBER() dedup pattern
# -----------------------------------------------------------------------------
#
# ``SELECT DISTINCT`` hides data-quality problems: it silently drops
# duplicate rows without making the duplication visible. Replace with
# an explicit two-CTE dedup so analysts can see how many rows were
# duplicated, why, and pick a representative row deterministically:
#
#   ``<entity>_ranked``   — original SELECT + ROW_NUMBER() OVER
#                            (PARTITION BY <all projection columns>
#                             ORDER BY (SELECT NULL)) AS rn
#   ``<entity>_deduped``  — SELECT <columns> FROM <ranked> WHERE rn = 1
#
# The top-level SELECT becomes ``SELECT * FROM <entity>_deduped`` plus
# any ORDER BY / LIMIT from the original.
#
# Scope:
#   - Plain ``SELECT DISTINCT col1, col2, ...``. The full projection
#     list (after expanding ``*`` if metadata is available) becomes
#     the PARTITION BY list.
#   - ``SELECT DISTINCT *`` is left alone (a finding is emitted) —
#     the partition-by would be unspecified.
#   - ``SELECT DISTINCT ON (...)`` (Postgres) is left alone — has
#     different semantics; the ROW_NUMBER pattern would need an
#     explicit ORDER BY which isn't safe to infer.
#   - DISTINCT inside aggregates (``COUNT(DISTINCT x)``) is unaffected
#     — that's a function-arg distinct, not a SELECT-level one.


def replace_distinct_with_rownum(
    sql: str,
    findings: List[QualityFinding],
    entity_hint: str,
    log: "TransformLog",
) -> Tuple[str, List[QualityFinding]]:
    """Rewrite ``SELECT DISTINCT ...`` as a two-CTE
    ROW_NUMBER-then-filter pattern.

    Customer rule: a plain DISTINCT is a code-smell hiding duplicate
    rows. Replace with an explicit ranking-then-filtering pattern so
    the deduplication is visible and inspectable.

    Fires when the target SELECT has a non-ON DISTINCT modifier and
    its projection list isn't ``*``. Skipped otherwise with a
    ``DISTINCT_NOT_REWRITTEN`` info finding.

    Safe-by-default: fail-soft on parse errors.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    root = statements[0]
    if isinstance(root, exp.Create) and root.this:
        target = root.expression or root.this
    elif isinstance(root, exp.Select):
        target = root
    else:
        target = root.find(exp.Select) if hasattr(root, "find") else None
    if not isinstance(target, exp.Select):
        return sql, findings

    distinct = target.args.get("distinct")
    if distinct is None:
        return sql, findings

    # Skip DISTINCT ON (...) — different semantics.
    if isinstance(distinct, exp.Distinct) and distinct.args.get("on"):
        findings.append(QualityFinding(
            rule="DISTINCT_NOT_REWRITTEN",
            severity="info",
            location="<distinct-on>",
            message=(
                "SELECT DISTINCT ON (...) left as-is — different "
                "semantics from plain DISTINCT; the ROW_NUMBER "
                "rewrite needs an explicit ORDER BY this transform "
                "won't invent."
            ),
        ))
        return sql, findings

    # Skip SELECT DISTINCT * — no projection list to partition on.
    projections = target.expressions or []
    if any(isinstance(p, exp.Star) for p in projections):
        findings.append(QualityFinding(
            rule="DISTINCT_NOT_REWRITTEN",
            severity="info",
            location="<distinct-star>",
            message=(
                "SELECT DISTINCT * left as-is — the ROW_NUMBER "
                "pattern needs an explicit projection list to "
                "partition on."
            ),
        ))
        return sql, findings
    if not projections:
        return sql, findings

    # Prefer the FROM-table name (or first source) as the CTE
    # prefix, so a `SELECT DISTINCT ... FROM customer` produces
    # ``customer_ranked`` / ``customer_deduped`` rather than the
    # generic file entity hint.
    prefix = _cte_name_prefix(target, entity_hint)
    ranked_name = f"{prefix}_ranked"
    deduped_name = f"{prefix}_deduped"

    # Pick a unique rn column name in case the projection already
    # has a column called `rn`.
    rn_col = "rn"
    existing_aliases: Set[str] = set()
    for p in projections:
        if isinstance(p, exp.Alias):
            existing_aliases.add(p.alias)
        elif isinstance(p, exp.Column):
            existing_aliases.add(p.name)
    counter = 2
    while rn_col in existing_aliases:
        rn_col = f"rn_{counter}"
        counter += 1

    # Build the PARTITION BY list from the projection list. For each
    # projection, the partition-by argument is its OUTPUT column
    # (after rename) — which is just a column reference inside the
    # ranking SELECT itself. So `PARTITION BY a, b, AS_alias` →
    # `PARTITION BY a, b, AS_alias`. Strip qualifiers so the
    # references resolve in the ranking CTE's own scope.
    partition_cols: List[exp.Expression] = []
    output_col_names: List[str] = []
    for p in projections:
        if isinstance(p, exp.Alias):
            out_name = p.alias
        elif isinstance(p, exp.Column):
            out_name = p.name
        else:
            # Bare expression in DISTINCT projection (e.g.,
            # SELECT DISTINCT UPPER(email)) — partition on it
            # by its position alias. We'll alias it for
            # determinism.
            out_name = f"col_{len(output_col_names) + 1}"
        output_col_names.append(out_name)
        partition_cols.append(exp.column(out_name))

    # The ranked CTE's body: the ORIGINAL select, with:
    #  - DISTINCT removed
    #  - ROW_NUMBER appended as an extra projection
    #  - All projections aliased to their output names so PARTITION
    #    BY can reference them
    ranked_body = target.copy()
    ranked_body.set("distinct", None)
    ranked_body.set("order", None)  # ORDER BY moves to outer
    ranked_body.set("limit", None)  # LIMIT moves to outer
    ranked_body.set("offset", None)
    # Re-alias each projection so the output-column names are stable.
    new_projs: List[exp.Expression] = []
    for orig, out_name in zip(projections, output_col_names):
        if isinstance(orig, exp.Alias):
            new_projs.append(orig.copy())
        elif isinstance(orig, exp.Column) and orig.name == out_name:
            new_projs.append(orig.copy())
        else:
            new_projs.append(exp.alias_(orig.copy(), out_name))
    # Append the ROW_NUMBER projection.
    rn_expr = exp.alias_(
        exp.Window(
            this=exp.RowNumber(),
            partition_by=[c.copy() for c in partition_cols],
            order=exp.Order(
                expressions=[
                    exp.Ordered(this=exp.Subquery(
                        this=exp.Select(
                            expressions=[exp.Null()],
                        )
                    ))
                ],
            ),
        ),
        rn_col,
    )
    new_projs.append(rn_expr)
    ranked_body.set("expressions", new_projs)

    # Existing WITH clause on the original — we'll re-attach it on the
    # NEW outer SELECT instead, so the ranked CTE sees it via scoping.
    original_with = target.args.get("with_")
    ranked_body.set("with_", None)

    # Build the deduped CTE.
    deduped_body = exp.Select(
        expressions=[exp.column(name) for name in output_col_names],
    ).from_(exp.Table(this=exp.to_identifier(ranked_name)))
    deduped_body.set(
        "where",
        exp.Where(
            this=exp.EQ(
                this=exp.column(rn_col),
                expression=exp.Literal.number(1),
            )
        ),
    )

    # Build the new outer SELECT (which carries ORDER BY / LIMIT from
    # the original — those are presentation concerns that belong with
    # the final result).
    new_outer = exp.Select(expressions=[exp.Star()]).from_(
        exp.Table(this=exp.to_identifier(deduped_name))
    )
    original_order = target.args.get("order")
    if original_order is not None:
        new_outer.set("order", original_order.copy())
    original_limit = target.args.get("limit")
    if original_limit is not None:
        new_outer.set("limit", original_limit.copy())
    original_offset = target.args.get("offset")
    if original_offset is not None:
        new_outer.set("offset", original_offset.copy())

    # Strip qualifiers in the new outer's ORDER BY since columns
    # come from the (unaliased) deduped CTE.
    order = new_outer.args.get("order")
    if order is not None:
        for col in order.find_all(exp.Column):
            col.set("table", None)

    # Compose the WITH chain.
    new_ctes: List[exp.CTE] = []
    if original_with is not None:
        new_ctes.extend(original_with.expressions)
    new_ctes.append(exp.CTE(
        this=ranked_body,
        alias=exp.TableAlias(this=exp.to_identifier(ranked_name)),
    ))
    new_ctes.append(exp.CTE(
        this=deduped_body,
        alias=exp.TableAlias(this=exp.to_identifier(deduped_name)),
    ))
    new_outer.set("with_", exp.With(expressions=new_ctes, recursive=False))

    # Swap the new outer into the parent statement.
    if isinstance(root, exp.Create):
        root.set("expression", new_outer)
        statements[0] = root
    else:
        statements[0] = new_outer

    log.distinct_rewritten = True

    try:
        rendered = "\n".join(
            s.sql(pretty=True) for s in statements if s is not None
        )
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


# -----------------------------------------------------------------------------
# ROW_NUMBER / RANK without ORDER BY — auto-fix via @mdde-order-by annotation
# -----------------------------------------------------------------------------
#
# ``ROW_NUMBER() OVER (PARTITION BY x)`` without an ORDER BY produces
# engine-dependent output. The quality check flags it as
# WINDOW_NO_ORDER. We can auto-fix when the file declares a
# ``-- @mdde-order-by: <col>[, <col>...]`` annotation; otherwise we
# leave the warning in place so a human decides which key gives
# deterministic results.
#
# Scope:
#   - Annotation must be present at the file level.
#   - Applies to every Window function with no existing ORDER BY in
#     its OVER clause (whether it's ROW_NUMBER, RANK, DENSE_RANK,
#     LAG, LEAD, FIRST_VALUE, LAST_VALUE, etc.).
#   - Windows that already have an ORDER BY are untouched.


def apply_window_order_by(
    sql: str,
    findings: List[QualityFinding],
    order_by_annotation: Optional[str],
    log: "TransformLog",
) -> Tuple[str, List[QualityFinding]]:
    """Add ORDER BY <annotation> to every Window function lacking one.

    ``order_by_annotation`` is the raw string from the
    ``@mdde-order-by`` annotation — comma-separated column names,
    optional ``ASC``/``DESC`` per column. When None or empty, the
    transform is a no-op.

    Fail-soft on parse errors.
    """
    if not order_by_annotation:
        return sql, findings
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    # Build the ORDER BY AST once from the annotation string.
    try:
        order_by_node = sqlglot.parse_one(
            f"SELECT 1 ORDER BY {order_by_annotation}",
            read=None,
        ).args.get("order")
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if order_by_node is None:
        return sql, findings

    rewrote = False
    for stmt in statements:
        if stmt is None:
            continue
        for win in stmt.find_all(exp.Window):
            if win.args.get("order"):
                continue  # Already has ORDER BY.
            win.set("order", order_by_node.copy())
            rewrote = True

    if not rewrote:
        return sql, findings

    # Mark any WINDOW_NO_ORDER findings as auto-fixed.
    for f in findings:
        if f.rule == "WINDOW_NO_ORDER":
            f.auto_fixed = True

    log.window_order_by_applied = True

    try:
        rendered = "\n".join(
            s.sql(pretty=True) for s in statements if s is not None
        )
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


def lift_subqueries_to_ctes(
    sql: str,
    findings: List[QualityFinding],
) -> Tuple[str, List[QualityFinding]]:
    """Rewrite inline subqueries as named CTEs.

    Operates AST-level via sqlglot. Mutates findings in place: adds
    one ``SUBQUERY_NOT_LIFTED`` finding per subquery we deliberately
    left inline (correlated, or inside a predicate). Returns the new
    SQL plus the (mutated) findings list.

    Safe-by-default: if parsing fails or no top-level Select is found,
    returns the input unchanged.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings

    if not statements or statements[0] is None:
        return sql, findings

    # Resolve the rewritable target — same logic as parse_file()'s
    # statement picker.
    root = statements[0]
    target: Optional[exp.Expression] = None
    if isinstance(root, exp.Create) and root.this:
        target = root.expression or root.this
    elif isinstance(root, exp.Query):
        target = root
    else:
        target = root.find(exp.Select)

    if target is None or not isinstance(
        target, (exp.Select, exp.Union, exp.Except, exp.Intersect)
    ):
        return sql, findings

    # Existing CTE names — start the used-name set with these so we
    # never collide. sqlglot stores the WITH clause under "with_"
    # (trailing underscore to avoid the Python keyword).
    used_names: Set[str] = set()
    existing_with = target.args.get("with_")
    if existing_with:
        for cte in existing_with.expressions:
            used_names.add(cte.alias_or_name)

    new_ctes: List[exp.CTE] = []
    fallback_idx = 1
    rewrote_any = False

    # Collect candidates. Three categories:
    #   1. Derived tables in FROM/JOIN, scalar subqueries in SELECT —
    #      lift the inner SELECT into a CTE; replace the subquery
    #      with a reference to that CTE.
    #   2. IN / EXISTS predicate subqueries — lift the inner SELECT's
    #      data-access logic into a CTE; rewrite the predicate so it
    #      references the CTE. For correlated cases the CTE
    #      additionally projects the correlation column(s).
    #   3. Other predicate-side subqueries (comparison subqueries with
    #      non-scalar potential) — flagged, left inline.
    candidates: List[exp.Subquery] = []
    predicate_subqueries: List[exp.Expression] = []  # In / Exists nodes
    non_candidates: List[exp.Subquery] = []
    for sub in target.find_all(exp.Subquery):
        parent = sub.parent
        if parent is None:
            continue
        # Derived table in FROM/JOIN
        if isinstance(parent, (exp.From, exp.Join)):
            candidates.append(sub)
            continue
        # Scalar subquery in a SELECT projection (direct child of Select
        # or wrapped in an Alias inside Select).
        if isinstance(parent, exp.Select) and sub in parent.expressions:
            candidates.append(sub)
            continue
        if isinstance(parent, exp.Alias) and isinstance(parent.parent, exp.Select):
            candidates.append(sub)
            continue
        # WHERE IN (SELECT ...) — sub.parent is an In node.
        if isinstance(parent, exp.In) and parent.args.get("query") is sub:
            predicate_subqueries.append(parent)
            continue
        # Comparison subqueries — non-scalar potential; leave inline.
        non_candidates.append(sub)
    # Also collect WHERE EXISTS (SELECT ...) — these wrap a Select
    # directly (no Subquery node), so they don't show up in the
    # find_all(exp.Subquery) loop.
    for exists_node in target.find_all(exp.Exists):
        predicate_subqueries.append(exists_node)

    # Sort predicate subqueries by depth (deepest first) so that
    # NESTED ones get lifted before their containers. Without this,
    # an outer ``IN (SELECT ... WHERE col IN (SELECT ...))`` would
    # have the inner IN lifted to its own CTE, but the outer body
    # already captured the inline inner before the inner-to-CTE
    # swap happened — leaving the outer CTE body with the original
    # inline subquery instead of a reference to the inner CTE.
    def _node_depth(n: exp.Expression) -> int:
        d = 0
        cur = n.parent
        while cur is not None:
            d += 1
            cur = cur.parent
        return d
    predicate_subqueries.sort(key=_node_depth, reverse=True)

    for sub in non_candidates:
        findings.append(QualityFinding(
            rule="SUBQUERY_NOT_LIFTED",
            severity="info",
            location="<predicate>",
            message=(
                f"Subquery inside {type(sub.parent).__name__} left inline — "
                "non-IN/EXISTS predicate subquery; lifting would require "
                "synthesising a join/DISTINCT and may change row counts."
            ),
        ))

    for sub in candidates:
        # Predicate slots are handled separately below — skip here.
        if _subquery_in_predicate(sub):
            continue

        # Skip correlated subqueries in FROM/JOIN/SELECT positions —
        # moving them into a CTE changes semantics.
        if _is_correlated(sub, []):
            findings.append(QualityFinding(
                rule="SUBQUERY_NOT_LIFTED",
                severity="info",
                location="<correlated>",
                message=(
                    "Correlated subquery in FROM/SELECT position left inline — "
                    "lifting would orphan the outer-scope reference."
                ),
            ))
            continue

        inner = sub.this  # The wrapped Select/Union
        if inner is None:
            continue

        parent = sub.parent
        is_scalar_projection = (
            (isinstance(parent, exp.Select) and sub in parent.expressions)
            or (isinstance(parent, exp.Alias) and isinstance(parent.parent, exp.Select))
        )

        # For a scalar projection we need a CTE that exposes the
        # selected value under a known column name so the wrapping
        # `(SELECT col FROM cte)` stays a scalar. If the inner Select
        # doesn't have exactly one named (or namable) projection, skip
        # it — semantic safety trumps cleverness.
        scalar_col_name: Optional[str] = None
        if is_scalar_projection:
            if not isinstance(inner, exp.Select) or len(inner.expressions) != 1:
                findings.append(QualityFinding(
                    rule="SUBQUERY_NOT_LIFTED",
                    severity="info",
                    location="<scalar>",
                    message=(
                        "Scalar subquery with non-trivial projection left "
                        "inline — lifting would require an alias rewrite."
                    ),
                ))
                continue
            proj = inner.expressions[0]
            if isinstance(proj, exp.Alias):
                scalar_col_name = proj.alias
            elif isinstance(proj, exp.Column):
                scalar_col_name = proj.name
            else:
                # Bare aggregate or expression — give it a deterministic
                # alias inside the CTE so the wrapper can reference it.
                scalar_col_name = "value"
                inner = inner.copy()
                inner.set(
                    "expressions",
                    [exp.alias_(proj.copy(), scalar_col_name)],
                )

        # Pick a name, build the CTE, swap the subquery for a Table ref.
        cte_name = _make_cte_name(sub, fallback_idx, used_names)
        if sub.alias_or_name == "":
            fallback_idx += 1

        new_ctes.append(exp.CTE(
            this=inner.copy(),
            alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
        ))

        if isinstance(parent, (exp.From, exp.Join)):
            replacement: exp.Expression = exp.Table(
                this=exp.to_identifier(cte_name),
            )
            # Preserve any outer alias on the subquery so downstream
            # column refs (e.g. `x.foo`) keep resolving.
            outer_alias = sub.alias
            if outer_alias and outer_alias != cte_name:
                replacement = exp.alias_(replacement, outer_alias, table=True)
            sub.replace(replacement)
        else:
            # Scalar subquery in SELECT — rewrite as
            # `(SELECT <col> FROM cte_name)` so the projection slot
            # still receives a single scalar value.
            new_inner = exp.Select(
                expressions=[exp.column(scalar_col_name)],
            ).from_(exp.Table(this=exp.to_identifier(cte_name)))
            sub.set("this", new_inner)

        rewrote_any = True

    # Predicate subqueries (WHERE IN (SELECT ...) and EXISTS (SELECT ...)).
    # We lift the inner SELECT's source-table logic into a CTE; the
    # predicate stays at the outer SELECT but its body becomes a
    # reference to the CTE. For correlated cases the CTE additionally
    # projects the correlation columns so the predicate's body can
    # WHERE on them.
    for pred_node in predicate_subqueries:
        if isinstance(pred_node, exp.In):
            sub = pred_node.args.get("query")
            if not isinstance(sub, exp.Subquery):
                continue
            inner = sub.this
        elif isinstance(pred_node, exp.Exists):
            inner = pred_node.this
            sub = None  # No wrapping Subquery node for Exists.
        else:
            continue
        if not isinstance(inner, exp.Select):
            continue

        # Build the CTE body: a copy of the inner SELECT. We rewrite
        # the inner WHERE to remove correlation leaves and lift each
        # correlation-equality (``l.x = c.x``) to the outer predicate.
        cte_select = inner.copy()

        cte_inner_names: Set[str] = set()
        for tbl in cte_select.find_all(exp.Table):
            if tbl.alias:
                cte_inner_names.add(tbl.alias)
            if tbl.name:
                cte_inner_names.add(tbl.name)

        # Walk the inner WHERE's AND leaves; classify each as
        #   - inner-only        → stays in CTE WHERE
        #   - correlation-only  → moves to outer predicate
        #   - mixed equality    → split: inner side promoted as a CTE
        #                         projection; outer predicate gets an
        #                         equality referencing the CTE.
        outer_correlation_predicates: List[exp.Expression] = []
        # Maps inner column name → expression to put on the outer side
        # of the recovered correlation predicate.
        correlation_pairs: List[Tuple[str, exp.Expression]] = []
        cte_where = cte_select.args.get("where")
        remaining: List[exp.Expression] = []
        if cte_where is not None:
            for leaf in _split_and(cte_where.this):
                inner_refs: List[exp.Column] = []
                outer_refs: List[exp.Column] = []
                for col in leaf.find_all(exp.Column):
                    if col.table and col.table not in cte_inner_names:
                        outer_refs.append(col)
                    else:
                        inner_refs.append(col)
                if not outer_refs:
                    # Pure inner predicate — stays inside the CTE.
                    remaining.append(leaf)
                    continue
                if not inner_refs:
                    # Pure correlation — bubble to outer predicate.
                    outer_correlation_predicates.append(leaf)
                    continue
                # Mixed: try to split a simple equality between one
                # inner column and one outer column. If the shape
                # isn't an EQ of two columns, fall back to leaving
                # the leaf inside the CTE (conservative).
                if (
                    isinstance(leaf, exp.EQ)
                    and isinstance(leaf.this, exp.Column)
                    and isinstance(leaf.expression, exp.Column)
                ):
                    lhs_inner = (
                        not leaf.this.table or leaf.this.table in cte_inner_names
                    )
                    rhs_inner = (
                        not leaf.expression.table
                        or leaf.expression.table in cte_inner_names
                    )
                    if lhs_inner and not rhs_inner:
                        inner_col, outer_col = leaf.this, leaf.expression
                    elif rhs_inner and not lhs_inner:
                        inner_col, outer_col = leaf.expression, leaf.this
                    else:
                        remaining.append(leaf)
                        continue
                    correlation_pairs.append((inner_col.name, outer_col.copy()))
                    continue
                # Anything more complex (function on either side,
                # arithmetic, IN, …) — conservatively keep inside.
                remaining.append(leaf)
            new_inner_pred = _rebuild_and(remaining)
            if new_inner_pred is None:
                cte_select.set("where", None)
            else:
                cte_where.set("this", new_inner_pred)

        # Promote correlation columns + the inner side of mixed
        # equalities as projections in the CTE.
        existing_proj_names: Set[str] = set()
        bare_projections: List[exp.Expression] = []
        for proj in cte_select.expressions or []:
            if isinstance(proj, exp.Alias):
                existing_proj_names.add(proj.alias)
            elif isinstance(proj, exp.Column):
                existing_proj_names.add(proj.name)
        promoted_proj_names: Set[str] = set()
        for inner_name, _ in correlation_pairs:
            if inner_name in existing_proj_names or inner_name in promoted_proj_names:
                continue
            bare_projections.append(exp.column(inner_name))
            promoted_proj_names.add(inner_name)
        # Also promote columns referenced by pure-correlation predicates
        # that point at inner-side columns (rare but possible when the
        # inner-side column is what gets compared to a literal).
        for leaf in outer_correlation_predicates:
            for col in leaf.find_all(exp.Column):
                if col.table and col.table in cte_inner_names:
                    if col.name not in existing_proj_names and col.name not in promoted_proj_names:
                        bare_projections.append(exp.column(col.name))
                        promoted_proj_names.add(col.name)

        # Decide what the CTE's projection list will be.
        # - For IN: keep the original single-column projection, then
        #   add the promoted correlation columns.
        # - For EXISTS: the inner's projection (often `SELECT 1`) is
        #   semantically useless; replace it with the promoted
        #   correlation columns. If there are none (uncorrelated
        #   EXISTS), keep a placeholder ``1`` so the CTE is valid.
        if isinstance(pred_node, exp.In):
            # Compose: existing projections + promoted columns.
            new_projs: List[exp.Expression] = []
            for proj in cte_select.expressions or []:
                new_projs.append(proj)
            for p in bare_projections:
                new_projs.append(p)
            cte_select.set("expressions", new_projs)
        else:  # Exists
            if bare_projections:
                cte_select.set("expressions", list(bare_projections))
            elif not cte_select.expressions:
                cte_select.set("expressions", [exp.Literal.number(1)])
            # else: keep whatever the inner had.

        # Pick a CTE name. For predicate-subquery lifts we always need
        # a fresh fallback name (no useful alias from the parent).
        cte_name = _make_cte_name(
            sub if sub is not None else exp.Subquery(this=cte_select),
            fallback_idx, used_names,
        )
        fallback_idx += 1
        new_ctes.append(exp.CTE(
            this=cte_select,
            alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
        ))

        # Build the outer correlation predicate (re-attached to the
        # IN/EXISTS body so the correlation still holds, but now
        # referencing the lifted CTE's columns instead of the inner
        # table's).
        rebuilt_predicates: List[exp.Expression] = []
        # 1. Mixed equalities lifted from inside the CTE.
        for inner_name, outer_expr in correlation_pairs:
            rebuilt_predicates.append(
                exp.EQ(
                    this=exp.column(inner_name, table=cte_name),
                    expression=outer_expr,
                )
            )
        # 2. Pure correlation leaves bubbled up wholesale. Inner-side
        # columns get rewritten to point at the CTE; outer-side
        # references stay as-is.
        for leaf in outer_correlation_predicates:
            leaf_copy = leaf.copy()
            for col in leaf_copy.find_all(exp.Column):
                if col.table and col.table in cte_inner_names:
                    col.set("table", exp.to_identifier(cte_name))
            rebuilt_predicates.append(leaf_copy)
        outer_pred = _rebuild_and(rebuilt_predicates) if rebuilt_predicates else None

        # Rewrite the predicate body.
        if isinstance(pred_node, exp.In):
            # What single column did the inner SELECT project? sqlglot's
            # IN expects the inner to project exactly one column.
            inner_proj = inner.expressions[0] if inner.expressions else None
            select_col_name: Optional[str] = None
            if isinstance(inner_proj, exp.Alias):
                select_col_name = inner_proj.alias
            elif isinstance(inner_proj, exp.Column):
                select_col_name = inner_proj.name
            if select_col_name is None:
                # Couldn't safely rewrite — flag and skip.
                findings.append(QualityFinding(
                    rule="SUBQUERY_NOT_LIFTED",
                    severity="info",
                    location="<in>",
                    message=(
                        "WHERE IN subquery left inline — inner projection "
                        "shape was not a single named column."
                    ),
                ))
                continue
            new_in_body = exp.Select(
                expressions=[exp.column(select_col_name)],
            ).from_(exp.Table(this=exp.to_identifier(cte_name)))
            if outer_pred is not None:
                new_in_body.set("where", exp.Where(this=outer_pred))
            sub.set("this", new_in_body)
        else:  # Exists
            new_exists_body = exp.Select(
                expressions=[exp.Literal.number(1)],
            ).from_(exp.Table(this=exp.to_identifier(cte_name)))
            if outer_pred is not None:
                new_exists_body.set("where", exp.Where(this=outer_pred))
            pred_node.set("this", new_exists_body)

        rewrote_any = True

    # EXCEPT (or INTERSECT) at the top level — lift each branch into
    # its own CTE so the body becomes ``SELECT * FROM a EXCEPT
    # SELECT * FROM b``. Operates on top-level Except/Intersect only
    # (a nested EXCEPT inside another query is left alone).
    if isinstance(target, (exp.Except, exp.Intersect)):
        # Lift each side independently. Skip a side that's already a
        # bare ``SELECT * FROM <name>`` (already lifted, e.g., by the
        # UNION-branch pass that may have run earlier on a nested
        # Union inside the EXCEPT).
        def _is_already_lifted_setop_side(b: exp.Expression) -> bool:
            if not isinstance(b, exp.Select):
                return False
            projs = b.expressions or []
            if len(projs) != 1 or not isinstance(projs[0], exp.Star):
                return False
            if (
                b.args.get("joins")
                or b.args.get("where")
                or b.args.get("group")
                or b.args.get("having")
                or b.args.get("qualify")
                or b.args.get("with_")
            ):
                return False
            from_clause = b.args.get("from_") or b.args.get("from")
            if from_clause is None:
                return False
            tables = list(from_clause.find_all(exp.Table))
            if len(tables) != 1 or list(from_clause.find_all(exp.Subquery)):
                return False
            return True

        for side in ("this", "expression"):
            side_node = target.args.get(side)
            if side_node is None:
                continue
            # Only lift Select bodies — Union sides are already handled
            # by ``extract_union_branches_to_ctes``.
            if not isinstance(side_node, exp.Select):
                continue
            if _is_already_lifted_setop_side(side_node):
                continue
            cte_name = _make_cte_name(
                exp.Subquery(this=side_node), fallback_idx, used_names,
            )
            fallback_idx += 1
            new_ctes.append(exp.CTE(
                this=side_node.copy(),
                alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
            ))
            target.set(
                side,
                exp.Select(expressions=[exp.Star()]).from_(
                    exp.Table(this=exp.to_identifier(cte_name))
                ),
            )
            rewrote_any = True

    if not rewrote_any:
        return sql, findings

    # Merge the new CTEs into the target's WITH clause. Note the
    # "with_" key — sqlglot's convention to avoid the Python keyword.
    if existing_with:
        existing_with.set(
            "expressions",
            list(existing_with.expressions) + new_ctes,
        )
    else:
        target.set("with_", exp.With(expressions=new_ctes, recursive=False))

    # Render back to SQL. If rendering fails for any reason, fall back
    # to the original string — never produce broken SQL. Preserve the
    # trailing semicolon so apply_auto_fixes' format-normalise pass
    # can re-emit it consistently.
    try:
        rendered = root.sql(pretty=True)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


# -----------------------------------------------------------------------------
# Single-table projection pushdown
# -----------------------------------------------------------------------------
#
# Rewrites the outer SELECT so that single-table column picks and pure
# renames, plus single-table WHERE predicates, move into a per-source
# CTE named ``<table>_filtered`` (when a WHERE predicate is also
# pushed) or ``<table>_prepared`` (projections only, no filter).
# Matches the customer naming convention (CUSTOMER_RULES.md rule 7).
#
# Split of concerns between layers:
#   - Source CTE — the "what this table exposes" layer. Bare columns
#     and pure renames (``col AS alias``) only. Single-table WHERE
#     predicates may also be pushed (a filter is not a value transform).
#   - Outer SELECT (the joining layer) — the "final formatting" layer.
#     CAST, CASE, COALESCE, arithmetic, function calls, and constants
#     all live HERE, even when single-source. This keeps casting and
#     defaulting next to the joins so a reader sees the whole
#     formatting/result shape in one place.
#
# Skipped sources:
#   - CTEs already defined in this file's WITH (except for "passthrough"
#     CTEs of shape ``SELECT * FROM real_table [WHERE ...]``, which the
#     pushdown mutates in place — see ``_passthrough_cte_target``).
#   - Tables with nothing to push (no renames, no single-table filters).
#     Avoids creating identity CTEs.


def _expression_uses_only(
    node: exp.Expression,
    alias: str,
    other_aliases: Set[str],
) -> bool:
    """True iff every Column reference in ``node`` is qualified with
    ``alias`` (or unqualified — in which case it's ambiguous and we
    play safe by treating it as cross-table)."""
    columns = list(node.find_all(exp.Column))
    if not columns:
        # No column refs at all (a literal, a CURRENT_DATE call, etc.).
        # Not single-table — leave it in the outer SELECT so it doesn't
        # get duplicated across multiple per-source CTEs.
        return False
    for col in columns:
        ref = col.table
        if not ref:
            return False  # Unqualified ref — ambiguous in a multi-table query.
        if ref == alias:
            continue
        if ref in other_aliases:
            return False  # References another table in this query.
        # ref is something we don't know — be conservative.
        return False
    return True


def _is_pushable_projection(proj: exp.Expression) -> bool:
    """True if a projection MIGHT belong in a source CTE.

    Rule (2026-05-12 revised): allow bare columns, renames, AND
    non-cast value transforms (UPPER, TRIM, arithmetic, etc.) — the
    caller still needs to verify single-source ownership before
    actually pushing. Reject anything that's an aggregate, window,
    subquery, ``*``, CAST/TRY_CAST, CASE, COALESCE/NULLIF/IFNULL/NVL,
    or a pure literal (constants don't depend on any source).

    Rationale: the source CTE owns ALL single-table work — bare
    columns, renames, and value transforms (`UPPER(c.email)`,
    `c.amount + c.tax`, `SUBSTRING(c.addr, 1, 5)`). Casting and
    defaulting still stay outer (they're formatting concerns).
    Multi-source derivations stay in the joined CTE.

    Pushable (subject to single-source ownership check downstream):
      - ``c.foo``                       (bare column)
      - ``c.foo AS bar``                (pure rename)
      - ``UPPER(c.email) AS clean``     (single-source transform)
      - ``c.x + c.tax AS total``        (single-source arithmetic)
      - ``SUBSTRING(c.addr, 1, 5)``     (single-source function)

    Not pushable (stays at outer SELECT, or in joined CTE if
    multi-source):
      - ``CAST(c.foo AS x)``                          (cast)
      - ``CASE WHEN c.flag = 'Y' THEN TRUE ... END``  (case / default)
      - ``COALESCE(c.foo, c.bar)``                    (default)
      - ``'literal' AS valuation_type`` / ``1 AS qty`` (constants)
      - ``SUM(c.x)`` / ``ROW_NUMBER() OVER (...)``    (agg / window)
    """
    if isinstance(proj, exp.Column):
        return True
    if isinstance(proj, exp.Alias) and isinstance(proj.this, exp.Column):
        return True
    # Disqualify formatting / non-source-CTE shapes.
    if _expression_has_aggregate(proj):
        return False
    if any(proj.find_all(exp.Window)):
        return False
    if any(proj.find_all(exp.Subquery)):
        return False
    if any(proj.find_all(exp.Star)):
        return False
    if any(proj.find_all(exp.Cast)) or any(proj.find_all(exp.TryCast)):
        return False
    if any(proj.find_all(exp.Case)):
        return False
    if any(proj.find_all(exp.Coalesce)) or any(proj.find_all(exp.Nullif)):
        return False
    for fn in proj.find_all(exp.Func):
        name = (fn.key or "").lower()
        if name in _FORMATTING_FN_NAMES:
            return False
    # Pure literal projection (no Column refs anywhere) — stays outer.
    if isinstance(proj, exp.Literal):
        return False
    if isinstance(proj, exp.Alias) and isinstance(proj.this, exp.Literal):
        return False
    if not any(proj.find_all(exp.Column)):
        return False
    return True


def _split_and(predicate: exp.Expression) -> List[exp.Expression]:
    """Flatten an AND-tree into a list of leaf predicates."""
    if isinstance(predicate, exp.And):
        return _split_and(predicate.this) + _split_and(predicate.expression)
    return [predicate]


def _rebuild_and(parts: List[exp.Expression]) -> Optional[exp.Expression]:
    """Inverse of _split_and."""
    if not parts:
        return None
    out = parts[0]
    for p in parts[1:]:
        out = exp.And(this=out, expression=p)
    return out


def _push_column_name(proj: exp.Expression, fallback_idx: int) -> Tuple[str, int]:
    """Pick the column name a pushed projection should expose in the
    per-source CTE. Prefers the existing alias; falls back to the bare
    column name; finally falls back to ``col_<n>``."""
    if isinstance(proj, exp.Alias):
        return proj.alias, fallback_idx
    if isinstance(proj, exp.Column):
        return proj.name, fallback_idx
    return f"col_{fallback_idx}", fallback_idx + 1


def push_projections_to_source_ctes(
    sql: str,
    findings: List[QualityFinding],
    metadata_blacklist: Optional[List[str]] = None,
) -> Tuple[str, List[QualityFinding]]:
    """Rewrite the outermost SELECT so that single-table projections
    and filters move into ``<table>_filtered`` / ``<table>_prepared``
    CTEs (matching the customer naming convention).

    Safe-by-default: anything ambiguous, cross-table, aggregate,
    windowed, or sourced from an existing CTE is left in place.

    ``metadata_blacklist`` (when provided) filters out blacklisted
    columns from the source CTE's "expose these too" pass, so a
    metadata column referenced anywhere in the AST (a leftover WHERE
    in another CTE, a JOIN ON elsewhere, etc.) doesn't get
    re-introduced into the source CTE we're building.
    """
    _meta_lower = (
        {b.lower() for b in metadata_blacklist}
        if metadata_blacklist
        else set()
    )
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    root = statements[0]
    if isinstance(root, exp.Create) and root.this:
        target = root.expression or root.this
    elif isinstance(root, exp.Query):
        target = root
    else:
        target = root.find(exp.Select)
    if not isinstance(target, exp.Select):
        return sql, findings

    # Existing CTEs — by default off-limits as pushdown targets.
    # Exception: a "passthrough" CTE shaped exactly as
    # ``SELECT * FROM <real_table> [WHERE ...]`` is treated as a thin
    # wrapper over the real table — we push projections / predicates
    # INTO that CTE's body (rewriting ``*`` to the actual projection
    # list and AND-merging the WHERE). Anything more complex (joins,
    # GROUP BY, UNION, explicit columns, DISTINCT, ...) stays off
    # limits.
    existing_with = target.args.get("with_")
    existing_cte_names: Set[str] = set()
    passthrough_ctes: Dict[str, Tuple[exp.CTE, exp.Select, exp.Table]] = {}
    # ^ keyed by CTE name -> (CTE node, inner SELECT, underlying Table)
    if existing_with:
        for cte in existing_with.expressions:
            cte_name = cte.alias_or_name
            existing_cte_names.add(cte_name)
            pt = _passthrough_cte_target(cte)
            if pt is not None:
                inner_select, real_table = pt
                passthrough_ctes[cte_name] = (cte, inner_select, real_table)

    # Collect base-table sources (table + alias) from the outer FROM
    # and JOINs. Skip references to non-passthrough existing CTEs.
    # For passthrough CTEs, treat the underlying real table as the
    # push target but remember the CTE so we mutate IT instead of
    # creating a new sibling CTE. sqlglot stores FROM under
    # "from_" (trailing underscore, like "with_").
    from_clause = target.args.get("from_") or target.args.get("from")
    if not from_clause:
        return sql, findings

    # (table_name, alias, table_node, passthrough_cte_name_or_None)
    sources: List[Tuple[str, str, exp.Table, Optional[str]]] = []
    other_aliases: Set[str] = set()

    def collect_table(t: exp.Table) -> None:
        name = t.name
        if not name:
            return
        if name in existing_cte_names:
            if name in passthrough_ctes:
                # Push INTO this CTE. The "real" table for pushdown is
                # the table inside the CTE body; the outer alias used
                # by the outer SELECT is the CTE alias (or its name).
                _, _, real_table = passthrough_ctes[name]
                alias = t.alias or name
                sources.append((real_table.name, alias, real_table, name))
                other_aliases.add(alias)
                return
            # Non-passthrough user-defined CTE. Treat it as a "table"
            # source for pushdown: a sibling ``<cte_name>_prepared``
            # CTE will be emitted between this CTE and the outer
            # SELECT, capturing single-source projections/derivations
            # that would otherwise leak into the joined CTE.
            # The "table_name" we pass is the CTE name itself; the
            # repoint step at apply-time will set the outer's FROM
            # to ``<cte_name>_prepared``.
            alias = t.alias or name
            sources.append((name, alias, t, None))
            other_aliases.add(alias)
            return
        alias = t.alias or name
        sources.append((name, alias, t, None))
        other_aliases.add(alias)

    # FROM side
    for t in from_clause.find_all(exp.Table):
        collect_table(t)
    # JOIN side
    for join in target.args.get("joins") or []:
        for t in join.find_all(exp.Table):
            collect_table(t)

    if not sources:
        return sql, findings

    # ------------------------------------------------------------------
    # Plan the pushdown per source.
    # ------------------------------------------------------------------
    @dataclass
    class PushPlan:
        table_name: str
        alias: str
        table_node: exp.Table
        projections: List[Tuple[exp.Expression, str]] = field(default_factory=list)
        # ^ list of (original projection, new column name in CTE)
        predicates: List[exp.Expression] = field(default_factory=list)
        outer_replacements: Dict[int, exp.Expression] = field(default_factory=dict)
        # ^ keyed by id(original projection) -> simple `alias.new_name` ref
        passthrough_cte_name: Optional[str] = None
        # ^ When set, the source is an existing passthrough CTE
        # (``WITH name AS (SELECT * FROM real_table [WHERE ...])``).
        # The apply step mutates that CTE's body in place instead of
        # creating a new ``<table>_prepared`` / ``<table>_filtered``
        # sibling CTE.

    plans: Dict[str, PushPlan] = {
        alias: PushPlan(
            table_name=tn,
            alias=alias,
            table_node=tnode,
            passthrough_cte_name=pt_name,
        )
        for tn, alias, tnode, pt_name in sources
    }
    fallback_idx = 1

    # Used CTE names so generated per-source names don't collide.
    used_cte_names: Set[str] = set(existing_cte_names)

    # Walk outer projections and decide which to push.
    new_outer_projections: List[exp.Expression] = []
    for proj in target.expressions:
        if not _is_pushable_projection(proj):
            new_outer_projections.append(proj)
            continue

        # Identify which (single) source this projection belongs to.
        owning_alias: Optional[str] = None
        for alias in plans:
            others = {a for a in plans if a != alias} | (other_aliases - {alias})
            if _expression_uses_only(proj, alias, others):
                owning_alias = alias
                break
        if owning_alias is None:
            new_outer_projections.append(proj)
            continue

        plan = plans[owning_alias]
        col_name, fallback_idx = _push_column_name(proj, fallback_idx)
        plan.projections.append((proj, col_name))
        # Outer SELECT replacement: `alias.col_name`, preserving any
        # original alias so downstream consumers see the same name.
        ref = exp.column(col_name, table=owning_alias)
        original_alias = proj.alias if isinstance(proj, exp.Alias) else None
        if original_alias and original_alias != col_name:
            ref = exp.alias_(ref, original_alias)
        plan.outer_replacements[id(proj)] = ref
        new_outer_projections.append(ref)

    # Walk the outer WHERE and decide which leaf predicates to push.
    where = target.args.get("where")
    new_where_parts: List[exp.Expression] = []
    if where is not None:
        for pred in _split_and(where.this):
            owning_alias = None
            for alias in plans:
                others = {a for a in plans if a != alias} | (other_aliases - {alias})
                if _expression_uses_only(pred, alias, others):
                    owning_alias = alias
                    break
            if owning_alias is None:
                new_where_parts.append(pred)
            else:
                plans[owning_alias].predicates.append(pred)

    # ------------------------------------------------------------------
    # Bail out if no source has anything to push.
    # ------------------------------------------------------------------
    if not any(p.projections or p.predicates for p in plans.values()):
        return sql, findings

    # Apply the outer SELECT + WHERE rewrites FIRST. The per-source
    # CTE-build loop below uses ``_collect_alias_columns(target, ...)``
    # to figure out which additional source columns to expose; that
    # must run against the POST-rewrite outer query so it doesn't
    # re-discover the projections we just moved into the CTE.
    target.set("expressions", new_outer_projections)
    if where is not None:
        new_where = _rebuild_and(new_where_parts)
        if new_where is None:
            target.set("where", None)
        else:
            where.set("this", new_where)

    # ------------------------------------------------------------------
    # Apply: build per-source CTEs and rewrite the outer SELECT.
    # Naming follows the customer convention (see CUSTOMER_RULES.md
    # rule 7): ``<table>_filtered`` when a WHERE predicate is pushed,
    # ``<table>_prepared`` when only projections are pushed (no
    # filter). For passthrough CTEs (``WITH x AS (SELECT * FROM t
    # [WHERE ...])``) we MUTATE the existing CTE's body in place
    # instead of adding a new sibling CTE — preserves the user's CTE
    # name and avoids stacking ``x`` + ``t_prepared`` for the same
    # source.
    # ------------------------------------------------------------------
    new_ctes: List[exp.CTE] = []
    for plan in plans.values():
        if not (plan.projections or plan.predicates):
            continue

        # Build the CTE's projection list. Same logic for new CTEs and
        # for mutating an existing passthrough CTE.
        cte_projections: List[exp.Expression] = []
        already_projected: Set[str] = set()
        # Also dedupe against case-insensitive duplicates so a column
        # already projected as ``PortfolioBalance`` doesn't get
        # exposed again as ``portfoliobalance``.
        already_projected_lower: Set[str] = set()
        for original, col_name in plan.projections:
            inner = _strip_alias(original, plan.alias)
            if isinstance(original, exp.Alias) or not _is_simple_column_named(inner, col_name):
                inner = exp.alias_(inner, col_name)
            cte_projections.append(inner)
            already_projected.add(col_name)
            already_projected_lower.add(col_name.lower())

        # Find every column the OUTER query references via this alias
        # (in projections, JOIN ON, WHERE, etc.). For each such
        # reference that pushdown didn't already cover via an aliased
        # projection of the SAME output name, expose the raw column
        # in the CTE. This is what keeps ``prp.Product`` resolvable
        # in a JOIN ON when there's also a pushed
        # ``Product AS financing_product_id`` projection — the JOIN
        # needs the raw ``Product`` and the projection produces a
        # different output name.
        referenced_columns = _collect_alias_columns(target, plan.alias)
        for col_name in sorted(referenced_columns):
            if col_name in already_projected or col_name.lower() in already_projected_lower:
                continue
            # Skip metadata columns — they may still be referenced
            # in leftover predicates / other CTEs that the strip pass
            # didn't touch, but they should never resurface in a
            # source CTE's output.
            if col_name.lower() in _meta_lower:
                continue
            cte_projections.append(exp.column(col_name))
            already_projected.add(col_name)
            already_projected_lower.add(col_name.lower())

        # If we still have zero columns (e.g., the alias is only used
        # in a JOIN ON condition that itself got rewritten away),
        # synthesise a single placeholder so the CTE remains valid.
        if not cte_projections:
            cte_projections.append(exp.column(plan.table_name + "_id"))

        # Rewrite the pushed predicates similarly.
        cte_predicates = [_strip_alias(p, plan.alias) for p in plan.predicates]

        if plan.passthrough_cte_name is not None:
            # Mutate the existing passthrough CTE: replace its
            # ``SELECT *`` projection list with the pushed columns
            # and AND-merge new predicates into its existing WHERE.
            cte_node, inner_select, _real_table = passthrough_ctes[
                plan.passthrough_cte_name
            ]
            inner_select.set("expressions", cte_projections)
            existing_pred = (
                inner_select.args.get("where").this
                if inner_select.args.get("where") is not None
                else None
            )
            merged_parts: List[exp.Expression] = []
            if existing_pred is not None:
                merged_parts.extend(_split_and(existing_pred))
            merged_parts.extend(cte_predicates)
            merged_pred = _rebuild_and(merged_parts)
            if merged_pred is None:
                inner_select.set("where", None)
            else:
                inner_select.set("where", exp.Where(this=merged_pred))
            # Don't repoint the outer source — it already references
            # ``plan.passthrough_cte_name``.
            continue

        # Pick a unique CTE name for a brand-new sibling CTE.
        suffix = "_filtered" if plan.predicates else "_prepared"
        base = f"{plan.table_name}{suffix}"
        cte_name = base
        counter = 2
        while cte_name in used_cte_names:
            cte_name = f"{base}_{counter}"
            counter += 1
        used_cte_names.add(cte_name)

        # Preserve the original qualifier (catalog.schema) on the
        # source CTE's FROM so downstream passes (e.g.,
        # ``apply_table_qualifier``) can still normalise it. Strip the
        # alias — inside the CTE we don't need one. Strip any pivots
        # that the original carried.
        source_table_node = plan.table_node.copy()
        source_table_node.set("alias", None)
        source_table_node.set("pivots", None)
        # Strip the quoted-identifier flag on every Identifier inside
        # the FROM. Customer SQL often uses ``FROM "my_table"`` even
        # when the name doesn't need quoting; the rewrite should
        # produce bare identifiers in the source CTE.
        for ident in source_table_node.find_all(exp.Identifier):
            if ident.args.get("quoted"):
                ident.set("quoted", False)
        cte_select = exp.Select(expressions=cte_projections).from_(
            source_table_node
        )
        combined_pred = _rebuild_and(cte_predicates)
        if combined_pred is not None:
            cte_select = cte_select.where(combined_pred)

        new_ctes.append(exp.CTE(
            this=cte_select,
            alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
        ))

        # Repoint the outer source from <db.table> to <cte_name>.
        # Clear any catalog/db qualifier — the new reference is a
        # bare CTE name.
        plan.table_node.set("this", exp.to_identifier(cte_name))
        plan.table_node.set("db", None)
        plan.table_node.set("catalog", None)
        # Keep the original alias on the table node so outer refs
        # (`c.foo`) stay valid.
        if not plan.table_node.alias:
            plan.table_node.set(
                "alias",
                exp.TableAlias(this=exp.to_identifier(plan.alias)),
            )

    # Merge new CTEs into the WITH clause. Order matters: each new
    # ``_prepared`` / ``_filtered`` CTE must come AFTER any user-
    # defined CTE it depends on. We insert each new CTE right after
    # its dependency if it reads from one, otherwise prepend it.
    if existing_with:
        cur = list(existing_with.expressions)
        existing_names = {c.alias_or_name for c in cur}
        for new_cte in new_ctes:
            # Find the base name this CTE reads from.
            body = new_cte.this
            tables_in_body = (
                [t.name for t in body.find_all(exp.Table) if t.name]
                if hasattr(body, "find_all")
                else []
            )
            dep_name = next(
                (n for n in tables_in_body if n in existing_names),
                None,
            )
            if dep_name is None:
                # No dependency on a user CTE — declare first.
                cur = [new_cte] + cur
                continue
            # Insert right after the dependency.
            idx = next(
                i for i, c in enumerate(cur) if c.alias_or_name == dep_name
            )
            cur.insert(idx + 1, new_cte)
            existing_names.add(new_cte.alias_or_name)
        existing_with.set("expressions", cur)
    else:
        target.set("with_", exp.With(expressions=new_ctes, recursive=False))

    try:
        rendered = root.sql(pretty=True)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


def _strip_alias(node: exp.Expression, alias: str) -> exp.Expression:
    """Return a copy of ``node`` with every ``alias.col`` reference
    rewritten to bare ``col``. Used when moving an expression inside a
    single-table CTE."""
    out = node.copy()
    for col in list(out.find_all(exp.Column)):
        if col.table == alias:
            col.set("table", None)
    return out


def _passthrough_cte_target(
    cte: exp.CTE,
) -> Optional[Tuple[exp.Select, exp.Table]]:
    """If ``cte`` is shaped exactly ``SELECT * FROM <real_table>
    [WHERE ...]`` (no JOIN, no GROUP BY, no UNION, no DISTINCT, no
    explicit column list), return ``(inner SELECT, underlying Table)``.

    These are the CTEs we treat as thin wrappers — the
    projection-pushdown pass can mutate their body in place
    instead of adding a sibling ``<table>_prepared`` CTE.

    Returns ``None`` for anything more complex.
    """
    body = cte.this
    if not isinstance(body, exp.Select):
        return None
    # Must be a single SELECT — bail on UNION / INTERSECT / EXCEPT.
    if isinstance(body, exp.Union) or body.args.get("unions"):
        return None
    if body.args.get("distinct"):
        return None
    if body.args.get("group"):
        return None
    if body.args.get("having"):
        return None
    if body.args.get("qualify"):
        return None
    if body.args.get("joins"):
        return None
    # Projection list must be exactly one ``*``.
    projections = body.expressions or []
    if len(projections) != 1 or not isinstance(projections[0], exp.Star):
        return None
    # FROM must be a single base table.
    from_clause = body.args.get("from_") or body.args.get("from")
    if from_clause is None:
        return None
    tables = list(from_clause.find_all(exp.Table))
    subqueries = list(from_clause.find_all(exp.Subquery))
    if len(tables) != 1 or subqueries:
        return None
    return body, tables[0]


def _collect_alias_columns(node: exp.Expression, alias: str) -> Set[str]:
    """Return the set of column names referenced as ``<alias>.<col>``
    in ``node``'s OWN scope — that is, in its projection list,
    WHERE / GROUP BY / HAVING / QUALIFY / ORDER BY clauses, and the
    JOIN ON conditions directly attached to it. References inside
    other CTE bodies attached via ``with_`` are NOT counted — those
    have their own scope and (often) a same-named alias that means
    something different in their context.

    Used by the projection-pushdown transform to expose exactly the
    columns the outer query reads from a per-source CTE — no more
    `SELECT *` placeholders.
    """
    out: Set[str] = set()
    if alias is None:
        return out

    def _add_from(expr: Optional[exp.Expression]) -> None:
        if expr is None:
            return
        for col in expr.find_all(exp.Column):
            # Skip columns that live inside a CTE body — that's a
            # different scope, even if the alias name matches.
            cur = col.parent
            in_other_cte = False
            while cur is not None and cur is not expr:
                if isinstance(cur, exp.CTE):
                    in_other_cte = True
                    break
                cur = cur.parent
            if in_other_cte:
                continue
            if col.table == alias and col.name:
                out.add(col.name)

    # Only walk the OWN slots of ``node`` — projections, WHERE,
    # GROUP / HAVING / QUALIFY / ORDER, FROM/JOIN. Skip ``with_``
    # which holds OTHER CTEs.
    if isinstance(node, exp.Select):
        for proj in node.expressions or []:
            _add_from(proj)
        _add_from(node.args.get("where"))
        _add_from(node.args.get("group"))
        _add_from(node.args.get("having"))
        _add_from(node.args.get("qualify"))
        _add_from(node.args.get("order"))
        # FROM/JOIN ON references — those legitimately reference
        # the source we're pushing down to.
        from_clause = node.args.get("from_") or node.args.get("from")
        _add_from(from_clause)
        for join in node.args.get("joins") or []:
            _add_from(join)
    else:
        # Fallback: walk the whole subtree (legacy callers).
        for col in node.find_all(exp.Column):
            if col.table == alias and col.name:
                out.add(col.name)
    return out


def _is_simple_column_named(node: exp.Expression, name: str) -> bool:
    return isinstance(node, exp.Column) and node.name == name


# -----------------------------------------------------------------------------
# Joined CTE extraction
# -----------------------------------------------------------------------------
#
# When the outer SELECT has at least one JOIN, lift the JOIN(s) and
# any non-cast single-source derivations into a dedicated
# ``<entity>_joined`` CTE. The outer SELECT then reads from a single-
# table FROM and no longer has any JOIN of its own — which lets the
# aggregation CTE (next pass) just GROUP BY a single-table input
# instead of carrying its own JOIN logic.
#
# Goal layering:
#   <table>_prepared    sources, renames + filters    (existing pass)
#   <entity>_joined     JOINs + non-cast derivations  (this pass)
#   <entity>_aggregated joined input + SUM/MAX/...    (next pass)
#   outer SELECT        CAST + CASE + COALESCE + literals
#
# When aggregation extraction also fires, the agg CTE reads FROM the
# joined CTE (no JOIN clauses in the agg CTE — it just GROUP BYs the
# joined output).
#
# Non-cast derivation examples (LIFTED into the joined CTE):
#   - UPPER(c.first_name) AS first_name_clean
#   - c.amount + c.tax AS total
#   - SUBSTR(c.email, INSTR(c.email, '@') + 1) AS email_domain
#   - c.quantity * c.unit_price AS line_total
#
# Excluded (these stay at the outer SELECT — formatting concerns):
#   - CAST(...)
#   - CASE WHEN ... END
#   - COALESCE(...) / NULLIF(...) / IFNULL(...)
#   - bare columns and ``column AS alias`` renames (already in sources)
#   - literal projections (constants stay outer)


_FORMATTING_FN_NAMES = frozenset({
    "cast", "trycast", "try_cast", "convert",
    "coalesce", "nullif", "ifnull", "nvl", "isnull",
})


def _is_non_cast_derivation(proj: exp.Expression) -> bool:
    """True if ``proj`` is a value transform we want in the joined
    CTE rather than the outer SELECT.

    Rule: not a bare column/rename, not a literal/constant, no
    CAST/CASE/COALESCE/NULLIF/IFNULL anywhere. Must contain at least
    one Column reference (so it depends on data, not just literals).
    Aggregates / windows / subqueries / stars stay outer.
    """
    if isinstance(proj, exp.Column):
        return False
    if isinstance(proj, exp.Alias):
        inner = proj.this
        if isinstance(inner, exp.Column):
            return False
        if isinstance(inner, exp.Literal):
            return False
    else:
        inner = proj
        if isinstance(inner, exp.Literal):
            return False
    if _expression_has_aggregate(proj):
        return False
    if any(proj.find_all(exp.Window)):
        return False
    if any(proj.find_all(exp.Subquery)):
        return False
    if any(proj.find_all(exp.Star)):
        return False
    if any(proj.find_all(exp.Cast)) or any(proj.find_all(exp.TryCast)):
        return False
    if any(proj.find_all(exp.Case)):
        return False
    if any(proj.find_all(exp.Coalesce)) or any(proj.find_all(exp.Nullif)):
        return False
    # Reject if any descendant function's name is in the formatting list.
    for fn in proj.find_all(exp.Func):
        name = (fn.key or "").lower()
        if name in _FORMATTING_FN_NAMES:
            return False
    # Must reference at least one Column.
    if not any(proj.find_all(exp.Column)):
        return False
    return True


def _from_table_name(target: exp.Select) -> Optional[str]:
    """Return the bare name of the FIRST source table in the outer
    SELECT's FROM clause (without qualifier). Falls back to ``None``
    when no FROM is present or the first source isn't a Table.

    Used to derive CTE names like ``product_joined`` /
    ``product_aggregated`` from the data the query is about, rather
    than from the file's entity hint which is often less specific.
    """
    from_clause = target.args.get("from_") or target.args.get("from")
    if from_clause is None:
        return None
    tbl = from_clause.this
    if isinstance(tbl, exp.Table) and tbl.name:
        return tbl.name
    # Could be a subquery; walk to find the first table.
    first = next(from_clause.find_all(exp.Table), None)
    if first and first.name:
        return first.name
    return None


def _cte_name_prefix(target: exp.Select, entity_hint: str) -> str:
    """Pick the prefix for layered CTE names. Prefers the bare name
    of the first FROM source (so a query starting ``FROM product``
    yields ``product_joined`` / ``product_aggregated``). Falls back
    to ``entity_hint`` when the FROM source has no recognisable
    name. Strips a single trailing ``_prepared`` / ``_filtered`` /
    ``_joined`` / ``_aggregated`` so a subsequent layer doesn't
    double-stack the suffix when the input is already layered.
    """
    base = _from_table_name(target) or entity_hint or "result"
    for suffix in ("_prepared", "_filtered", "_joined", "_aggregated"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base


def extract_joined_cte(
    sql: str,
    findings: List[QualityFinding],
    entity_hint: str,
    log: "TransformLog",
) -> Tuple[str, List[QualityFinding]]:
    """Lift JOINs + non-cast single-source derivations into a
    dedicated ``<entity>_joined`` CTE.

    Fires when ALL of:
      - The top-level SELECT has at least one JOIN.
      - At least one projection is a non-cast derivation (see
        ``_is_non_cast_derivation``).
      - There is no UNION at the top level.

    Strategy:
      1. Build a new SELECT (the joined CTE body) that takes the
         outer FROM + JOINs + WHERE, and projects:
           - every bare column / rename / non-cast derivation from
             the outer, with a stable column alias;
           - every other source column the outer SELECT still
             references (in casts, cases, coalesces, aggregates)
             exposed as a bare column.
      2. Outer SELECT then reads from a single-table FROM
         (the joined CTE), with JOINs and WHERE moved into the CTE.
         Bare/derivation projections become qualifier-less references
         to the joined CTE's columns. Casts/cases/coalesces keep
         their structure but reference joined CTE columns.

    Safety: fail-soft on parse or analysis errors.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    root = statements[0]
    if isinstance(root, exp.Create) and root.this:
        target = root.expression or root.this
    elif isinstance(root, exp.Query):
        target = root
    else:
        target = root.find(exp.Select)
    if not isinstance(target, exp.Select):
        return sql, findings

    # Skip top-level UNION.
    if target.args.get("unions"):
        return sql, findings
    if isinstance(root, exp.Union):
        return sql, findings

    # Must have at least one JOIN. The joined CTE always fires when
    # the outer SELECT has a multi-source FROM, even without any
    # non-cast derivations — its purpose is also to hand the
    # aggregation CTE a single-table input.
    joins = target.args.get("joins") or []
    if not joins:
        return sql, findings

    outer_projections = list(target.expressions)

    # Prefer FROM-table name over file entity hint for naming.
    prefix = _cte_name_prefix(target, entity_hint)
    cte_name = f"{prefix}_joined"

    # Plan the joined CTE's projection list and the outer's replacement
    # column references.
    joined_projections: List[exp.Expression] = []
    used_names: Set[str] = set()
    # Map id(orig_outer_proj) -> output column name in joined CTE.
    proj_to_joined_name: Dict[int, str] = {}

    def _alloc_name(base: str) -> str:
        name = base
        n = 2
        while name in used_names:
            name = f"{base}_{n}"
            n += 1
        used_names.add(name)
        return name

    for proj in outer_projections:
        # Bare column ``alias.col`` or ``col`` — joined CTE projects
        # it (qualifier dropped) under the same name; outer reads
        # the bare name.
        if isinstance(proj, exp.Column):
            col_name = proj.name
            alloc = _alloc_name(col_name)
            inner = exp.column(col_name)  # bare, no qualifier
            joined_projections.append(
                exp.alias_(inner, alloc) if alloc != col_name else inner
            )
            proj_to_joined_name[id(proj)] = alloc
            continue
        # ``alias.col AS out_name`` rename — joined CTE projects
        # under ``out_name`` (renamed at this layer already).
        if isinstance(proj, exp.Alias) and isinstance(proj.this, exp.Column):
            inner_col = proj.this
            out_name = proj.alias
            alloc = _alloc_name(out_name)
            joined_projections.append(
                exp.alias_(exp.column(inner_col.name), alloc)
                if alloc != inner_col.name else exp.column(inner_col.name)
            )
            proj_to_joined_name[id(proj)] = alloc
            continue
        # Non-cast derivation — joined CTE evaluates the expression
        # under its alias (or a derived alias).
        if _is_non_cast_derivation(proj):
            if isinstance(proj, exp.Alias):
                out_name = proj.alias
                value = proj.this
            else:
                out_name = f"derived_{len(joined_projections) + 1}"
                value = proj
            alloc = _alloc_name(out_name)
            joined_projections.append(exp.alias_(value.copy(), alloc))
            proj_to_joined_name[id(proj)] = alloc
            continue
        # Everything else (CAST, CASE, COALESCE, aggregate, window,
        # literal) stays at the outer SELECT. The joined CTE needs to
        # expose any source columns these expressions reference, so
        # the outer can find them by name.
        for col in proj.find_all(exp.Column):
            col_name = col.name
            if col_name in used_names:
                continue
            joined_projections.append(exp.column(col_name))
            used_names.add(col_name)

    # Also expose any source columns referenced in the outer WHERE so
    # the joined CTE's body (and a downstream filtered CTE that reads
    # from the joined CTE) can resolve them.
    where = target.args.get("where")
    if where is not None:
        for col in where.find_all(exp.Column):
            col_name = col.name
            if col_name in used_names:
                continue
            joined_projections.append(exp.column(col_name))
            used_names.add(col_name)

    # Build the joined-CTE Select (FROM + JOINs + WHERE preserved).
    joined_select = exp.Select(expressions=joined_projections)
    from_clause = target.args.get("from_") or target.args.get("from")
    if from_clause is not None:
        joined_select.set("from_", from_clause.copy())
    joined_select.set("joins", [j.copy() for j in joins])
    if where is not None:
        joined_select.set("where", where.copy())

    new_cte = exp.CTE(
        this=joined_select,
        alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
    )

    # Rewrite the OUTER SELECT.
    new_outer_projections: List[exp.Expression] = []
    for proj in outer_projections:
        joined_name = proj_to_joined_name.get(id(proj))
        if joined_name is not None:
            # Lifted into joined CTE — reference its output column.
            original_alias = (
                proj.alias if isinstance(proj, exp.Alias) else None
            )
            ref = exp.column(joined_name)
            if original_alias and original_alias != joined_name:
                ref = exp.alias_(ref, original_alias)
            new_outer_projections.append(ref)
            continue
        # Kept at outer: strip qualifiers (single source = joined CTE).
        proj_copy = proj.copy()
        for col in proj_copy.find_all(exp.Column):
            col.set("table", None)
        new_outer_projections.append(proj_copy)

    target.set("expressions", new_outer_projections)
    target.set(
        "from_",
        exp.From(this=exp.Table(this=exp.to_identifier(cte_name))),
    )
    target.set("joins", None)
    target.set("where", None)
    # GROUP BY / HAVING that survived from the input still reference
    # original source aliases (``c.customer_id``). After joined-CTE
    # extraction the outer FROM is the joined CTE itself with no
    # alias, so strip those qualifiers so the next agg-CTE pass sees
    # consistent column references.
    group = target.args.get("group")
    if group is not None:
        for col in group.find_all(exp.Column):
            col.set("table", None)
    having = target.args.get("having")
    if having is not None:
        for col in having.find_all(exp.Column):
            col.set("table", None)
    # ORDER BY / QUALIFY also reference the (now-gone) source aliases.
    order = target.args.get("order")
    if order is not None:
        for col in order.find_all(exp.Column):
            col.set("table", None)
    qualify = target.args.get("qualify")
    if qualify is not None:
        for col in qualify.find_all(exp.Column):
            col.set("table", None)

    # Merge the new CTE into the WITH clause.
    existing_with = target.args.get("with_")
    if existing_with:
        existing_with.set(
            "expressions",
            list(existing_with.expressions) + [new_cte],
        )
    else:
        target.set("with_", exp.With(expressions=[new_cte], recursive=False))

    log.joined_cte_extracted = True

    try:
        rendered = root.sql(pretty=True)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


# -----------------------------------------------------------------------------
# Aggregation CTE extraction
# -----------------------------------------------------------------------------
#
# When the top-level SELECT mixes aggregations (SUM, MAX, COUNT, ...)
# with non-aggregate projections AND casts/defaults, lift the join +
# aggregation into a dedicated ``<entity>_aggregated`` CTE. The outer
# SELECT then reads from that CTE and applies the casting / defaulting
# / constants.
#
# Goal layering:
#   <table>_prepared    sources, renames + filters    (existing pass)
#   <entity>_aggregated joins + SUM/MAX/... + GROUP BY (this pass)
#   outer SELECT        CAST + CASE + COALESCE + literals
#
# Heuristic for ``<entity>`` name:
#   1. The SQL file's CREATE [OR REPLACE] VIEW name (if present).
#   2. The file's annotation `entity` value (if present).
#   3. Otherwise: the filename stem.
# The caller passes ``entity_hint`` and we use it; the function itself
# is parser-only and stays naming-agnostic.


def _expression_has_aggregate(node: exp.Expression) -> bool:
    """True if ``node`` contains an aggregate function in its OWN
    scope that is NOT windowed.

    Carve-outs (these aggregates don't trigger the GROUP BY semantics
    we're capturing):
      - Aggregates inside subqueries (scalar subselects, derived
        tables) — their own SELECT scope handles grouping.
      - Aggregates inside ``OVER (...)`` clauses (windowed
        aggregates) — semantically a window function, not a true
        aggregate. ``SUM(x) OVER (...)`` produces one row per input
        row; ``SUM(x)`` (no OVER) collapses rows.
    """
    for agg in node.find_all(exp.AggFunc):
        cur = agg.parent
        in_nested_scope = False
        while cur is not None and cur is not node:
            if isinstance(cur, exp.Window):
                in_nested_scope = True
                break
            if isinstance(cur, (exp.Subquery, exp.Select)) and cur is not node:
                in_nested_scope = True
                break
            cur = cur.parent
        if not in_nested_scope:
            return True
    return False


def _strip_outer_alias_and_qualifiers(
    node: exp.Expression,
    table_aliases: Set[str],
) -> exp.Expression:
    """Return a copy of ``node`` with every ``<alias>.<col>`` reference
    (where alias is in ``table_aliases``) rewritten to bare ``col``.

    Used when moving an expression INTO the aggregation CTE that lives
    one level above the per-source CTEs: at that level columns are
    table-qualified, so renaming a projection's columns to plain
    column refs would break it. We keep the qualifier when it points
    to a source CTE alias, and drop it only for aliases we know we're
    flattening (the join-input aliases)."""
    # Currently we KEEP qualifiers; the agg CTE references columns
    # via their source CTE aliases. This is a placeholder helper for
    # future per-layer flattening.
    return node.copy()


# -----------------------------------------------------------------------------
# Filtered CTE extraction
# -----------------------------------------------------------------------------
#
# When the joined CTE was created and inherited a WHERE clause (cross-
# source predicates that couldn't be pushed into source CTEs), move
# that WHERE into a separate ``<entity>_filtered`` CTE so each CTE
# stays single-concern:
#
#   <entity>_joined    JOIN + multi-source derivations    (no WHERE)
#   <entity>_filtered  WHERE filters over joined output   (this pass)
#   <entity>_aggregated GROUP BY + aggregates             (next pass)
#
# Skipped when:
#   - No joined CTE exists.
#   - Joined CTE has no WHERE clause to lift.


def extract_filtered_cte(
    sql: str,
    findings: List[QualityFinding],
    entity_hint: str,
    log: "TransformLog",
) -> Tuple[str, List[QualityFinding]]:
    """Lift the joined CTE's WHERE into a dedicated
    ``<entity>_filtered`` CTE. The outer SELECT (or whatever currently
    reads from ``<entity>_joined``) gets repointed to the new filtered
    CTE; joined CTE keeps only JOIN + derivation work.

    Fires when ALL of:
      - A ``<entity>_joined`` CTE exists in this file's WITH.
      - That joined CTE has a non-empty WHERE clause.

    Safety: fail-soft on parse / analysis errors.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    root = statements[0]
    if isinstance(root, exp.Create) and root.this:
        target = root.expression or root.this
    elif isinstance(root, exp.Query):
        target = root
    else:
        target = root.find(exp.Select)
    if target is None:
        return sql, findings

    existing_with = target.args.get("with_")
    if existing_with is None:
        return sql, findings

    # Find the joined CTE. We don't know the prefix used here
    # (the extract_joined_cte pass may have used a FROM-table name),
    # so look for ANY existing CTE ending in ``_joined`` whose name
    # is what the outer FROM currently references.
    outer_from = target.args.get("from_") or target.args.get("from")
    referenced_cte_name = None
    if outer_from is not None:
        first_tbl = outer_from.this if isinstance(outer_from.this, exp.Table) else None
        if first_tbl is not None and first_tbl.name.endswith("_joined"):
            referenced_cte_name = first_tbl.name
    if referenced_cte_name is None:
        return sql, findings
    joined_cte_name = referenced_cte_name
    joined_cte: Optional[exp.CTE] = None
    joined_idx: Optional[int] = None
    for i, cte in enumerate(existing_with.expressions):
        if cte.alias_or_name == joined_cte_name:
            joined_cte = cte
            joined_idx = i
            break
    if joined_cte is None:
        return sql, findings

    inner = joined_cte.this
    if not isinstance(inner, exp.Select):
        return sql, findings
    where = inner.args.get("where")
    if where is None:
        return sql, findings

    # Build the filtered CTE: SELECT * FROM <joined_cte> WHERE <where>.
    # Use the same prefix as the joined CTE so the names align
    # (``product_joined`` → ``product_filtered``).
    prefix = joined_cte_name[: -len("_joined")] if joined_cte_name.endswith("_joined") else (entity_hint or "result")
    filtered_cte_name = f"{prefix}_filtered"
    counter = 2
    used_names = {c.alias_or_name for c in existing_with.expressions}
    while filtered_cte_name in used_names:
        filtered_cte_name = f"{prefix}_filtered_{counter}"
        counter += 1

    # Strip the qualifiers from columns in the moved WHERE. They
    # originally pointed at source-CTE aliases (``c.country`` /
    # ``o.payment_method``); after moving the WHERE into the filtered
    # CTE — which reads from the joined CTE with no alias — those
    # qualifiers don't resolve. The joined CTE projects these columns
    # unqualified.
    where_copy = where.copy()
    for col in where_copy.find_all(exp.Column):
        col.set("table", None)

    filtered_select = exp.Select(expressions=[exp.Star()]).from_(
        exp.Table(this=exp.to_identifier(joined_cte_name))
    )
    filtered_select.set("where", where_copy)
    filtered_cte = exp.CTE(
        this=filtered_select,
        alias=exp.TableAlias(this=exp.to_identifier(filtered_cte_name)),
    )

    # Strip the WHERE from the joined CTE — it now lives in filtered.
    inner.set("where", None)

    # Insert filtered CTE right after joined CTE.
    new_ctes = list(existing_with.expressions)
    new_ctes.insert(joined_idx + 1, filtered_cte)
    existing_with.set("expressions", new_ctes)

    # Repoint everything that was reading from <entity>_joined to read
    # from <entity>_filtered instead. That's the outer SELECT's FROM
    # (and possibly later-defined CTE bodies, though those are unusual).
    def _repoint(node: exp.Expression) -> None:
        for tbl in node.find_all(exp.Table):
            if tbl.name == joined_cte_name and not tbl.args.get("db") and not tbl.args.get("catalog"):
                tbl.set("this", exp.to_identifier(filtered_cte_name))

    # Repoint outer SELECT's FROM / JOINs.
    from_clause = target.args.get("from_") or target.args.get("from")
    if from_clause is not None:
        _repoint(from_clause)
    for join in target.args.get("joins") or []:
        _repoint(join)
    # Repoint any LATER CTE body that references the joined CTE. (Earlier
    # CTEs can't see it; the joined CTE itself shouldn't reference itself.)
    for cte in existing_with.expressions[joined_idx + 2:]:
        _repoint(cte.this)

    log.filtered_cte_extracted = True

    try:
        rendered = root.sql(pretty=True)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


def extract_aggregation_cte(
    sql: str,
    findings: List[QualityFinding],
    entity_hint: str,
    log: "TransformLog",
) -> Tuple[str, List[QualityFinding]]:
    """Lift aggregates + GROUP BY into a dedicated ``<entity>_aggregated``
    CTE.

    Fires when ALL of the following are true:
      - The top-level SELECT contains at least one aggregate
        (``SUM``, ``MAX``, ``MIN``, ``COUNT``, ``AVG``, ...).
      - The outer SELECT also has at least one CAST / CASE / COALESCE /
        function call / constant in its projections. (If everything
        outer is just bare columns + aggregates, the aggregation CTE
        wouldn't simplify anything.)
      - There is no UNION at the top level. UNION-of-aggregations is
        out of scope for this transform.

    Strategy: build a new SELECT that takes the existing outer FROM /
    JOIN / WHERE / GROUP BY / HAVING and replaces the projection list
    with:
      - every non-aggregate outer projection that's a bare column /
        rename (kept as-is — these are the implicit GROUP BY keys),
      - every aggregate sub-expression assigned a stable
        ``<source-name>_<agg>`` alias.
    Wrap that SELECT as ``<entity>_aggregated``.

    Rewrite the OUTER SELECT to project from ``<entity>_aggregated``,
    replacing each aggregate occurrence with a reference to its
    pre-aggregated column. Bare/non-aggregate projections become bare
    column references.

    Safety: fail-soft on any parse or analysis problem — return the
    input unchanged.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    root = statements[0]
    if isinstance(root, exp.Create) and root.this:
        target = root.expression or root.this
    elif isinstance(root, exp.Query):
        target = root
    else:
        target = root.find(exp.Select)
    if not isinstance(target, exp.Select):
        return sql, findings

    # Bail on UNION at the top level.
    if target.args.get("unions") or any(target.find_all(exp.Union)):
        # Only bail when the UNION is at the top-level (not nested
        # in a subquery / CTE body).
        parent_union = None
        for u in target.find_all(exp.Union):
            # If `u` is a descendant of an already-collected CTE body,
            # it's not at top-level; ignore. Easier heuristic: if u
            # equals target's parent or target is u.this / u.expression.
            if target is u.this or target is u.expression:
                parent_union = u
                break
        if parent_union is not None:
            return sql, findings

    # Look for aggregates in outer projections only — aggregates
    # inside subqueries / window functions are not our target.
    outer_projections = list(target.expressions)
    has_aggregate = any(_expression_has_aggregate(p) for p in outer_projections)
    if not has_aggregate:
        return sql, findings

    # Look for at least one "formatting" projection — defined as any
    # projection that's NOT just a bare column / rename / bare
    # aggregate. Specifically: an aggregate wrapped in CAST / CASE /
    # COALESCE / function / arithmetic counts as formatting (the
    # outer SELECT can offload the aggregate to the CTE and keep the
    # wrapping formatting alone). A pure CAST/CASE/COALESCE on a
    # bare column also counts.
    def _is_formatting(p: exp.Expression) -> bool:
        if isinstance(p, exp.Column):
            return False
        if isinstance(p, exp.Alias) and isinstance(p.this, exp.Column):
            return False
        # Bare aggregate (e.g., `SUM(x) AS total`) is not formatting —
        # it's an aggregate that the agg CTE will produce directly.
        inner = p.this if isinstance(p, exp.Alias) else p
        if isinstance(inner, exp.AggFunc):
            return True if _wrapped_in_formatting(p) else False
        # Otherwise: anything that's not a bare column/rename and not
        # a bare aggregate counts as formatting.
        return True

    def _wrapped_in_formatting(p: exp.Expression) -> bool:
        """True if any ancestor of the aggregate inside ``p`` is a
        CAST/CASE/COALESCE/Func that would benefit from the agg-CTE
        split (so the wrapping stays at the outer SELECT)."""
        # Walk the expression: if we find a CAST/CASE/COALESCE/etc.
        # ABOVE any aggregate, return True.
        for agg in p.find_all(exp.AggFunc):
            cur = agg.parent
            while cur is not None and cur is not p:
                if isinstance(cur, (exp.Cast, exp.TryCast, exp.Case, exp.Coalesce, exp.Nullif)):
                    return True
                # Any non-aggregate function wrapping the aggregate also
                # counts (e.g., `ROUND(SUM(x), 2)`).
                if isinstance(cur, exp.Func) and not isinstance(cur, exp.AggFunc):
                    return True
                cur = cur.parent
        return False

    has_formatting = any(_is_formatting(p) for p in outer_projections)
    if not has_formatting:
        return sql, findings

    # Construct the aggregation CTE name. Prefer the existing
    # joined/filtered CTE's prefix (the agg CTE typically reads
    # from one of them, so naming should align). Falls back to
    # the FROM-table or entity hint.
    prefix = _cte_name_prefix(target, entity_hint)
    cte_name = f"{prefix}_aggregated"

    # Find every distinct aggregate expression in the outer
    # projections, give it a stable name, and remember the mapping.
    # The same aggregate expression appearing twice (e.g.,
    # ``CAST(SUM(x) AS ...)`` twice for different outer alias names)
    # should share ONE pre-aggregated column.
    agg_cache: Dict[str, Tuple[str, exp.Expression]] = {}
    # ^ key = canonical SQL of the aggregate expression
    # value = (column alias inside the agg CTE, original aggregate
    #          expression to put in the agg CTE projection list)

    def _stable_agg_alias(agg: exp.Expression, idx: int) -> str:
        # Try to derive a readable name from the aggregate's first
        # column argument + the aggregate's function name (suffix).
        cols = list(agg.find_all(exp.Column))
        fn_name = (agg.key or "agg").lower()  # 'sum', 'max', 'count', ...
        if cols:
            base = cols[0].name
            return f"{base}_{fn_name}"
        return f"agg_{idx}"

    def _agg_is_top_level(agg: exp.AggFunc, root: exp.Expression) -> bool:
        """True if ``agg`` is a true top-level aggregate (not inside a
        Window, Subquery, or nested Select scope under ``root``)."""
        cur = agg.parent
        while cur is not None and cur is not root:
            if isinstance(cur, exp.Window):
                return False
            if isinstance(cur, (exp.Subquery, exp.Select)) and cur is not root:
                return False
            cur = cur.parent
        return True

    agg_idx = 1

    def _intern_agg(agg: exp.Expression) -> str:
        """Look up or allocate a stable column alias for an aggregate
        expression. Side-effect: registers it in ``agg_cache``."""
        nonlocal agg_idx
        key = agg.sql()
        if key in agg_cache:
            return agg_cache[key][0]
        col_alias = _stable_agg_alias(agg, agg_idx)
        used = {a for (a, _) in agg_cache.values()}
        if col_alias in used:
            base = col_alias
            n = 2
            while f"{base}_{n}" in used:
                n += 1
            col_alias = f"{base}_{n}"
        agg_cache[key] = (col_alias, agg.copy())
        agg_idx += 1
        return col_alias

    new_outer_projections: List[exp.Expression] = []
    for proj in outer_projections:
        # Rewrite each top-level aggregate within the projection to a
        # reference to its pre-aggregated column. Aggregates inside
        # OVER (...) clauses or scalar subqueries are LEFT IN PLACE
        # because they aren't truly grouping aggregates.
        proj_copy = proj.copy()
        for agg in list(proj_copy.find_all(exp.AggFunc)):
            if not _agg_is_top_level(agg, proj_copy):
                continue
            agg.replace(exp.column(_intern_agg(agg)))
        new_outer_projections.append(proj_copy)

    # Build the aggregation CTE's projection list:
    #   1. Every non-aggregate, non-formatting outer projection
    #      (bare columns + renames) — these are implicit GROUP BY keys
    #      and pass through unchanged.
    #   2. Each cached aggregate, aliased to its stable name.
    agg_cte_projections: List[exp.Expression] = []
    seen_passthrough: Set[str] = set()
    for proj in outer_projections:
        if _expression_has_aggregate(proj):
            continue
        if _is_formatting(proj):
            # Formatting projections stay outer — but if they reference
            # a bare column from a source CTE, that column needs to
            # exist in the agg CTE output too. Expose it as a bare
            # column.
            for col in proj.find_all(exp.Column):
                key = col.sql()
                if key in seen_passthrough:
                    continue
                seen_passthrough.add(key)
                agg_cte_projections.append(col.copy())
            continue
        # Non-aggregate, non-formatting → pass through.
        key = proj.sql()
        if key in seen_passthrough:
            continue
        seen_passthrough.add(key)
        agg_cte_projections.append(proj.copy())

    for col_alias, agg_expr in agg_cache.values():
        agg_cte_projections.append(exp.alias_(agg_expr, col_alias))

    # The agg CTE's FROM/JOIN/WHERE/GROUP BY/HAVING are copied from
    # the outer SELECT.
    agg_select = exp.Select(expressions=agg_cte_projections)
    from_clause = target.args.get("from_") or target.args.get("from")
    if from_clause is not None:
        agg_select.set("from_", from_clause.copy())
    joins = target.args.get("joins")
    if joins:
        agg_select.set("joins", [j.copy() for j in joins])
    where = target.args.get("where")
    if where is not None:
        agg_select.set("where", where.copy())
    group = target.args.get("group")
    if group is not None:
        agg_select.set("group", group.copy())
    having = target.args.get("having")
    if having is not None:
        having_copy = having.copy()
        # Rewrite top-level aggregates in HAVING to reference the
        # cached column alias (so the HAVING reads from the agg
        # CTE's own projection list instead of recomputing).
        for agg in list(having_copy.find_all(exp.AggFunc)):
            if not _agg_is_top_level(agg, having_copy):
                continue
            agg.replace(exp.column(_intern_agg(agg)))
        agg_select.set("having", having_copy)

    # Build the new outer SELECT — replaces FROM with the agg CTE,
    # drops JOINs / WHERE / GROUP BY / HAVING (all moved up into
    # the CTE), and uses the rewritten outer projections.
    target.set("expressions", new_outer_projections)
    target.set("from_", exp.From(this=exp.Table(this=exp.to_identifier(cte_name))))
    target.set("joins", None)
    target.set("where", None)
    target.set("group", None)
    target.set("having", None)
    # Strip any remaining table qualifiers from the OUTER SELECT's
    # projection list, ORDER BY and QUALIFY only — at this level
    # columns come from a single source (the agg CTE) with no alias,
    # so leaving e.g., ``app.appraisal_amount_currency`` would be
    # invalid. Critically, don't walk into CTE bodies attached via
    # ``with_`` (they reference their own sources and aliases).
    for proj in target.expressions:
        for col in proj.find_all(exp.Column):
            col.set("table", None)
    order = target.args.get("order")
    if order is not None:
        # Rewrite top-level aggregates in ORDER BY to the agg-CTE
        # column alias (so e.g. ``ORDER BY SUM(amount) DESC`` reads as
        # ``ORDER BY amount_sum DESC`` against the agg CTE).
        for agg in list(order.find_all(exp.AggFunc)):
            if not _agg_is_top_level(agg, order):
                continue
            agg.replace(exp.column(_intern_agg(agg)))
        for col in order.find_all(exp.Column):
            col.set("table", None)
    qualify = target.args.get("qualify")
    if qualify is not None:
        for col in qualify.find_all(exp.Column):
            col.set("table", None)

    # Merge the new CTE into the WITH clause.
    new_cte = exp.CTE(
        this=agg_select,
        alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
    )
    existing_with = target.args.get("with_")
    if existing_with:
        existing_with.set(
            "expressions",
            list(existing_with.expressions) + [new_cte],
        )
    else:
        target.set("with_", exp.With(expressions=[new_cte], recursive=False))

    log.aggregation_cte_extracted = True

    try:
        rendered = root.sql(pretty=True)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


# -----------------------------------------------------------------------------
# UNION-branch extraction
# -----------------------------------------------------------------------------
#
# When the top-level statement is a ``UNION ALL`` (two or more
# branches), lift every branch into its own CTE so the top-level
# becomes a pure
#
#     SELECT * FROM <branch_1_cte>
#     UNION ALL
#     SELECT * FROM <branch_2_cte>
#     ...
#
# Nothing else lives in the union body. Each branch CTE holds its
# own per-source layering (sources, joins, aggregations, formatting)
# unchanged from however the previous passes shaped that branch.
#
# Scope:
#   - UNION ALL only. UNION (distinct), INTERSECT, EXCEPT are left
#     alone — their row-count semantics make wholesale lifting
#     surprising.
#   - Only fires when there are 2+ non-trivial branches. If every
#     branch is already ``SELECT * FROM <name>``, nothing to do.


def _walk_union_branches(node: exp.Expression) -> List[exp.Expression]:
    """Flatten a left-associative UNION tree into ``[branch1, branch2, ...]``.

    sqlglot represents ``A UNION ALL B UNION ALL C`` as
    ``Union(this=Union(this=A, expression=B), expression=C)``. This
    walker returns ``[A, B, C]`` in source order.

    Stops descending at the first non-Union node; the result always
    contains at least one element. UNION nodes with ``distinct=True``
    (i.e., set-distinct UNION) act as boundaries — we don't flatten
    across them.
    """
    out: List[exp.Expression] = []

    def _visit(n: exp.Expression) -> None:
        if (
            isinstance(n, exp.Union)
            and not n.args.get("distinct")
            and not n.args.get("by_name")
        ):
            _visit(n.this)
            _visit(n.expression)
        else:
            out.append(n)

    _visit(node)
    return out


def _infer_branch_name_from_literal(branch: exp.Expression) -> Optional[str]:
    """Look for a ``'literal' AS <something>`` projection in the
    branch and snake_case the literal as a candidate CTE name.

    The customer's UNION-of-aggregations files typically have a tag
    column like ``'Loan Loss Allowance' AS valuation_type`` so each
    branch is uniquely identifiable. Returns ``None`` when no
    suitable literal is found.
    """
    select = branch if isinstance(branch, exp.Select) else branch.find(exp.Select)
    if not isinstance(select, exp.Select):
        return None
    for proj in select.expressions or []:
        # ``'X' AS something`` → Alias whose ``.this`` is a Literal.
        if not isinstance(proj, exp.Alias):
            continue
        inner = proj.this
        if not isinstance(inner, exp.Literal):
            continue
        if not inner.is_string:
            continue
        value = inner.this  # the string content
        if not value:
            continue
        # Snake-case: lower, replace non-alphanum with _, collapse runs.
        slug_chars: List[str] = []
        prev_underscore = False
        for ch in value.lower():
            if ch.isalnum():
                slug_chars.append(ch)
                prev_underscore = False
            else:
                if not prev_underscore:
                    slug_chars.append("_")
                prev_underscore = True
        slug = "".join(slug_chars).strip("_")
        if slug and not slug[0].isdigit():
            return slug
    return None


def extract_union_branches_to_ctes(
    sql: str,
    findings: List[QualityFinding],
    entity_hint: str,
    log: "TransformLog",
) -> Tuple[str, List[QualityFinding]]:
    """Lift each top-level ``UNION ALL`` branch into its own CTE.

    Outer statement becomes a pure union of ``SELECT * FROM <cte>``
    references in source order. Existing CTEs at the top-level WITH
    are preserved; new branch CTEs are appended.

    Naming:
      - First try a string-literal projection in the branch (e.g.,
        ``'Loan Loss Allowance' AS valuation_type`` →
        ``loan_loss_allowance``).
      - Fall back to ``<entity>_<n>`` (1-based index).

    Skipped:
      - Statements that aren't a top-level ``UNION ALL`` (or with
        ``distinct=True`` — set-distinct UNION).
      - Branches that are already a bare ``SELECT * FROM <single_name>``
        (already lifted; replacing them with the same shape is noise).
        If EVERY branch is already lifted, the whole transform is a
        no-op.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql, findings
    if not statements or statements[0] is None:
        return sql, findings

    root = statements[0]
    # We operate on a top-level Union. The Union may be wrapped in a
    # Create (e.g., ``CREATE VIEW foo AS <union>``) or composed with a
    # higher-precedence set-op like EXCEPT / INTERSECT
    # (``... UNION ALL ... EXCEPT ...`` parses as
    # ``Except(this=Union(...), expression=Select)``). In all those
    # cases we still want to lift the Union's branches; the Except /
    # Create wrapper stays in place.
    container: Optional[exp.Expression] = None
    union_node: Optional[exp.Union] = None
    parent_setop: Optional[exp.Expression] = None
    parent_side: Optional[str] = None  # 'this' or 'expression'

    def _find_top_union(node: exp.Expression) -> Tuple[
        Optional[exp.Union],
        Optional[exp.Expression],
        Optional[str],
    ]:
        """Return (union_node, parent_setop, side) where ``side`` is
        'this' or 'expression' identifying which arg of ``parent_setop``
        the union is. Returns ``(None, None, None)`` when no top-level
        Union exists."""
        if isinstance(node, exp.Union) and not node.args.get("distinct") and not node.args.get("by_name"):
            return node, None, None
        if isinstance(node, (exp.Except, exp.Intersect)):
            # Recurse into both sides; return the first Union we find.
            for side in ("this", "expression"):
                child = node.args.get(side)
                if child is None:
                    continue
                if isinstance(child, exp.Union) and not child.args.get("distinct") and not child.args.get("by_name"):
                    return child, node, side
                # Recurse one level deeper.
                inner = _find_top_union(child)
                if inner[0] is not None:
                    return inner
        return None, None, None

    if isinstance(root, exp.Create):
        body = root.expression or root.this
        if isinstance(body, exp.Union) and not body.args.get("distinct") and not body.args.get("by_name"):
            container = root
            union_node = body
        else:
            union_node, parent_setop, parent_side = _find_top_union(body) if body else (None, None, None)
            if union_node is not None:
                container = root
    elif isinstance(root, exp.Union):
        union_node = root
    elif isinstance(root, (exp.Except, exp.Intersect)):
        union_node, parent_setop, parent_side = _find_top_union(root)
    else:
        return sql, findings

    if union_node is None:
        return sql, findings

    if union_node.args.get("distinct") or union_node.args.get("by_name"):
        return sql, findings

    branches = _walk_union_branches(union_node)
    if len(branches) < 2:
        return sql, findings

    def _is_already_lifted(b: exp.Expression) -> bool:
        """``SELECT * FROM <name>`` with no JOINs / WHERE / GROUP / etc."""
        if not isinstance(b, exp.Select):
            return False
        projections = b.expressions or []
        if len(projections) != 1 or not isinstance(projections[0], exp.Star):
            return False
        if (
            b.args.get("joins")
            or b.args.get("where")
            or b.args.get("group")
            or b.args.get("having")
            or b.args.get("qualify")
            or b.args.get("with_")
        ):
            return False
        from_clause = b.args.get("from_") or b.args.get("from")
        if from_clause is None:
            return False
        tables = list(from_clause.find_all(exp.Table))
        subqueries = list(from_clause.find_all(exp.Subquery))
        if len(tables) != 1 or subqueries:
            return False
        return True

    if all(_is_already_lifted(b) for b in branches):
        return sql, findings

    # Existing top-level WITH. Most cases: the WITH is on the union
    # itself. When the union is nested inside an EXCEPT / INTERSECT,
    # the WITH lives on that outer set-op instead.
    with_owner: exp.Expression = parent_setop if parent_setop is not None else union_node
    existing_with = with_owner.args.get("with_")
    used_names: Set[str] = set()
    if existing_with:
        for cte in existing_with.expressions:
            used_names.add(cte.alias_or_name)

    # Build a CTE per branch and remember the replacement Select.
    # For each non-trivial branch we ALSO run the layering passes
    # (pushdown → joined → filtered → aggregation) on the branch's
    # body so each branch becomes a fully-layered nested pipeline.
    # The branch CTE name doubles as the entity_hint for that
    # branch's internal layering, so its internal CTEs are named
    # ``<branch_name>_joined`` / ``<branch_name>_aggregated``, etc.
    new_ctes: List[exp.CTE] = []
    replacements: List[exp.Select] = []
    fallback_idx = 1
    for branch in branches:
        if _is_already_lifted(branch):
            # Already a ``SELECT * FROM <name>`` reference — keep it.
            replacements.append(branch)
            continue

        # Pick a name for this branch's CTE. Preferred order:
        #   1. FROM-table / CTE name of the branch (most concrete —
        #      ``FROM raw.customer`` → ``customer``;
        #      ``FROM web_orders_filtered`` → ``web_orders``).
        #   2. Literal tag in the branch's projection list
        #      (``'A' AS source_ind``) → snake-case the literal.
        #   3. ``<entity>_<n>`` fallback.
        from_name: Optional[str] = None
        if isinstance(branch, exp.Select):
            from_name = _from_table_name(branch)
            if from_name:
                # Strip a known layer suffix so the branch name lands
                # at the source level.
                for suffix in ("_prepared", "_filtered", "_joined", "_aggregated"):
                    if from_name.endswith(suffix):
                        from_name = from_name[: -len(suffix)]
                        break
        inferred = from_name or _infer_branch_name_from_literal(branch)
        base = inferred or f"{entity_hint}_{fallback_idx}"
        if not inferred:
            fallback_idx += 1
        cte_name = base
        counter = 2
        while cte_name in used_names:
            cte_name = f"{base}_{counter}"
            counter += 1
        used_names.add(cte_name)

        # Recursively apply the layering passes to the branch body
        # before wrapping it as a CTE. Each branch effectively becomes
        # its own mini-pipeline: source CTEs → joined → filtered →
        # aggregated → branch's outer SELECT. Failures fall back to
        # the un-layered branch.
        branch_sql_in = branch.sql()
        try:
            branch_sql_out = branch_sql_in
            branch_sql_out, _ = push_projections_to_source_ctes(
                branch_sql_out, []
            )
            branch_sql_out, _ = extract_joined_cte(
                branch_sql_out, [], cte_name, log
            )
            branch_sql_out, _ = extract_filtered_cte(
                branch_sql_out, [], cte_name, log
            )
            branch_sql_out, _ = extract_aggregation_cte(
                branch_sql_out, [], cte_name, log
            )
            branch_parsed = sqlglot.parse(branch_sql_out, read=None)
            if branch_parsed and branch_parsed[0] is not None:
                branch_body = branch_parsed[0]
            else:
                branch_body = branch.copy()
        except Exception:  # noqa: BLE001 — fail-soft, use original
            branch_body = branch.copy()

        # The branch can carry its own ``WITH`` clause. sqlglot lets a
        # nested Select have its own with_, but rendering one inside a
        # CTE body is valid SQL only when the dialect supports nested
        # WITH. Most dialects do (Snowflake / Databricks / Postgres).
        new_ctes.append(exp.CTE(
            this=branch_body,
            alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
        ))

        # Build the replacement: SELECT * FROM <cte_name>.
        replacement = exp.Select(expressions=[exp.Star()]).from_(
            exp.Table(this=exp.to_identifier(cte_name))
        )
        replacements.append(replacement)

    # Rebuild the union tree from the (possibly mixed lifted/already-
    # lifted) replacement list. Left-associative chain:
    #   ((R1 UNION ALL R2) UNION ALL R3) ...
    new_union: exp.Expression = replacements[0]
    for nxt in replacements[1:]:
        new_union = exp.Union(this=new_union, expression=nxt, distinct=False)

    # Decide where the new union sits in the AST and where the WITH
    # ends up. Three cases:
    #   (a) Standalone union (no Create wrapper, no Except parent).
    #       WITH and union root are the new statement root.
    #   (b) CREATE VIEW ... AS <union>.
    #       WITH and union both attach to the Create's expression.
    #   (c) Nested in EXCEPT/INTERSECT. WITH stays on the parent
    #       set-op; the union is just one operand.
    if parent_setop is not None:
        # Case (c): replace the union inside its parent. WITH stays
        # on the parent (we already used parent_setop as the
        # with_owner above).
        parent_setop.set(parent_side, new_union)
        if existing_with:
            existing_with.set(
                "expressions",
                list(existing_with.expressions) + new_ctes,
            )
        elif new_ctes:
            parent_setop.set(
                "with_",
                exp.With(expressions=new_ctes, recursive=False),
            )
        if container is not None:
            container.set("expression", parent_setop)
            statements[0] = container
        else:
            statements[0] = parent_setop
    else:
        # Cases (a) and (b): WITH attaches to the new union root.
        if existing_with:
            existing_with.set(
                "expressions",
                list(existing_with.expressions) + new_ctes,
            )
            new_union.set("with_", existing_with)
        elif new_ctes:
            new_union.set(
                "with_",
                exp.With(expressions=new_ctes, recursive=False),
            )
        if container is not None:
            container.set("expression", new_union)
            statements[0] = container
        else:
            statements[0] = new_union

    log.union_branches_extracted = True

    try:
        rendered = "\n".join(
            s.sql(pretty=True) for s in statements if s is not None
        )
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered, findings
    except Exception:  # noqa: BLE001 — fail-soft
        return sql, findings


# -----------------------------------------------------------------------------
# Customer rule-pack transforms
# -----------------------------------------------------------------------------


@dataclass
class TransformLog:
    """Records which transforms actually fired during ``apply_auto_fixes``.

    Used by ``emit_comment_header`` to populate the 8-section summary
    so the header reflects what changed, not a hard-coded list.
    """
    schema_replaced: bool = False
    subqueries_lifted: bool = False
    projections_pushed: bool = False
    obsolete_ctes_removed: bool = False
    legacy_date_replaced: bool = False
    where_true_removed: bool = False
    qualifier_rewritten: bool = False
    metadata_columns_stripped: bool = False
    aggregation_cte_extracted: bool = False
    joined_cte_extracted: bool = False
    filtered_cte_extracted: bool = False
    union_branches_extracted: bool = False
    distinct_rewritten: bool = False
    full_outer_rewritten: bool = False
    window_order_by_applied: bool = False
    schema_replacements: List[Tuple[str, str]] = field(default_factory=list)
    obsolete_cte_names: List[str] = field(default_factory=list)
    stripped_metadata_columns: List[str] = field(default_factory=list)


def apply_schema_replacement(
    sql: str,
    findings: List[QualityFinding],
    legacy_schemas: List[str],
    replacement: str,
    log: TransformLog,
) -> str:
    """Rule 1: replace legacy schemas (``bodm``, ``csz``, ...) with
    the configured replacement (default
    ``automatically_inferred_qualifier``).

    Operates on the parsed AST when possible. Mutates ``findings`` to
    mark matching ``LEGACY_SCHEMA`` findings as auto-fixed.
    """
    if not legacy_schemas:
        return sql
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql
    if not statements or statements[0] is None:
        return sql

    replaced = False
    legacy_set = set(legacy_schemas)
    seen_replacements: Set[Tuple[str, str]] = set()
    for stmt in statements:
        if stmt is None:
            continue
        for tbl in stmt.find_all(exp.Table):
            db = tbl.args.get("db")
            if db is not None and db.name in legacy_set:
                seen_replacements.add((db.name, replacement))
                tbl.set("db", exp.to_identifier(replacement))
                replaced = True

    if not replaced:
        return sql

    log.schema_replaced = True
    log.schema_replacements = sorted(seen_replacements)
    for f in findings:
        if f.rule == "LEGACY_SCHEMA":
            f.auto_fixed = True

    try:
        rendered = "\n".join(s.sql(pretty=True) for s in statements if s is not None)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered
    except Exception:  # noqa: BLE001 — fail-soft
        return sql


def apply_legacy_date_variable_replacement(
    sql: str,
    findings: List[QualityFinding],
    target_variable: str,
    log: TransformLog,
) -> str:
    """Rule 2.2: replace legacy ``{reporting_date}`` with
    ``{<target_variable>}`` (default ``{process_date}``).

    String-level substitution. Template variables aren't part of the
    parsed AST, so this runs textually.
    """
    legacy_variants = ("reporting_date",)
    out = sql
    replaced = False
    for legacy in legacy_variants:
        if legacy == target_variable:
            continue
        needle = "{" + legacy + "}"
        if needle in out:
            out = out.replace(needle, "{" + target_variable + "}")
            replaced = True
    if replaced:
        log.legacy_date_replaced = True
        for f in findings:
            if f.rule == "LEGACY_DATE_VARIABLE":
                f.auto_fixed = True
    return out


def remove_obsolete_ctes(
    sql: str,
    findings: List[QualityFinding],
    obsolete_names: List[str],
    log: TransformLog,
) -> str:
    """Rule 3: remove CTEs whose names match the customer's
    obsolete-timeline blacklist (``extract_dates``, ``create_timeline``,
    ``finalize_timeline``).

    Only fires when the CTE name matches. Conservative: if removing a
    CTE would leave a downstream reference dangling, the rewrite is
    skipped and a finding stays.
    """
    if not obsolete_names:
        return sql
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql
    if not statements or statements[0] is None:
        return sql

    blacklist = set(obsolete_names)
    removed_names: List[str] = []

    for stmt in statements:
        if stmt is None:
            continue
        # Find every Select / Query that has a WITH and drop matching CTEs.
        for node in list(stmt.find_all(exp.With)):
            keep: List[exp.CTE] = []
            for cte in node.expressions:
                if cte.alias_or_name in blacklist:
                    # Check whether the CTE is referenced elsewhere
                    # (not just in its own definition).
                    name = cte.alias_or_name
                    siblings = [c for c in node.expressions if c is not cte]
                    parent_query = node.parent
                    references = False
                    for sibling in siblings:
                        for tbl in sibling.find_all(exp.Table):
                            if tbl.name == name:
                                references = True
                                break
                        if references:
                            break
                    if not references and parent_query is not None:
                        for tbl in parent_query.find_all(exp.Table):
                            # Skip table refs inside the obsolete CTE itself.
                            ancestor = tbl.parent
                            inside_obsolete = False
                            while ancestor is not None:
                                if ancestor is cte:
                                    inside_obsolete = True
                                    break
                                ancestor = ancestor.parent
                            if not inside_obsolete and tbl.name == name:
                                references = True
                                break
                    if references:
                        # Conservative: keep the CTE rather than break the query.
                        keep.append(cte)
                    else:
                        removed_names.append(name)
                else:
                    keep.append(cte)
            if keep != list(node.expressions):
                if keep:
                    node.set("expressions", keep)
                else:
                    # Remove the entire WITH clause.
                    parent = node.parent
                    if parent is not None:
                        parent.set("with_", None)

    if not removed_names:
        return sql

    log.obsolete_ctes_removed = True
    log.obsolete_cte_names = sorted(set(removed_names))
    for f in findings:
        if f.rule == "OBSOLETE_CTE":
            f.auto_fixed = True

    try:
        rendered = "\n".join(s.sql(pretty=True) for s in statements if s is not None)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered
    except Exception:  # noqa: BLE001 — fail-soft
        return sql


def apply_table_qualifier(
    sql: str,
    qualifier: str,
    log: TransformLog,
    cte_names: Optional[Set[str]] = None,
) -> str:
    """Rewrite every base-table reference in the SQL to use the given
    qualifier in place of its original catalog/schema.

    ``catalog.schema.table`` -> ``<qualifier>.table``
    ``schema.table``         -> ``<qualifier>.table``
    Bare ``table`` references are left untouched (no qualifier added).

    CTE references are NOT rewritten — only base tables. Pass the
    set of CTE names defined in the same query so the rewrite skips
    them. When ``cte_names`` is None, the helper discovers them by
    walking the AST itself.
    """
    if not qualifier:
        return sql
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql
    if not statements or statements[0] is None:
        return sql

    rewritten = False
    for stmt in statements:
        if stmt is None:
            continue
        local_ctes = (
            cte_names
            if cte_names is not None
            else {c.alias_or_name for c in stmt.find_all(exp.CTE)}
        )
        for tbl in stmt.find_all(exp.Table):
            name = tbl.name
            if not name:
                continue
            db = tbl.args.get("db")
            catalog = tbl.args.get("catalog")
            # CTE references have no qualifier (they're local names).
            # Skip ONLY the unqualified ones whose name matches a CTE.
            # A qualified reference like ``raw.loans`` is a base-table
            # reference even when ``loans`` happens to also be a CTE
            # name elsewhere — it must get rewritten.
            if name in local_ctes and db is None and catalog is None:
                continue
            # Only rewrite when there's an existing qualifier to replace.
            if db is None and catalog is None:
                continue
            tbl.set("db", exp.to_identifier(qualifier))
            tbl.set("catalog", None)
            rewritten = True

    if not rewritten:
        return sql

    log.qualifier_rewritten = True

    try:
        rendered = "\n".join(s.sql(pretty=True) for s in statements if s is not None)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered
    except Exception:  # noqa: BLE001 — fail-soft
        return sql


def strip_metadata_columns(
    sql: str,
    findings: List[QualityFinding],
    metadata_blacklist: List[str],
    log: TransformLog,
) -> str:
    """Rule 4 / 13: remove blacklisted metadata columns from every
    SELECT's projections, from every WHERE predicate, AND from every
    JOIN ON predicate. If a WHERE / JOIN ON ends up empty, drop the
    whole clause.

    Conservative: only strips exact name matches (case-insensitive)
    on bare or qualified column references. Predicates that touch a
    metadata column on either operand are dropped wholesale —
    including ``prp.snapshot_date = fp.snapshot_date`` join keys.
    """
    if not metadata_blacklist:
        return sql
    try:
        statements = sqlglot.parse(sql, read=None)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return sql
    if not statements or statements[0] is None:
        return sql

    blacklist = {b.lower() for b in metadata_blacklist}
    stripped_columns: Set[str] = set()

    def _matches_metadata(node: exp.Expression) -> Optional[str]:
        """Return the metadata-column name if ``node`` is a (possibly
        qualified) Column reference to one. Else None."""
        col = node
        if isinstance(col, exp.Alias):
            col = col.this
        if isinstance(col, exp.Column) and col.name.lower() in blacklist:
            return col.name
        return None

    def _predicate_touches_metadata(pred: exp.Expression) -> Optional[str]:
        """Return the metadata column name if a leaf predicate
        references one (in either operand or via IS [NOT] NULL).
        Walks the whole predicate subtree so qualified refs on either
        side of an equality (``prp.snapshot_date = fp.snapshot_date``)
        are caught."""
        if isinstance(pred, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.In)):
            for col in pred.find_all(exp.Column):
                if col.name.lower() in blacklist:
                    return col.name
            return None
        if isinstance(pred, exp.Is):
            m = _matches_metadata(pred.this)
            if m:
                return m
        return None

    def _strip_predicate_tree(
        clause: Optional[exp.Expression],
    ) -> Tuple[Optional[exp.Expression], bool]:
        """Split an AND-tree into leaves, drop ones touching metadata,
        rebuild. Returns ``(new_tree_or_None, changed)``."""
        if clause is None:
            return None, False
        parts = _split_and(clause)
        kept_parts: List[exp.Expression] = []
        changed = False
        for p in parts:
            m = _predicate_touches_metadata(p)
            if m is not None:
                stripped_columns.add(m)
                changed = True
                continue
            kept_parts.append(p)
        if not changed:
            return clause, False
        return _rebuild_and(kept_parts), True

    for stmt in statements:
        if stmt is None:
            continue
        # 1. Strip metadata-column projections from every SELECT.
        for select in list(stmt.find_all(exp.Select)):
            kept_projections: List[exp.Expression] = []
            for proj in select.expressions:
                m = _matches_metadata(proj)
                if m is not None:
                    stripped_columns.add(m)
                    continue
                kept_projections.append(proj)
            if kept_projections != list(select.expressions):
                if not kept_projections:
                    # Don't produce a SELECT with zero projections —
                    # leave at least one column in place. Restore the
                    # last metadata projection so the SELECT stays valid.
                    kept_projections = [select.expressions[-1]]
                    stripped_columns.discard(
                        _matches_metadata(select.expressions[-1]) or ""
                    )
                select.set("expressions", kept_projections)

            # 2. Strip metadata-column predicates from WHERE.
            where = select.args.get("where")
            if where is not None and where.this is not None:
                new_where, changed = _strip_predicate_tree(where.this)
                if changed:
                    if new_where is None:
                        select.set("where", None)
                    else:
                        where.set("this", new_where)

            # 3. Strip metadata-column predicates from every JOIN ON.
            for join in select.args.get("joins") or []:
                on = join.args.get("on")
                new_on, changed = _strip_predicate_tree(on)
                if not changed:
                    continue
                if new_on is None:
                    # All ON predicates were metadata-only. Drop the
                    # ``on`` slot — note that this turns a regular
                    # join into a cartesian product, which is almost
                    # never what the analyst wanted. Emit a finding
                    # so it's visible, but apply the strip anyway
                    # since the rule unambiguously says "remove the
                    # metadata predicate".
                    join.set("on", None)
                    findings.append(QualityFinding(
                        rule="JOIN_ON_EMPTIED_BY_METADATA_STRIP",
                        severity="warning",
                        location="<join>",
                        message=(
                            "JOIN ON clause was emptied by metadata-column "
                            "stripping; verify the resulting cartesian join "
                            "is intentional or move the join key to a "
                            "non-metadata column"
                        ),
                    ))
                else:
                    join.set("on", new_on)

    if not stripped_columns:
        return sql

    log.metadata_columns_stripped = True
    log.stripped_metadata_columns = sorted(stripped_columns)
    for f in findings:
        if f.rule == "METADATA_COLUMN_EXPOSED":
            f.auto_fixed = True

    try:
        rendered = "\n".join(s.sql(pretty=True) for s in statements if s is not None)
        if sql.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        return rendered
    except Exception:  # noqa: BLE001 — fail-soft
        return sql


def emit_comment_header(
    pf: ParsedFile,
    log: TransformLog,
    output_filename: str,
) -> str:
    """Generate the customer-format comment header for the optimised
    SQL output (Rule 8). Each summary bullet and checklist entry is
    included only when the corresponding transform actually fired —
    so a file that didn't need schema replacement won't report it as
    applied.
    """
    summary: List[str] = []
    checklist: List[str] = []

    if log.schema_replaced:
        if log.schema_replacements:
            replacement_target = log.schema_replacements[0][1]
            sources = ", ".join(f"`{s}`" for s, _ in log.schema_replacements)
            summary.append(
                f"  - Replaced legacy schemas ({sources}) with `{replacement_target}`."
            )
        else:
            summary.append("  - Replaced legacy schemas with the new qualifier.")
        checklist.append("- [X] Schema replacement completed.")

    if log.legacy_date_replaced:
        summary.append("  - Replaced legacy `{reporting_date}` with `{process_date}`.")
        checklist.append("- [X] Date variable normalised.")

    if log.subqueries_lifted:
        summary.append("  - Lifted inline subqueries into named CTEs.")
        checklist.append("- [X] Subqueries encapsulated as CTEs.")

    if log.projections_pushed:
        summary.append(
            "  - Pushed single-table projections and filters into "
            "per-source `_filtered` / `_prepared` CTEs."
        )
        checklist.append("- [X] Modular CTE structure applied.")

    if log.joined_cte_extracted:
        summary.append(
            "  - Lifted JOINs and multi-source derivations into a "
            "dedicated `_joined` CTE; outer SELECT reads from a single-table FROM."
        )
        checklist.append("- [X] JOIN isolated into joined CTE.")

    if log.filtered_cte_extracted:
        summary.append(
            "  - Lifted cross-source WHERE predicates into a dedicated "
            "`_filtered` CTE so the joined CTE stays single-concern."
        )
        checklist.append("- [X] Cross-source filtering isolated.")

    if log.aggregation_cte_extracted:
        summary.append(
            "  - Lifted aggregates and GROUP BY into a dedicated "
            "`_aggregated` CTE; outer SELECT applies casting / defaulting only."
        )
        checklist.append("- [X] Aggregation isolated from formatting.")

    if log.union_branches_extracted:
        summary.append(
            "  - Lifted each `UNION ALL` branch into its own CTE; "
            "top-level statement is a pure `SELECT * FROM cte_a UNION ALL "
            "SELECT * FROM cte_b ...`."
        )
        checklist.append("- [X] UNION branches lifted to CTEs.")

    if log.distinct_rewritten:
        summary.append(
            "  - Replaced `SELECT DISTINCT` with explicit "
            "`ROW_NUMBER() OVER (PARTITION BY <projections>)` + "
            "`WHERE rn = 1` dedup pattern (`_ranked` + `_deduped` CTEs)."
        )
        checklist.append("- [X] DISTINCT replaced by explicit dedup CTEs.")

    if log.full_outer_rewritten:
        summary.append(
            "  - Replaced `FULL OUTER JOIN` + `COALESCE` with three "
            "explicit CTEs (`unique_rows_from_<a>`, "
            "`unique_rows_from_<b>`, `matching_rows`) UNION ALLed "
            "with a `source_ind` tag column."
        )
        checklist.append("- [X] FULL OUTER + COALESCE restructured into 3 CTEs.")

    if log.window_order_by_applied:
        summary.append(
            "  - Added `ORDER BY` (from `@mdde-order-by` annotation) "
            "to every window function lacking one, producing "
            "deterministic results."
        )
        checklist.append("- [X] Window functions made deterministic via `@mdde-order-by`.")

    if log.obsolete_ctes_removed:
        if log.obsolete_cte_names:
            obsolete = ", ".join(f"`{n}`" for n in log.obsolete_cte_names)
            summary.append(f"  - Removed obsolete CTEs ({obsolete}).")
        else:
            summary.append("  - Removed obsolete timeline-related CTEs.")
        checklist.append("- [X] Obsolete logic removed.")

    if log.metadata_columns_stripped:
        if log.stripped_metadata_columns:
            cols = ", ".join(f"`{c}`" for c in log.stripped_metadata_columns)
            summary.append(
                f"  - Excluded metadata columns from outputs and WHERE ({cols})."
            )
        else:
            summary.append("  - Excluded metadata columns from outputs and WHERE.")
        checklist.append("- [X] Metadata columns excluded.")

    if log.qualifier_rewritten:
        summary.append(
            "  - Rewrote table qualifiers (catalog/schema) to the target qualifier."
        )
        checklist.append("- [X] Table qualifiers normalised.")

    if log.where_true_removed:
        summary.append("  - Removed `WHERE 1=1` placeholder.")

    if not summary:
        # Nothing fired — skip the header entirely (the caller decides
        # not to prepend it).
        return ""

    lines: List[str] = []
    lines.append("/*")
    lines.append("Migration Details:")
    lines.append(f"- Original SQL File: {pf.path.name}")
    lines.append(f"- Target SQL File:  {output_filename}")
    lines.append("- Summary of Changes:")
    lines.extend(summary)
    if checklist:
        lines.append("")
        lines.append("Validation Checklist:")
        lines.extend(checklist)
    lines.append("*/")
    return "\n".join(lines) + "\n"


# Profile timings — populated when ``--profile`` is in effect. Keyed
# by transform name → cumulative seconds across all files in the run.
# Reset per ``process_folder()`` invocation.
_PROFILE_TIMINGS: Dict[str, float] = {}
_PROFILE_ENABLED: bool = False


def _reset_profile() -> None:
    """Reset the global timings + enable flag."""
    _PROFILE_TIMINGS.clear()


def _profile_record(name: str, elapsed: float) -> None:
    """Add ``elapsed`` seconds to the cumulative bucket for ``name``."""
    if not _PROFILE_ENABLED:
        return
    _PROFILE_TIMINGS[name] = _PROFILE_TIMINGS.get(name, 0.0) + elapsed


class _ProfileBlock:
    """Context manager that records elapsed time under a transform name."""
    __slots__ = ("_name", "_t0")

    def __init__(self, name: str) -> None:
        self._name = name
        self._t0 = 0.0

    def __enter__(self) -> "_ProfileBlock":
        if _PROFILE_ENABLED:
            self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        if _PROFILE_ENABLED:
            _profile_record(self._name, time.perf_counter() - self._t0)


def apply_auto_fixes(
    sql: str,
    findings: List[QualityFinding],
    customer_config: Optional["CustomerRuleConfig"] = None,
    optimize_config: Optional["OptimizeConfig"] = None,
    detection_only: bool = False,
    entity_hint: str = "result",
    order_by_annotation: Optional[str] = None,
) -> Tuple[str, List[QualityFinding], TransformLog]:
    """Rewrite the SQL to fix the safe issues. Mutates findings in
    place, marking the auto-fixed ones. Returns the rewritten SQL,
    the (mutated) findings list, and a ``TransformLog`` recording
    which transforms actually fired.

    When ``detection_only`` is True, every auto-fix transform is
    skipped — the function only format-normalises the input. The
    findings list is still computed and returned. Used as a safety
    fallback when the auto-fix transforms misbehave on real customer
    SQL: drop to detection-only mode and ship the linter findings.
    """
    out = sql
    log = TransformLog()
    cfg = customer_config or CustomerRuleConfig()
    opt = optimize_config or OptimizeConfig()

    if not detection_only:
        rules_enabled = opt.enabled if opt and opt.enabled else {}

        def rule_on(key: str) -> bool:
            """True if a rule is enabled (defaults to True for keys
            not present in the toggle map)."""
            return rules_enabled.get(key, True)

        if rule_on("where_true_removed"):
            for f in findings:
                if f.rule == "WHERE_1_EQUALS_1":
                    new = _AUTOFIX_WHERE_TRUE.sub("WHERE ", out)
                    new = _AUTOFIX_LEADING_WHERE_TRUE.sub("\n", new)
                    if new != out:
                        out = new
                        f.auto_fixed = True
                        log.where_true_removed = True

        # Customer rule 1: legacy schema replacement (opt-in via config).
        if rule_on("schema_replacement") and cfg.legacy_schemas:
            out = apply_schema_replacement(
                out, findings, cfg.legacy_schemas, cfg.replacement_schema, log,
            )

        # Customer rule 2.2: legacy date-variable normalisation.
        if rule_on("legacy_date_replacement"):
            out = apply_legacy_date_variable_replacement(
                out, findings, cfg.date_variable, log,
            )

        # Customer rule 3: obsolete-CTE removal.
        if rule_on("obsolete_cte_removal"):
            out = remove_obsolete_ctes(out, findings, cfg.obsolete_cte_names, log)

        # Customer rule 4 / 13: strip metadata columns from SELECT
        # projections AND from WHERE predicates.
        if rule_on("metadata_column_strip"):
            out = strip_metadata_columns(out, findings, cfg.metadata_blacklist, log)

        # Lift inline subqueries into named CTEs (in-scope shapes only).
        # Done before format-normalisation so the final pretty-print covers
        # the rewritten AST in one pass.
        if rule_on("subquery_lift"):
            before = out
            out, findings = lift_subqueries_to_ctes(out, findings)
            if out != before:
                log.subqueries_lifted = True

        # Rewrite ``FULL OUTER JOIN`` + ``COALESCE`` into three
        # explicit CTEs + UNION ALL with `source_ind` tag.
        # Runs AFTER subquery lifting (so derived-table shapes
        # settle) and BEFORE DISTINCT/pushdown so the resulting
        # three CTEs can themselves be layered.
        if rule_on("full_outer_rewrite"):
            before = out
            out, findings = rewrite_full_outer_with_coalesce(
                out, findings, log,
            )
            if out != before:
                log.full_outer_rewritten = True

        # Add ORDER BY to every window function lacking one, using
        # the file's ``@mdde-order-by`` annotation. No-op when the
        # annotation isn't present.
        if rule_on("window_order_by"):
            before = out
            out, findings = apply_window_order_by(
                out, findings, order_by_annotation, log,
            )
            if out != before:
                log.window_order_by_applied = True

        # Replace ``SELECT DISTINCT`` with an explicit two-CTE dedup
        # pattern (``<entity>_ranked`` + ``<entity>_deduped``).
        # Runs AFTER subquery lifting (so the subquery shapes settle
        # before we touch DISTINCT) and BEFORE source pushdown so the
        # downstream layering passes see a deduped CTE as their FROM
        # rather than a DISTINCT-flagged SELECT.
        if rule_on("distinct_rewrite"):
            before = out
            out, findings = replace_distinct_with_rownum(
                out, findings, entity_hint, log,
            )
            if out != before:
                log.distinct_rewritten = True

        # Push single-table projections and filters into per-source CTEs.
        # Runs after the subquery lift so any derived tables that became
        # CTEs are correctly excluded from pushdown targets.
        if rule_on("source_pushdown"):
            before = out
            out, findings = push_projections_to_source_ctes(
                out, findings,
                metadata_blacklist=(
                    cfg.metadata_blacklist
                    if rule_on("metadata_column_strip")
                    else None
                ),
            )
            if out != before:
                log.projections_pushed = True

        # Re-run the metadata-strip AFTER pushdown so any metadata
        # columns that the source-CTE pushdown re-exposed (via the
        # "outer references" walk over the AST) get removed from
        # the source CTEs too. The first strip ran on the raw outer
        # SELECT; the second cleans up after pushdown.
        if rule_on("metadata_column_strip"):
            out = strip_metadata_columns(
                out, findings, cfg.metadata_blacklist, log,
            )

        # Lift JOINs + non-cast single-source derivations into a
        # dedicated ``<entity>_joined`` CTE. Runs BEFORE aggregation
        # extraction so the agg CTE just groups over a single-table
        # input.
        if rule_on("joined_cte"):
            before = out
            out, findings = extract_joined_cte(out, findings, entity_hint, log)
            if out != before:
                log.joined_cte_extracted = True

        # Lift the joined CTE's WHERE into a dedicated
        # ``<entity>_filtered`` CTE so each layer stays single-concern.
        # Runs between joined and aggregation passes.
        if rule_on("filtered_cte"):
            before = out
            out, findings = extract_filtered_cte(out, findings, entity_hint, log)
            if out != before:
                log.filtered_cte_extracted = True

        # Lift aggregates + GROUP BY into a dedicated ``<entity>_aggregated``
        # CTE so the outer SELECT only applies casting / defaulting.
        # Runs AFTER projection pushdown + joined-CTE extraction.
        if rule_on("aggregation_cte"):
            before = out
            out, findings = extract_aggregation_cte(out, findings, entity_hint, log)
            if out != before:
                log.aggregation_cte_extracted = True

        # Lift each top-level ``UNION ALL`` branch into its own CTE so
        # the top-level statement is a pure
        # ``SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b ...``.
        # Runs after the per-branch transforms.
        if rule_on("union_branch_lift"):
            before = out
            out, findings = extract_union_branches_to_ctes(
                out, findings, entity_hint, log
            )
            if out != before:
                log.union_branches_extracted = True

        # Customer rule 7 (qualifier rewrite): replace catalog/schema
        # on every base-table reference with the configured qualifier.
        # Runs last so it picks up tables introduced by earlier
        # transforms (e.g., schema-replacement).
        if rule_on("table_qualifier_rewrite") and opt.table_qualifier:
            out = apply_table_qualifier(out, opt.table_qualifier, log)

    # Format-normalise via sqlglot. Preserves semantics; produces
    # consistent indentation across all output files. Runs even in
    # detection-only mode so the output isn't byte-identical to the
    # input (analysts can still see something happened).
    #
    # We parse-then-render (rather than transpile) so we can strip
    # large multi-line block comments that sqlglot collapses inline
    # on render. These are typically the customer's
    # ``SQL Query Conversion Summary`` banners which the script
    # replaces with its own Migration Details header in
    # ``emit_comment_header``.
    try:
        parsed_out = sqlglot.parse(out, read=None)
        for stmt in parsed_out:
            if stmt is None:
                continue
            _strip_banner_comments(stmt)
        rendered = "\n".join(s.sql(pretty=True) for s in parsed_out if s is not None)
        # Re-attach trailing semicolon if the original had one.
        if out.rstrip().endswith(";") and not rendered.rstrip().endswith(";"):
            rendered = rendered.rstrip() + ";"
        out = rendered
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        pass

    # Annotate every CTE in the rendered output with a one-line
    # functional-role comment derived from its name suffix. Operates
    # textually on the pretty-printed SQL because sqlglot's renderer
    # doesn't preserve free-form comments between CTEs reliably.
    if not detection_only:
        out = _annotate_ctes_with_roles(out, entity_hint)

    # Split run-on banner comments — sqlglot's pretty printer
    # concatenates multiple ``/* ... */`` block comments attached to
    # the same AST node onto a single long line. Break them onto
    # individual lines so the output stays readable.
    out = _split_runon_block_comments(out)

    return out, findings, log


# Pattern matches `*/ <whitespace not including newline> /*` on the
# same line. We split that into ``*/`` + newline + indent + ``/*``.
_RUNON_BLOCK_COMMENT_RE = re.compile(r"\*/[ \t]+/\*")


def _split_runon_block_comments(sql: str) -> str:
    """Break run-on `/* a */ /* b */ /* c */` chains into one
    block-comment per line. Preserves the indent of whichever line
    the chain started on, so the resulting comments stay vertically
    aligned. Fail-soft on errors."""
    if not sql:
        return sql
    try:
        out_lines: List[str] = []
        for line in sql.split("\n"):
            if "*/" not in line or "/*" not in line:
                out_lines.append(line)
                continue
            # Capture the line's leading whitespace.
            stripped = line.lstrip()
            indent = line[: len(line) - len(stripped)]
            # Split on the run-on pattern.
            split = _RUNON_BLOCK_COMMENT_RE.split(line)
            if len(split) == 1:
                out_lines.append(line)
                continue
            # First piece keeps the original indent + ``*/``;
            # subsequent pieces get the same indent + ``/*`` prefix.
            out_lines.append(split[0] + "*/")
            for piece in split[1:-1]:
                out_lines.append(f"{indent}/*{piece}*/")
            out_lines.append(f"{indent}/*{split[-1]}")
        return "\n".join(out_lines)
    except Exception:  # noqa: BLE001 — fail-soft
        return sql


# Map from CTE-name suffix to the one-line role comment that
# describes that CTE's single concern. Order matters: longer suffixes
# checked first.
_CTE_ROLE_BY_SUFFIX: List[Tuple[str, str]] = [
    ("_aggregated", "Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)"),
    ("_deduped",    "Deduped: filters WHERE rn = 1 (removes duplicates surfaced by the ranked CTE above)"),
    ("_ranked",     "Ranked: ROW_NUMBER() OVER (PARTITION BY all projection columns) — replaces SELECT DISTINCT, makes duplicates inspectable"),
    ("_joined",     "Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)"),
    ("_filtered",   "Source filter: single-table SELECT + WHERE for one source"),
    ("_prepared",   "Source prep: single-table SELECT + renames + single-source value transforms"),
]


def _cte_role_comment(cte_name: str, entity_hint: str) -> Optional[str]:
    """Return the one-line role comment for a CTE name, or ``None``
    when no rule matches.

    Disambiguates entity-level ``<entity>_filtered`` (cross-source
    filter CTE) from source-level ``<table>_filtered`` (single-source
    pushdown CTE) by checking whether the prefix matches the
    file's entity hint.
    """
    if entity_hint and cte_name == f"{entity_hint}_filtered":
        return "Filtered: cross-source WHERE predicates (no JOIN, no derivation, no aggregation)"
    for suffix, role in _CTE_ROLE_BY_SUFFIX:
        if cte_name.endswith(suffix):
            return role
    # UNION-branch CTEs: ``<entity>_<n>`` numeric suffix or a literal
    # tag like ``loan_loss_allowance`` / ``web`` / ``store``.
    if entity_hint:
        # Pattern: <entity>_<digit>+
        m = re.match(rf"^{re.escape(entity_hint)}_(\d+)$", cte_name)
        if m:
            return f"Branch CTE: UNION ALL branch #{m.group(1)} (fully layered internally)"
    # Other CTEs not matching a known suffix: probably a user-defined
    # CTE — don't comment.
    return None


# Lazy import to keep the regex compile out of module-level cost when
# the comment pass is disabled. The pattern matches lines like
# ``WITH foo AS (`` or ``), foo AS (`` at any indentation.
_CTE_DECL_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<prefix>(?:WITH\s+|\)\s*,\s*))(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s+AS\s+\(",
    re.MULTILINE,
)


def _annotate_ctes_with_roles(sql: str, entity_hint: str) -> str:
    """Insert a one-line ``-- Role: ...`` comment above every CTE
    declaration in the rendered SQL. Comments are derived from each
    CTE's name suffix (see ``_CTE_ROLE_BY_SUFFIX``).

    The pass is text-level (regex-based) because sqlglot's pretty
    printer doesn't preserve free-form comments between CTEs reliably.
    Fail-soft: any error returns the SQL unchanged.
    """
    if not sql:
        return sql
    try:
        def _annotate(match: re.Match) -> str:
            indent = match.group("indent")
            prefix = match.group("prefix")
            name = match.group("name")
            role = _cte_role_comment(name, entity_hint)
            if role is None:
                return match.group(0)
            # ``WITH foo AS (`` keeps WITH on its own preceding line
            # of indent — emit the comment on the line before WITH.
            # ``), foo AS (`` emits the comment on its own line
            # between the previous ``)`` and the comma.
            if prefix.strip().startswith("WITH"):
                # Place comment ABOVE the WITH keyword. sqlglot
                # always emits WITH at column 0, so prepend.
                return f"{indent}-- {role}\n{match.group(0)}"
            # ``), foo AS (`` — comment goes between the previous
            # ``)`` and this declaration. Split: keep the `)`, then
            # newline, then comment, then `, foo AS (`.
            return f")\n{indent}-- {role}\n{indent}, {name} AS ("
        return _CTE_DECL_RE.sub(_annotate, sql)
    except Exception:  # noqa: BLE001 — fail-soft
        return sql


# Heuristic markers for "banner" comments: multi-line block comments
# whose body contains the customer's conversion-summary patterns
# (``================`` rules, ``Conversion Summary`` text, etc.).
# When we see one attached to an AST node, we drop it during render
# so it doesn't collapse onto a single line.
_BANNER_PATTERNS = (
    "================",  # ASCII rule lines
    "----------------",
    "Conversion Summary",
    "Migration Details",
    "Source File",
    "Target SQL",
)


def _comment_is_banner(text: str) -> bool:
    """True when a block-comment body matches a 'header banner' shape
    we want to drop. Multi-line comments containing any of the
    well-known banner patterns count, plus single-line comments that
    look like attribute tags (``Table: ...``, ``Generated: ...``,
    ``XSD Version: ...``)."""
    stripped = text.strip()
    if "\n" in text and any(p in text for p in _BANNER_PATTERNS):
        return True
    # Single-line metadata-like comments (`<key>: <value>`) that look
    # like extractor-tool output, not user-authored documentation.
    if "\n" not in stripped and ":" in stripped:
        head = stripped.split(":", 1)[0].strip()
        # Whitelist of header keys that the extractor tool emits.
        head_lower = head.lower()
        for marker in ("table", "generated", "source file", "ddm version",
                       "dda version", "xsd version", "xsd vers"):
            if head_lower.startswith(marker):
                return True
    return False


def _strip_banner_comments(node: exp.Expression) -> None:
    """Walk ``node`` and drop ``.comments`` that match the banner
    heuristic. Keeps annotation-style comments (``@pk``, ``@pii``,
    etc.) intact — those don't match the banner patterns."""
    for sub in node.walk():
        comments = sub.comments
        if not comments:
            continue
        kept = [c for c in comments if not _comment_is_banner(c)]
        if len(kept) != len(comments):
            sub.comments = kept or None


# =============================================================================
# 4. MAPPING EMITTERS (BFM + CTE-notebook shapes)
# =============================================================================


def emit_bfm_mapping(pf: ParsedFile) -> Dict[str, Any]:
    """Business-Friendly Mapping shape — entity-level, target ← sources."""
    mapping: Dict[str, Any] = {
        "target_entity": pf.entity_name,
        "target_layer": pf.annotations.entity.get("layer", "unknown"),
        "target_stereotype": pf.annotations.entity.get("stereotype", ""),
        "description": pf.annotations.entity.get("description", ""),
        "source_entities": pf.source_tables,
        "attributes": [],
    }

    for lin in pf.lineage:
        if lin.output_column == "*":
            continue
        ann = pf.annotations.columns.get(lin.output_column, ColumnAnnotations())
        attr_entry: Dict[str, Any] = {
            "target": lin.output_column,
            "derivation": lin.derivation,
            "expression": lin.expression,
            "sources": [
                {"entity": tbl or "<inherited>", "attribute": col}
                for tbl, col in lin.source_columns
            ],
        }
        if ann.flags:
            attr_entry["tags"] = sorted(ann.flags)
        if ann.fk_target:
            attr_entry["fk"] = ann.fk_target
        if ann.decimal:
            attr_entry["decimal"] = list(ann.decimal)
        mapping["attributes"].append(attr_entry)

    return mapping


def emit_cte_mapping(pf: ParsedFile) -> Dict[str, Any]:
    """CTE-notebook mapping shape — pipeline-level, contract-aware."""
    mapping: Dict[str, Any] = {
        "target": pf.entity_name,
        "version": "1.0.0",
        "layer": pf.annotations.entity.get("layer", "unknown"),
        "stereotype": pf.annotations.entity.get("stereotype", ""),
        "ctes": pf.cte_names,
        "sources": [
            {"name": s, "type": "table"} for s in pf.source_tables
        ],
        "outputs": [
            {
                "name": lin.output_column,
                "from_sources": [
                    f"{tbl or '<inherited>'}.{col}"
                    for tbl, col in lin.source_columns
                ],
                "derivation": lin.derivation,
            }
            for lin in pf.lineage if lin.output_column != "*"
        ],
        "contract": {
            "owner": "<unknown>",
            "freshness_minutes": 60,
            "primary_key": [
                col for col, ann in pf.annotations.columns.items()
                if "pk" in ann.flags
            ],
        },
    }
    return mapping


def emit_entity_yaml(pf: ParsedFile) -> Dict[str, Any]:
    """SQL-First entity YAML (matching what `sql_first sync` produces)."""
    entity = {
        "entity": {
            "name": pf.entity_name,
            "layer": pf.annotations.entity.get("layer", "unknown"),
            "stereotype": pf.annotations.entity.get("stereotype", ""),
            "description": pf.annotations.entity.get("description", ""),
            "domain": pf.annotations.entity.get("domain", ""),
        },
        "attributes": [],
    }
    for lin in pf.lineage:
        if lin.output_column == "*":
            continue
        ann = pf.annotations.columns.get(lin.output_column, ColumnAnnotations())
        attr: Dict[str, Any] = {
            "name": lin.output_column,
            "is_primary_key": "pk" in ann.flags,
        }
        if "business_key" in ann.flags:
            attr["is_business_key"] = True
        if "pii" in ann.flags:
            attr["pii"] = True
        if "nullable" in ann.flags:
            attr["nullable"] = True
        if "derived" in ann.flags:
            attr["derived"] = True
        if ann.fk_target:
            attr["fk"] = ann.fk_target
        if ann.decimal:
            attr["decimal"] = list(ann.decimal)
        entity["attributes"].append(attr)
    return entity


# =============================================================================
# 4b. GENIE PROMPT EMITTER
# =============================================================================
#
# Derives a tight natural-language description of the ORIGINAL query so
# a user can paste it into Databricks Genie (or any LLM-backed SQL
# generator) and get an equivalent result against the source tables
# directly. The prompt covers only what Genie needs to act:
#   - source tables
#   - join conditions (when not trivially equi-join on a single column)
#   - filters
#   - aggregations / windows
#   - output columns (with renames + derivations spelled out)
#   - sort order
# It deliberately omits boilerplate, schema dumps, and commentary.


def _unquoted_sql(node: exp.Expression) -> str:
    """Render an AST node back to SQL with identifier quoting removed.

    sqlglot's ``qualify()`` pass marks every Identifier as ``quoted=True``
    so the resulting SQL is portable across dialects. For Genie prose
    that's unhelpful — we render to unquoted identifiers for
    readability. Operates on a copy so the original AST is untouched.
    """
    clone = node.copy()
    for ident in clone.find_all(exp.Identifier):
        ident.set("quoted", False)
    return clone.sql()


def _column_label(col: exp.Column) -> str:
    """Render `c.email` -> `c.email` (unquoted)."""
    if col.table:
        return f"{col.table}.{col.name}"
    return col.name


def _humanise_expression(node: exp.Expression) -> str:
    """Best-effort SQL→prose for one expression. Falls back to the SQL
    fragment when no specific shape matches, since that's still
    actionable for Genie."""
    if isinstance(node, exp.Alias):
        return _humanise_expression(node.this)
    if isinstance(node, exp.Column):
        return _column_label(node)
    if isinstance(node, exp.AggFunc):
        inner = _unquoted_sql(node.this) if node.this is not None else "*"
        name = type(node).__name__.upper()
        return f"{name}({inner})"
    if isinstance(node, exp.Case):
        return f"CASE expression: {_unquoted_sql(node)}"
    return _unquoted_sql(node)


def _humanise_predicate(node: exp.Expression) -> str:
    """One leaf predicate as a short clause."""
    return _unquoted_sql(node)


def emit_genie_prompt(
    pf: ParsedFile,
    customer_config: Optional["CustomerRuleConfig"] = None,
    optimize_config: Optional["OptimizeConfig"] = None,
) -> str:
    """Build a Genie instruction block the user can paste **above**
    the original query in a Genie prompt window.

    The output is NOT a description of the query — it's a set of
    instructions Genie should apply to optimise/migrate that query.
    Contains the customer's rule pack distilled to actionable
    guidance, scoped only to what's relevant for the file at hand.
    """
    cfg = customer_config or CustomerRuleConfig()
    opt = optimize_config or OptimizeConfig()
    rules_enabled = opt.enabled if opt and opt.enabled else {}

    def rule_on(key: str) -> bool:
        return rules_enabled.get(key, True)

    lines: List[str] = []
    lines.append("# Genie instructions")
    lines.append("")
    lines.append(
        "Apply the following SQL migration and optimisation rules to "
        "the query below. Produce a single rewritten query plus a "
        "short summary of what changed."
    )
    lines.append("")

    # Workflow ordering matches CUSTOMER_RULES.md.
    n = 0

    # 1. Schema replacement (only when configured AND enabled).
    if rule_on("schema_replacement") and cfg.legacy_schemas:
        n += 1
        legacy = ", ".join(f"`{s}`" for s in cfg.legacy_schemas)
        lines.append(
            f"{n}. **Schema Replacement** — replace legacy schemas "
            f"({legacy}) with `{cfg.replacement_schema}` on every "
            f"table reference."
        )

    # 2. SCD2 filtering (controlled by `legacy_date_replacement` flag).
    if rule_on("legacy_date_replacement"):
        n += 1
        lines.append(
            f"{n}. **SCD2 Filtering** — every source table CTE must "
            f"apply point-in-time filtering:"
        )
        lines.append("   ```sql")
        lines.append(f"   WHERE CAST('{{{cfg.date_variable}}}' AS DATE) >= _valid_from")
        lines.append(
            f"     AND CAST('{{{cfg.date_variable}}}' AS DATE) <  "
            f"COALESCE(_valid_to, CAST('9999-12-31' AS DATE))"
        )
        lines.append("   ```")
        lines.append(
            "   Use the variable above; do **not** use `BETWEEN`. "
            "Replace any legacy `{reporting_date}` references with "
            f"`{{{cfg.date_variable}}}`."
        )

    # 3. Remove obsolete logic.
    if rule_on("obsolete_cte_removal") and cfg.obsolete_cte_names:
        n += 1
        obs = ", ".join(f"`{c}`" for c in cfg.obsolete_cte_names)
        lines.append(
            f"{n}. **Remove Obsolete Logic** — delete CTEs named "
            f"{obs} and update downstream references. Replace any "
            f"`snapshot_date` filtering with the SCD2 predicate above."
        )

    # 4. Exclude metadata columns.
    if rule_on("metadata_column_strip") and cfg.metadata_blacklist:
        n += 1
        meta = ", ".join(f"`{c}`" for c in cfg.metadata_blacklist)
        lines.append(
            f"{n}. **Exclude Metadata Columns** — never project "
            f"{meta} in CTE outputs or the final SELECT. Drop any "
            "WHERE predicate that references these columns; if the "
            "WHERE becomes empty, remove it. Retain `_valid_from` / "
            "`_valid_to` ONLY in the WHERE clause of initial source "
            "CTEs."
        )

    # 5. Joins.
    n += 1
    lines.append(
        f"{n}. **Optimize Joins** — remove `LEFT JOIN`s that "
        "contribute no columns to the final result. Pre-process any "
        "join-key transformations in source CTEs (no inline `CAST`, "
        "function call, or literal inside JOIN `ON` clauses)."
    )

    # 6. Explicit column selection.
    n += 1
    lines.append(
        f"{n}. **Explicit Column Selection** — replace every "
        "`SELECT *` with an explicit column list. Only project the "
        "columns the downstream consumer needs."
    )

    # 7. Layered CTE structure — the 5-layer model.
    # Show this section only when ANY of the layering rules is enabled.
    layering_rules = ("source_pushdown", "joined_cte", "filtered_cte", "aggregation_cte")
    if any(rule_on(k) for k in layering_rules):
        n += 1
        lines.append(
            f"{n}. **Layered CTE Structure** — every query should fan out "
            "into single-concern CTEs. Each CTE answers one question:"
        )
        if rule_on("source_pushdown"):
            lines.append(
                "   - **Source layer** — `<table>_prepared` or "
                "`<table>_filtered`. Owns bare columns, renames, single-source "
                "value transforms (UPPER, TRIM, arithmetic), and single-source "
                "WHERE filters. One CTE per source table referenced."
            )
        if rule_on("joined_cte"):
            lines.append(
                "   - **Joined layer** — `<entity>_joined`. Owns JOINs and "
                "multi-source derivations (column expressions that depend on "
                "more than one source). NO WHERE clause. NO aggregation."
            )
        if rule_on("filtered_cte"):
            lines.append(
                "   - **Filtered layer** — `<entity>_filtered`. Owns "
                "cross-source WHERE predicates (filters whose operands come "
                "from multiple source CTEs). Emitted ONLY when such "
                "predicates exist. NO joins, NO derivations, NO aggregation."
            )
        if rule_on("aggregation_cte"):
            lines.append(
                "   - **Aggregated layer** — `<entity>_aggregated`. Owns "
                "`GROUP BY`, aggregate functions (`SUM`, `MAX`, `COUNT`, ...), "
                "and `HAVING`. Reads from the filtered or joined CTE. NO "
                "JOINs of its own, NO derivations, NO formatting."
            )
        lines.append(
            "   - **Final SELECT** (no CTE — top level). Owns "
            "`CAST`, `COALESCE`, `NULLIF`, `CASE` with defaults, "
            "constants/literals, `ORDER BY`, `LIMIT`, and window functions "
            "(`ROW_NUMBER`, `LAG`, `SUM OVER PARTITION BY`). Reads from "
            "the aggregated/filtered/joined CTE. NO joins of its own, NO "
            "WHERE, NO GROUP BY."
        )

    # 8. UNION ALL purity + source tagging.
    if rule_on("union_branch_lift"):
        n += 1
        lines.append(
            f"{n}. **UNION ALL Purity** — when combining datasets, lift "
            "each branch into its own CTE first. The top-level body must "
            "be a pure `SELECT * FROM <branch_cte_a> UNION ALL SELECT * "
            "FROM <branch_cte_b> ...` with no casts, no filters, no "
            "expressions. Each branch must add a string-literal tag "
            "column identifying the source "
            "(e.g., `'A_SOURCE' AS source_ind`), and each branch's CTE "
            "body must itself be fully layered using the rules above."
        )

    # 9. Predicate-subquery → CTE (controlled by subquery_lift toggle).
    if rule_on("subquery_lift"):
        n += 1
        lines.append(
            f"{n}. **Predicate Subquery Lift** — `WHERE x IN (SELECT ...)` "
            "and `WHERE EXISTS (SELECT ...)` must have their inner SELECT "
            "lifted into a dedicated CTE; the predicate becomes `WHERE x "
            "IN (SELECT col FROM <cte>)` or `WHERE EXISTS (SELECT 1 FROM "
            "<cte> WHERE <cte>.x = outer.x)`. For correlated cases, "
            "promote the correlation column as a projection in the lifted "
            "CTE; non-correlation predicates stay inside the CTE."
        )

        # 10. EXCEPT / INTERSECT branch lift (shares the subquery_lift toggle).
        n += 1
        lines.append(
            f"{n}. **EXCEPT / INTERSECT Lift** — top-level "
            "`EXCEPT` / `INTERSECT` operands that aren't already bare "
            "`SELECT * FROM <cte>` references must be lifted into their "
            "own CTEs. The resulting body should be a pure "
            "`SELECT * FROM <a> EXCEPT SELECT * FROM <b>`."
        )

    # 11. Avoid DISTINCT — explicit ranked + deduped pattern.
    if rule_on("distinct_rewrite"):
        n += 1
        lines.append(
            f"{n}. **Replace DISTINCT with explicit dedup** — `SELECT "
            "DISTINCT` hides data-quality problems. Rewrite as two CTEs:"
        )
        lines.append("   ```sql")
        lines.append("   WITH <entity>_ranked AS (")
        lines.append("     SELECT <projection_list>,")
        lines.append("            ROW_NUMBER() OVER (")
        lines.append("              PARTITION BY <all projection columns>")
        lines.append("              ORDER BY (SELECT NULL)")
        lines.append("            ) AS rn")
        lines.append("     FROM <original-FROM-WHERE-GROUP-BY>")
        lines.append("   ), <entity>_deduped AS (")
        lines.append("     SELECT <projection_list> FROM <entity>_ranked WHERE rn = 1")
        lines.append("   )")
        lines.append("   SELECT * FROM <entity>_deduped")
        lines.append("   ```")
        lines.append(
            "   The `_ranked` CTE makes the duplication visible and "
            "inspectable; the `_deduped` CTE is a separate filtering step. "
            "Use a meaningful ORDER BY in the window when there's a "
            "deterministic tie-break key (e.g., latest scan_date wins); "
            "otherwise `(SELECT NULL)` signals an order-independent dedup."
        )

    # 12. Table qualifier rewrite.
    if rule_on("table_qualifier_rewrite") and opt.table_qualifier:
        n += 1
        lines.append(
            f"{n}. **Table Qualifier** — every base table reference "
            f"in the rewrite must use the qualifier "
            f"`{opt.table_qualifier}` in place of its original "
            "catalog/schema (e.g., `catalog.schema.t` -> "
            f"`{opt.table_qualifier}.t`)."
        )

    # 11. FULL OUTER replacement.
    n += 1
    lines.append(
        f"{n}. **Replace FULL OUTER JOIN + COALESCE** — restructure "
        "as three CTEs: `unique_rows_from_<a>` (LEFT JOIN + IS NULL), "
        "`unique_rows_from_<b>` (reverse direction), `matching_rows` "
        "(INNER JOIN), then `UNION ALL` the three. Each branch adds "
        "a source-tagging column."
    )

    # 12. Comment header.
    n += 1
    lines.append(
        f"{n}. **Comment Header** — prepend a `/* Migration Details "
        "... */` block to the rewrite, listing the rules applied and "
        "an `[X]` validation checklist."
    )

    lines.append("")
    lines.append("---")
    lines.append("Original query follows.")
    lines.append("")
    return "\n".join(lines) + "\n"


# =============================================================================
# 4c. MOVEMENT CSV EMITTER (customer-specific mapping format)
# =============================================================================
#
# Per-query CSV that captures the mapping in the shape one customer
# site uses (file is called ``movement.csv`` there). Columns:
#
#   target_model_name, target_table_name, target_column_name,
#   source_model_name, source_table_name, source_column_name,
#   derived_indicator, movement_expression, dependency_type
#
# Configurable: target_model_name, source_model_name, dependency_type.
# Defaults: SSF / SSF_SOURCE / strict.


MOVEMENT_CSV_HEADER = [
    "target_model_name",
    "target_table_name",
    "target_column_name",
    "source_model_name",
    "source_table_name",
    "source_column_name",
    "derived_indicator",
    "movement_expression",
    "dependency_type",
    "source_version",
]


@dataclass
class MovementConfig:
    """Customer-overridable values for movement.csv.

    Defaults align with the customer's current spreadsheet shape.
    Override via ``sql_process.config.yaml`` or CLI flags.
    """
    target_model_name: str = "Converter"
    source_model_name: str = "SSF"          # default when source has no schema
    dependency_type: str = "strict"
    granularity: str = "column"              # "column" | "table"


@dataclass
class CustomerRuleConfig:
    """Customer-overridable values for the migration rule pack.

    Defaults are deliberately MINIMAL — only the SCD2 / change-tracking
    columns that are broadly applicable across data warehouses.
    Site-specific columns (file_delivery_entity, xsd_version, …) live
    only in ``sql_process.config.yaml`` so that running with a
    different YAML (or ``--config none``) doesn't silently pollute the
    metadata blacklist.
    """
    legacy_schemas: List[str] = field(default_factory=list)
    replacement_schema: str = "automatically_inferred_qualifier"
    date_variable: str = "process_date"
    metadata_blacklist: List[str] = field(default_factory=lambda: [
        # SCD2 / change-tracking metadata only. Add site-specific
        # columns via YAML.
        "snapshot_date", "insert_dts", "update_dts",
        "current_flag", "delete_flag", "delta_flag",
        "create_timestamp", "start_dts", "end_dts",
    ])
    obsolete_cte_names: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "legacy_schemas": list(self.legacy_schemas),
            "replacement_schema": self.replacement_schema,
            "date_variable": self.date_variable,
            "metadata_blacklist": list(self.metadata_blacklist),
            "obsolete_cte_names": list(self.obsolete_cte_names),
        }


@dataclass
class OutputConfig:
    """Toggles for the optional per-query artefacts.

    Always emitted: optimized.sql, findings.md, genie.md, movement.csv.
    Optional: bfm_mapping (mapping.bfm.yaml), cte_mapping
    (mapping.cte.yaml), annotation_entity (annotation.entity.yaml).
    """
    bfm_mapping: bool = False
    cte_mapping: bool = False
    annotation_entity: bool = False


@dataclass
class OptimizeConfig:
    """Optimization knobs.

    ``table_qualifier`` rewrites every base-table reference in the
    optimized SQL to use this single qualifier instead of the
    original catalog/schema. Set to None or "" to skip.

    ``union_separator`` is a pre-parse substitution: when the input
    SQL uses a comma between top-level SELECTs to mean UNION ALL,
    this tells the script to convert each occurrence before parsing.

    ``enabled`` is a per-rule toggle map. Every key defaults to True;
    set a key to False in the YAML to skip that transform. Skipped
    transforms also drop out of the Genie instruction text so the
    prompt only lists rules the agent should apply.
    """
    table_qualifier: Optional[str] = "schema_identifier_ssf_snapshot"
    union_separator: Optional[str] = ","
    enabled: Dict[str, bool] = field(default_factory=lambda: {
        "where_true_removed":        True,
        "schema_replacement":        True,
        "legacy_date_replacement":   True,
        "obsolete_cte_removal":      True,
        "metadata_column_strip":     True,
        "subquery_lift":             True,
        "full_outer_rewrite":        True,
        "window_order_by":           True,
        "distinct_rewrite":          True,
        "source_pushdown":           True,
        "joined_cte":                True,
        "filtered_cte":              True,
        "aggregation_cte":           True,
        "union_branch_lift":         True,
        "table_qualifier_rewrite":   True,
    })


@dataclass
class FileConfig:
    """Glob include/exclude patterns relative to the input directory.

    When ``include`` is empty, every ``*.sql`` file under the input
    directory is processed (subject to the ``--recursive`` flag).
    ``exclude`` is applied after include.
    """
    include: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)


@dataclass
class RunConfig:
    """Where to read inputs from and where to write outputs.

    Lets a customer drive a full pipeline run from a single YAML
    rather than passing input/output/recursive on the command line
    every time. CLI flags override these when supplied.
    """
    input_dir: Optional[Path] = None
    output_dir: Optional[Path] = None
    recursive: bool = False


@dataclass
class PipelineConfig:
    """Top-level container for all configurable values.

    Loaded from ``sql_process.config.yaml`` (sibling of this script
    by default). CLI flags can override individual values after load.
    """
    movement: MovementConfig = field(default_factory=MovementConfig)
    rules: CustomerRuleConfig = field(default_factory=CustomerRuleConfig)
    outputs: OutputConfig = field(default_factory=OutputConfig)
    optimize: OptimizeConfig = field(default_factory=OptimizeConfig)
    files: FileConfig = field(default_factory=FileConfig)
    run: RunConfig = field(default_factory=RunConfig)


def load_config(path: Optional[Path]) -> PipelineConfig:
    """Load the pipeline configuration.

    Lookup order:
      1. Explicit ``path`` argument (when not None)
      2. ``<script_dir>/sql_process.config.yaml`` if it exists
      3. Built-in defaults (empty PipelineConfig)

    Missing keys at any level fall back to the dataclass defaults,
    so a partial config file is fine.
    """
    cfg_path: Optional[Path] = None
    if path is not None:
        if not path.is_file():
            raise FileNotFoundError(f"Config file not found: {path}")
        cfg_path = path
    else:
        script_dir = Path(__file__).parent if "__file__" in globals() else Path.cwd()
        candidate = script_dir / "sql_process.config.yaml"
        if candidate.is_file():
            cfg_path = candidate

    if cfg_path is None:
        return PipelineConfig()

    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    cfg = PipelineConfig()

    mv = raw.get("movement") or {}
    if mv:
        cfg.movement = MovementConfig(
            target_model_name=str(mv.get("target_model_name", cfg.movement.target_model_name)),
            source_model_name=str(mv.get("source_model_name", cfg.movement.source_model_name)),
            dependency_type=str(mv.get("dependency_type", cfg.movement.dependency_type)),
            granularity=str(mv.get("granularity", cfg.movement.granularity)),
        )

    rl = raw.get("rules") or {}
    if rl:
        # Use ``is None`` (not ``or``) when reading list-valued keys so
        # an explicit empty list in the YAML is honoured. ``or`` falls
        # back to the dataclass defaults on falsy values, which made
        # ``metadata_blacklist: []`` silently re-introduce the
        # hardcoded defaults.
        mb_raw = rl.get("metadata_blacklist")
        ob_raw = rl.get("obsolete_cte_names")
        ls_raw = rl.get("legacy_schemas")
        cfg.rules = CustomerRuleConfig(
            legacy_schemas=list(ls_raw) if ls_raw is not None else [],
            replacement_schema=str(rl.get("replacement_schema", cfg.rules.replacement_schema)),
            date_variable=str(rl.get("date_variable", cfg.rules.date_variable)),
            metadata_blacklist=(
                list(mb_raw) if mb_raw is not None else list(cfg.rules.metadata_blacklist)
            ),
            obsolete_cte_names=(
                list(ob_raw) if ob_raw is not None else list(cfg.rules.obsolete_cte_names)
            ),
        )

    outs = raw.get("outputs") or {}
    if outs:
        cfg.outputs = OutputConfig(
            bfm_mapping=bool(outs.get("bfm_mapping", cfg.outputs.bfm_mapping)),
            cte_mapping=bool(outs.get("cte_mapping", cfg.outputs.cte_mapping)),
            annotation_entity=bool(outs.get("annotation_entity", cfg.outputs.annotation_entity)),
        )

    op = raw.get("optimize") or {}
    if op:
        # Merge per-rule toggles: start from defaults (all True),
        # apply YAML overrides on top. Unknown keys are ignored.
        merged_enabled = dict(cfg.optimize.enabled)
        yaml_enabled = op.get("enabled") or {}
        for key, val in yaml_enabled.items():
            if key in merged_enabled:
                merged_enabled[key] = bool(val)
        cfg.optimize = OptimizeConfig(
            table_qualifier=op.get("table_qualifier") or None,
            union_separator=op.get("union_separator") or None,
            enabled=merged_enabled,
        )

    fl = raw.get("files") or {}
    if fl:
        cfg.files = FileConfig(
            include=list(fl.get("include") or []),
            exclude=list(fl.get("exclude") or []),
        )

    rn = raw.get("run") or {}
    if rn:
        cfg.run = RunConfig(
            input_dir=Path(rn["input_dir"]) if rn.get("input_dir") else None,
            output_dir=Path(rn["output_dir"]) if rn.get("output_dir") else None,
            recursive=bool(rn.get("recursive", False)),
        )

    return cfg


def _target_table_name(pf: ParsedFile) -> str:
    """Derive ``target_table_name`` from the input filename.

    If the filename stem contains a hyphen, take everything left of
    the first hyphen (``customer-revenue.sql`` -> ``customer``).
    Otherwise use the full stem (``customer_revenue.sql`` ->
    ``customer_revenue``).
    """
    stem = pf.path.stem
    if "-" in stem:
        return stem.split("-", 1)[0]
    return stem


def _source_version(pf: ParsedFile) -> str:
    """Return the 3rd hyphen-separated part of the filename stem.

    Convention: SQL filenames follow ``<entity>-<role>-<version>.sql``.
    When the filename has fewer than three hyphen-separated parts,
    returns the empty string. Callers should emit a
    ``MISSING_SOURCE_VERSION`` finding when they want the user to
    notice.
    """
    parts = pf.path.stem.split("-")
    if len(parts) >= 3:
        return parts[2]
    return ""


@dataclass
class SourceInfo:
    """Per-source metadata used to populate movement.csv rows.

    The bare table name (with catalog/schema stripped) becomes
    ``source_table_name``; the schema (uppercased) becomes
    ``source_model_name`` when present; ``left_join`` controls the
    per-row ``dependency_type`` (``loose`` vs the config default).
    """
    table_name: str
    schema: Optional[str] = None
    catalog: Optional[str] = None
    left_join: bool = False


def _build_source_info_map(pf: ParsedFile) -> Dict[str, SourceInfo]:
    """Build an ``alias -> SourceInfo`` map by walking the parsed AST.

    Records each base table's schema (if present) and whether the
    table was reached via a LEFT JOIN — both needed by the movement
    CSV emitter to populate the new columns.
    """
    out: Dict[str, SourceInfo] = {}
    if pf.parsed is None:
        return out
    # Walk all Tables and record name/schema/catalog.
    for tbl in pf.parsed.find_all(exp.Table):
        name = tbl.name
        if not name:
            continue
        alias = tbl.alias or name
        schema = tbl.args.get("db").name if tbl.args.get("db") else None
        catalog = tbl.args.get("catalog").name if tbl.args.get("catalog") else None
        out[alias] = SourceInfo(
            table_name=name,
            schema=schema,
            catalog=catalog,
        )
    # Walk Joins and flip the left_join flag for matching aliases.
    for join in pf.parsed.find_all(exp.Join):
        side = (join.args.get("side") or "").upper()
        if side != "LEFT":
            continue
        join_tbl = join.this if isinstance(join.this, exp.Table) else None
        if join_tbl is None:
            continue
        alias = join_tbl.alias or join_tbl.name
        info = out.get(alias)
        if info is not None:
            info.left_join = True
    return out


def _resolve_source_info(
    alias_or_name: str,
    info_map: Dict[str, SourceInfo],
) -> SourceInfo:
    """Resolve a column reference's table qualifier to its SourceInfo.

    Falls back to a bare SourceInfo when the alias isn't in the map
    (typical for column refs into CTEs the lineage extractor can't
    fully resolve)."""
    if alias_or_name and alias_or_name in info_map:
        return info_map[alias_or_name]
    return SourceInfo(table_name=alias_or_name or "")


def _is_derived_lineage(lin: ColumnLineage) -> bool:
    """Translate the lineage classifier into the customer's
    derived_indicator. True for aggregates / expressions / constants;
    False for direct column refs and pure renames."""
    return lin.derivation not in ("direct", "rename")


# Strip MDDE annotation block-comments (``/* @pk */`` etc.) that
# sqlglot reproduces from the parsed AST. They're useful in the
# optimised SQL output but pure noise in a CSV cell.
_MOVEMENT_COMMENT_RE = re.compile(r"\s*/\*[^*]*\*/\s*")


def _clean_movement_expression(expr_sql: str) -> str:
    """Return the SQL fragment in a form suitable for a CSV cell:
    unquoted identifiers, MDDE annotation comments stripped,
    whitespace collapsed."""
    # Re-parse just enough to drop the identifier quoting that
    # qualify() added. Fail-soft: if parse fails for any reason,
    # use the raw string with comments stripped.
    cleaned = _MOVEMENT_COMMENT_RE.sub(" ", expr_sql).strip()
    try:
        parsed = sqlglot.parse_one(cleaned)
        for ident in parsed.find_all(exp.Identifier):
            ident.set("quoted", False)
        cleaned = parsed.sql()
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        pass
    # Collapse runs of whitespace.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def emit_movement_csv_rows(
    pf: ParsedFile,
    config: MovementConfig,
    metadata_blacklist: Optional[List[str]] = None,
) -> List[List[str]]:
    """Build the per-query movement.csv rows for ``pf``.

    ``metadata_blacklist`` (when provided) filters out lineage entries
    whose target column matches a blacklisted metadata column —
    keeps the CSV in sync with the optimised SQL after
    ``strip_metadata_columns`` ran.

    Behaviour:
      - One row per ``(target_column, source_column)`` pair from the
        lineage extraction (column-level granularity, default), OR
      - One row per source table when ``config.granularity == "table"``
        (column slots empty, no expression)
      - Plus one row per join-only source table (referenced in
        FROM/JOIN but contributing no projection).
      - ``source_table_name`` strips catalog/schema.
      - ``source_model_name`` per-row: UPPER(schema) when the source
        table carries a schema; falls back to ``config.source_model_name``.
      - ``dependency_type`` per-row: ``loose`` when the source is
        reached via LEFT JOIN; otherwise ``config.dependency_type``.
      - ``source_version`` from the 3rd hyphen-part of the filename.
    """
    rows: List[List[str]] = []
    target_table = _target_table_name(pf)
    source_version = _source_version(pf)
    info_map = _build_source_info_map(pf)
    excluded_columns = {c.lower() for c in (metadata_blacklist or [])}

    def _model_name(info: SourceInfo) -> str:
        return info.schema.upper() if info.schema else config.source_model_name

    def _dep(info: SourceInfo) -> str:
        return "loose" if info.left_join else config.dependency_type

    # Table-level granularity: one row per unique source table,
    # column slots empty. Skip everything below.
    if config.granularity == "table":
        seen: Set[str] = set()
        for src in pf.source_tables:
            # `src` from pf.source_tables is already in "catalog.db.table"
            # qualified form when present — find the matching info by
            # bare table name.
            bare = src.split(".")[-1]
            info = next(
                (i for i in info_map.values() if i.table_name == bare),
                SourceInfo(table_name=bare),
            )
            key = info.table_name
            if key in seen:
                continue
            seen.add(key)
            rows.append([
                config.target_model_name,
                target_table,
                "",
                _model_name(info),
                info.table_name,
                "",
                "false",
                "",
                _dep(info),
                source_version,
            ])
        return rows

    sources_with_projections: Set[str] = set()

    for lin in pf.lineage:
        if lin.output_column == "*":
            continue
        # Skip metadata-column outputs — these are stripped from the
        # optimised SQL by `strip_metadata_columns`, so they shouldn't
        # show up in movement.csv either.
        if lin.output_column.lower() in excluded_columns:
            continue
        derived = "true" if _is_derived_lineage(lin) else "false"
        expression = _clean_movement_expression(lin.expression)
        if lin.source_columns:
            for tbl, col in lin.source_columns:
                info = _resolve_source_info(tbl, info_map)
                sources_with_projections.add(info.table_name)
                rows.append([
                    config.target_model_name,
                    target_table,
                    lin.output_column,
                    _model_name(info),
                    info.table_name,
                    col,
                    derived,
                    expression,
                    _dep(info),
                    source_version,
                ])
        else:
            # Constant or empty-source projection — record the
            # target column with no source.
            rows.append([
                config.target_model_name,
                target_table,
                lin.output_column,
                config.source_model_name,
                "",
                "",
                derived,
                expression,
                config.dependency_type,
                source_version,
            ])

    # Join-only sources: tables referenced via FROM/JOIN that no
    # projection actually reads. Emit one row with empty column slots
    # so the dependency is still recorded.
    for src in pf.source_tables:
        bare = src.split(".")[-1]
        if bare in sources_with_projections:
            continue
        info = next(
            (i for i in info_map.values() if i.table_name == bare),
            SourceInfo(table_name=bare),
        )
        rows.append([
            config.target_model_name,
            target_table,
            "",
            _model_name(info),
            info.table_name,
            "",
            "false",
            "",
            _dep(info),
            source_version,
        ])

    return rows


def _rows_to_csv(rows: List[List[str]]) -> str:
    """Render header + rows as CSV text. Every field is double-quoted
    to match the customer site's import expectations."""
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writerow(MOVEMENT_CSV_HEADER)
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def emit_movement_csv(
    pf: ParsedFile,
    config: MovementConfig,
    metadata_blacklist: Optional[List[str]] = None,
) -> str:
    """Per-query movement.csv text."""
    return _rows_to_csv(emit_movement_csv_rows(pf, config, metadata_blacklist))


def emit_movement_csv_rollup(
    parsed_files: List[ParsedFile],
    config: MovementConfig,
    metadata_blacklist: Optional[List[str]] = None,
) -> str:
    """Run-level rollup: header once, all per-query rows concatenated
    in input order."""
    all_rows: List[List[str]] = []
    for pf in parsed_files:
        if pf.parse_error:
            continue
        all_rows.extend(emit_movement_csv_rows(pf, config, metadata_blacklist))
    return _rows_to_csv(all_rows)


def emit_findings_md(
    pf: ParsedFile,
    findings: List[QualityFinding],
) -> str:
    """Per-query findings markdown. A scoped view of report.md
    covering only this file: parse/qualify status, transforms
    applied, and quality findings (auto-fixed + flagged)."""
    lines: List[str] = []
    lines.append(f"# Findings — `{pf.path.name}`")
    lines.append("")

    # Parse + qualify status.
    if pf.parse_error:
        lines.append(f"**Parse:** failed — {pf.parse_error}")
    else:
        lines.append("**Parse:** ok")
    if pf.qualified:
        lines.append("**Qualify:** ok")
    elif pf.qualify_error:
        lines.append(f"**Qualify:** skipped — {pf.qualify_error}")
    else:
        lines.append("**Qualify:** not run (no metadata)")
    lines.append("")

    if not findings:
        lines.append("_No quality findings._")
        return "\n".join(lines) + "\n"

    # Severity tally.
    by_sev: Dict[str, int] = {}
    fixed_count = 0
    for f in findings:
        by_sev[f.severity] = by_sev.get(f.severity, 0) + 1
        if f.auto_fixed:
            fixed_count += 1
    lines.append(
        f"**Total:** {len(findings)} "
        f"(error={by_sev.get('error', 0)}, "
        f"warning={by_sev.get('warning', 0)}, "
        f"info={by_sev.get('info', 0)})  "
    )
    lines.append(f"**Auto-fixed:** {fixed_count}")
    lines.append("")

    lines.append("| Location | Rule | Severity | Auto-fixed | Message |")
    lines.append("|---|---|---|---|---|")
    for f in findings:
        fixed = "yes" if f.auto_fixed else "no"
        msg = f.message.replace("\n", " ")
        if len(msg) > 120:
            msg = msg[:117] + "..."
        lines.append(
            f"| {f.location} | {f.rule} | {f.severity} | {fixed} | {msg} |"
        )

    return "\n".join(lines) + "\n"


# =============================================================================
# 5. OPENLINEAGE ROLL-UP
# =============================================================================


def emit_openlineage(parsed_files: List[ParsedFile]) -> Dict[str, Any]:
    """One OpenLineage event per input file, packaged as a list.

    Note on reproducibility: eventTime is omitted from individual
    events (the spec allows this for design-time lineage). Callers
    that need a real timestamp can wrap the emitted events at
    publish time.
    """
    events = []
    for pf in parsed_files:
        if pf.parse_error:
            continue
        # Inputs = source tables; output = the entity itself.
        inputs = []
        for src in pf.source_tables:
            inputs.append({
                "namespace": "mdde-demo.sql_process",
                "name": src,
            })

        output_facets: Dict[str, Any] = {
            "schema": {
                "fields": [
                    {"name": lin.output_column, "type": "INFERRED"}
                    for lin in pf.lineage if lin.output_column != "*"
                ],
            },
            "columnLineage": {
                "fields": {
                    lin.output_column: {
                        "inputFields": [
                            {
                                "namespace": "mdde-demo.sql_process",
                                "name": tbl or "<inherited>",
                                "field": col,
                            }
                            for tbl, col in lin.source_columns
                        ],
                    }
                    for lin in pf.lineage if lin.output_column != "*"
                },
            },
        }

        events.append({
            "eventType": "COMPLETE",
            "producer": "mdde-demo://examples/sql_process",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent",
            "job": {
                "namespace": "mdde-demo.sql_process",
                "name": pf.entity_name,
            },
            "inputs": inputs,
            "outputs": [{
                "namespace": "mdde-demo.sql_process",
                "name": pf.entity_name,
                "facets": output_facets,
            }],
        })

    # Cross-file lineage stitching: when one file's source matches
    # another's output, record the producer -> consumer edge.
    stitching = build_stitching(parsed_files)
    return {
        "events": events,
        "stitching": stitching,
    }


# =============================================================================
# 5b. CROSS-FILE LINEAGE STITCHING
# =============================================================================


@dataclass
class StitchEdge:
    """One producer -> consumer edge between two files in the run."""
    producer_file: str   # filename that emits the table
    consumer_file: str   # filename that reads it
    via_table: str       # the table/entity name they share


def build_file_graph(
    parsed_files: List[ParsedFile],
) -> List[StitchEdge]:
    """Compute cross-file edges by matching consumer source_tables
    against producer entity_names.

    A file's `entity_name` is treated as the table it produces.
    A file's `source_tables` are the tables it consumes. Wherever
    a consumer's source matches a producer's entity name, we record
    a single edge.
    """
    # Build a map: entity_name -> producing file. If two files declare
    # the same entity_name (rare; usually a copy-paste bug), the later
    # one wins — and we surface it later in the report.
    producers: Dict[str, ParsedFile] = {}
    for pf in parsed_files:
        if pf.parse_error:
            continue
        producers[pf.entity_name] = pf

    edges: List[StitchEdge] = []
    seen: Set[Tuple[str, str, str]] = set()
    for consumer in parsed_files:
        if consumer.parse_error:
            continue
        for src in consumer.source_tables:
            # Match on the bare table name (last segment) and on the
            # full qualified name. This covers both `FROM stg_customers`
            # and `FROM main.gold.dim_customer` if the producer's
            # entity_name is `dim_customer`.
            candidates = {src, src.split(".")[-1]}
            for cand in candidates:
                producer = producers.get(cand)
                if producer is None or producer is consumer:
                    continue
                key = (producer.path.name, consumer.path.name, cand)
                if key in seen:
                    continue
                seen.add(key)
                edges.append(StitchEdge(
                    producer_file=producer.path.name,
                    consumer_file=consumer.path.name,
                    via_table=cand,
                ))
    return edges


def build_stitching(
    parsed_files: List[ParsedFile],
) -> Dict[str, Any]:
    """Stitching block packaged for inclusion in lineage.json."""
    edges = build_file_graph(parsed_files)
    return {
        "edges": [
            {
                "producer_job": _entity_for_file(parsed_files, e.producer_file),
                "consumer_job": _entity_for_file(parsed_files, e.consumer_file),
                "via_table": e.via_table,
            }
            for e in edges
        ],
        "namespace": "mdde-demo.sql_process",
        "edge_count": len(edges),
    }


def _entity_for_file(
    parsed_files: List[ParsedFile],
    filename: str,
) -> str:
    for pf in parsed_files:
        if pf.path.name == filename:
            return pf.entity_name
    return filename


def emit_mermaid_graph(parsed_files: List[ParsedFile]) -> str:
    """Render the file dependency graph as a Mermaid flowchart.

    Nodes are grouped by `pf.annotations.entity['layer']` so the
    graph reads top-down: source -> staging -> integration ->
    business. Files without a declared layer drop into an
    "unknown" subgraph.
    """
    edges = build_file_graph(parsed_files)
    by_layer: Dict[str, List[ParsedFile]] = {}
    for pf in parsed_files:
        if pf.parse_error:
            continue
        layer = pf.annotations.entity.get("layer", "unknown")
        by_layer.setdefault(layer, []).append(pf)

    # Stable layer ordering when present.
    layer_order = ["source", "staging", "integration", "semantic",
                   "business", "delivery", "unknown"]
    sorted_layers = [l for l in layer_order if l in by_layer] + [
        l for l in by_layer if l not in layer_order
    ]

    lines: List[str] = ["flowchart LR"]
    for layer in sorted_layers:
        lines.append(f"  subgraph {layer}")
        for pf in by_layer[layer]:
            node_id = pf.entity_name.replace(".", "_").replace("-", "_")
            label = f"{pf.entity_name}<br/><i>{pf.path.name}</i>"
            lines.append(f'    {node_id}["{label}"]')
        lines.append("  end")

    # Edges between entities, stable ordering by producer name.
    edges = sorted(edges, key=lambda e: (e.producer_file, e.consumer_file))
    for e in edges:
        producer = _entity_for_file(parsed_files, e.producer_file)
        consumer = _entity_for_file(parsed_files, e.consumer_file)
        p_id = producer.replace(".", "_").replace("-", "_")
        c_id = consumer.replace(".", "_").replace("-", "_")
        lines.append(f"  {p_id} --> {c_id}")

    # External source tables (referenced FROM but not produced by any
    # file in the run). These hang off as inputs to the first layer.
    produced = {pf.entity_name for pf in parsed_files if not pf.parse_error}
    external_inputs: Dict[str, Set[str]] = {}
    for pf in parsed_files:
        if pf.parse_error:
            continue
        for src in pf.source_tables:
            if src not in produced and src.split(".")[-1] not in produced:
                external_inputs.setdefault(src, set()).add(pf.entity_name)
    if external_inputs:
        lines.append("  subgraph external [\"external sources\"]")
        for src in sorted(external_inputs):
            ext_id = "ext_" + src.replace(".", "_").replace("-", "_")
            lines.append(f'    {ext_id}[("{src}")]')
        lines.append("  end")
        for src, consumers in sorted(external_inputs.items()):
            ext_id = "ext_" + src.replace(".", "_").replace("-", "_")
            for consumer in sorted(consumers):
                c_id = consumer.replace(".", "_").replace("-", "_")
                lines.append(f"  {ext_id} --> {c_id}")

    return "\n".join(lines)


# =============================================================================
# 6. REPORT
# =============================================================================


def emit_report(
    parsed_files: List[ParsedFile],
    findings_per_file: Dict[str, List[QualityFinding]],
    timings_per_file: Optional[Dict[str, Dict[str, float]]] = None,
) -> str:
    """Markdown summary of the run.

    Timestamp is omitted so the report is byte-identical across
    re-runs on the same input. CI consumers can stamp it externally
    if they need provenance.

    When ``timings_per_file`` is provided (i.e., the run was profiled),
    a ``## Profile`` section is appended with phase-level timing
    totals plus the slowest 10 files by total elapsed time.
    """
    lines: List[str] = []
    lines.append("# sql_process — run report")
    lines.append("")
    lines.append(f"Files processed: **{len(parsed_files)}**")
    lines.append("")

    # Per-file summary
    lines.append("## Files")
    lines.append("")
    lines.append("| File | Entity | Layer | Qualified | CTEs | Sources | Output cols | Quality issues |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for pf in parsed_files:
        findings = findings_per_file.get(pf.path.name, [])
        n_issues = len(findings)
        if pf.qualified:
            qual = "yes"
        elif pf.qualify_error:
            qual = "skipped"
        else:
            qual = "—"
        lines.append(
            f"| `{pf.path.name}` | {pf.entity_name} | "
            f"{pf.annotations.entity.get('layer', '?')} | "
            f"{qual} | "
            f"{len(pf.cte_names)} | {len(pf.source_tables)} | "
            f"{len([l for l in pf.lineage if l.output_column != '*'])} | "
            f"{n_issues} |"
        )
    lines.append("")

    # Qualify failures — surface so users can fix their schema file.
    qualify_failures = [pf for pf in parsed_files if pf.qualify_error]
    if qualify_failures:
        lines.append("## Qualify skipped")
        lines.append("")
        lines.append(
            "These files could not be qualified (sqlglot.optimizer.qualify "
            "raised) — lineage extraction fell back to the unqualified "
            "AST. Usually means a referenced table is missing from the "
            "metadata file, or a column reference is genuinely ambiguous."
        )
        lines.append("")
        lines.append("| File | Reason |")
        lines.append("|---|---|")
        for pf in qualify_failures:
            reason = pf.qualify_error
            if len(reason) > 100:
                reason = reason[:97] + "..."
            lines.append(f"| `{pf.path.name}` | {reason} |")
        lines.append("")

    # Quality findings
    total_findings: List[QualityFinding] = []
    for fs in findings_per_file.values():
        total_findings.extend(fs)
    by_sev: Dict[str, int] = {}
    for f in total_findings:
        by_sev[f.severity] = by_sev.get(f.severity, 0) + 1

    lines.append("## Quality findings")
    lines.append("")
    if not total_findings:
        lines.append("_No quality issues detected._")
    else:
        lines.append(
            f"**Total:** {len(total_findings)} "
            f"(error={by_sev.get('error', 0)}, "
            f"warning={by_sev.get('warning', 0)}, "
            f"info={by_sev.get('info', 0)})  "
            f"\n**Auto-fixed:** "
            f"{sum(1 for f in total_findings if f.auto_fixed)}"
        )
        lines.append("")
        lines.append("| File | Location | Rule | Severity | Auto-fixed | Message |")
        lines.append("|---|---|---|---|---|---|")
        for filename, findings in findings_per_file.items():
            for f in findings:
                fixed = "yes" if f.auto_fixed else "no"
                msg = f.message.replace("\n", " ")
                if len(msg) > 80:
                    msg = msg[:77] + "..."
                lines.append(
                    f"| `{filename}` | {f.location} | {f.rule} | "
                    f"{f.severity} | {fixed} | {msg} |"
                )
    lines.append("")

    # Mapping coverage
    lines.append("## Mapping coverage")
    lines.append("")
    total_outputs = 0
    resolved_outputs = 0
    for pf in parsed_files:
        for lin in pf.lineage:
            if lin.output_column == "*":
                continue
            total_outputs += 1
            if lin.source_columns:
                resolved_outputs += 1
    if total_outputs:
        coverage = (resolved_outputs / total_outputs) * 100
        lines.append(
            f"**{resolved_outputs}/{total_outputs}** output columns have "
            f"a resolved source attribute ({coverage:.0f}%)"
        )
    else:
        lines.append("_No output columns to assess._")
    lines.append("")

    # Cross-file lineage stitching
    edges = build_file_graph(parsed_files)
    lines.append("## Cross-file lineage")
    lines.append("")
    if edges:
        lines.append(
            f"**{len(edges)}** producer-consumer edge(s) stitched across "
            f"the file set."
        )
        lines.append("")
        lines.append("| Producer | Consumer | Via table |")
        lines.append("|---|---|---|")
        for e in sorted(edges, key=lambda x: (x.producer_file, x.consumer_file)):
            lines.append(
                f"| `{e.producer_file}` | `{e.consumer_file}` | "
                f"`{e.via_table}` |"
            )
        lines.append("")
        lines.append("```mermaid")
        lines.append(emit_mermaid_graph(parsed_files))
        lines.append("```")
    else:
        lines.append(
            "_No cross-file edges detected — every input file's source "
            "tables are external to this folder._"
        )
    lines.append("")

    # Annotations summary
    lines.append("## SQL-First annotations")
    lines.append("")
    lines.append("| File | Entity | Annotated columns | Tags found |")
    lines.append("|---|---|---|---|")
    for pf in parsed_files:
        ann_cols = pf.annotations.columns
        all_flags: Set[str] = set()
        for ca in ann_cols.values():
            all_flags.update(ca.flags)
        lines.append(
            f"| `{pf.path.name}` | "
            f"{pf.annotations.entity.get('entity', '?')} | "
            f"{len(ann_cols)} | "
            f"{', '.join(sorted(all_flags)) if all_flags else '—'} |"
        )
    lines.append("")

    # ---- Profile section (only when timings were collected) -----------
    if timings_per_file:
        lines.append("## Profile")
        lines.append("")

        # Phase totals.
        phase_totals: Dict[str, float] = {}
        for fts in timings_per_file.values():
            for phase, secs in fts.items():
                phase_totals[phase] = phase_totals.get(phase, 0.0) + secs
        total_secs = phase_totals.get("total", sum(phase_totals.values()))

        lines.append("Phase totals (cumulative across all files):")
        lines.append("")
        lines.append("| Phase | Total (s) | % of total |")
        lines.append("|---|---:|---:|")
        for phase in ("parse", "quality", "auto_fix", "total"):
            secs = phase_totals.get(phase, 0.0)
            pct = (100.0 * secs / total_secs) if total_secs > 0 else 0.0
            lines.append(f"| {phase} | {secs:.3f} | {pct:.1f}% |")
        lines.append("")

        # Slowest 10 files by total.
        sorted_files = sorted(
            timings_per_file.items(),
            key=lambda kv: kv[1].get("total", 0.0),
            reverse=True,
        )
        lines.append("Slowest 10 files (by total elapsed):")
        lines.append("")
        lines.append("| File | Parse (s) | Quality (s) | Auto-fix (s) | Total (s) |")
        lines.append("|---|---:|---:|---:|---:|")
        for rel_key, fts in sorted_files[:10]:
            lines.append(
                f"| `{rel_key}` | {fts.get('parse', 0):.3f} | "
                f"{fts.get('quality', 0):.3f} | {fts.get('auto_fix', 0):.3f} | "
                f"{fts.get('total', 0):.3f} |"
            )
        lines.append("")

    return "\n".join(lines)


# =============================================================================
# 7. DIFF MODE — compare two output folders, emit diff.md
# =============================================================================


def emit_diff(previous_output: Path, current_output: Path) -> str:
    """Compare two output folders and return a markdown diff report.

    Compares four artefact classes:
      - per-file optimised SQL (boolean: changed yes/no)
      - per-target BFM mapping YAML (column-level: added/removed/changed)
      - cross-file stitching edges (added/removed)
      - quality-finding counts per file

    The report covers all four and is stable across runs (sorted by
    filename, no timestamps). When nothing changed, says so explicitly.
    """
    if not previous_output.is_dir():
        return f"# Diff\n\nPrevious output folder not found: `{previous_output}`\n"

    lines: List[str] = ["# sql_process — diff report", ""]
    lines.append(f"Previous run: `{previous_output}`")
    lines.append(f"Current run:  `{current_output}`")
    lines.append("")

    any_change = False

    # --------------------------------------------------------------
    # 1. Optimised SQL — boolean change per file
    # Per-query layout: <output>/<rel.parent>/<stem>/optimized.sql
    # The diff key is the query folder path (everything up to but
    # not including "optimized.sql").
    # --------------------------------------------------------------
    prev_sql_files = _collect_per_query_files(previous_output, "optimized.sql")
    curr_sql_files = _collect_per_query_files(current_output, "optimized.sql")

    added_sql = sorted(set(curr_sql_files) - set(prev_sql_files))
    removed_sql = sorted(set(prev_sql_files) - set(curr_sql_files))
    common_sql = sorted(set(prev_sql_files) & set(curr_sql_files))

    changed_sql: List[str] = []
    for rel in common_sql:
        prev_text = (previous_output / rel / "optimized.sql").read_text(encoding="utf-8")
        curr_text = (current_output / rel / "optimized.sql").read_text(encoding="utf-8")
        if prev_text != curr_text:
            changed_sql.append(rel)

    lines.append("## Optimised SQL")
    lines.append("")
    if not (added_sql or removed_sql or changed_sql):
        lines.append("_No changes._")
    else:
        any_change = True
        if added_sql:
            lines.append("**Added queries:**")
            lines.extend(f"- `{f}`" for f in added_sql)
            lines.append("")
        if removed_sql:
            lines.append("**Removed queries:**")
            lines.extend(f"- `{f}`" for f in removed_sql)
            lines.append("")
        if changed_sql:
            lines.append("**Modified queries:**")
            lines.extend(f"- `{f}`" for f in changed_sql)
            lines.append("")
    lines.append("")

    # --------------------------------------------------------------
    # 2. BFM mappings — per-target column-level diff
    # --------------------------------------------------------------
    prev_maps = _load_bfm_mappings(previous_output)
    curr_maps = _load_bfm_mappings(current_output)

    added_maps = sorted(set(curr_maps) - set(prev_maps))
    removed_maps = sorted(set(prev_maps) - set(curr_maps))
    common_maps = sorted(set(prev_maps) & set(curr_maps))

    map_rows: List[Tuple[str, List[str]]] = []
    for name in common_maps:
        deltas = _diff_bfm_mapping(prev_maps[name], curr_maps[name])
        if deltas:
            map_rows.append((name, deltas))

    lines.append("## Mapping (BFM)")
    lines.append("")
    if not (added_maps or removed_maps or map_rows):
        lines.append("_No changes._")
    else:
        any_change = True
        if added_maps:
            lines.append("**Added mappings:**")
            lines.extend(f"- `{f}`" for f in added_maps)
            lines.append("")
        if removed_maps:
            lines.append("**Removed mappings:**")
            lines.extend(f"- `{f}`" for f in removed_maps)
            lines.append("")
        if map_rows:
            lines.append("**Modified mappings:**")
            lines.append("")
            for name, deltas in map_rows:
                lines.append(f"### `{name}`")
                lines.append("")
                lines.extend(f"- {d}" for d in deltas)
                lines.append("")
    lines.append("")

    # --------------------------------------------------------------
    # 3. Cross-file stitching — edge changes
    # --------------------------------------------------------------
    prev_edges = _load_stitching(previous_output / "lineage.json")
    curr_edges = _load_stitching(current_output / "lineage.json")

    added_edges = sorted(curr_edges - prev_edges)
    removed_edges = sorted(prev_edges - curr_edges)

    lines.append("## Cross-file stitching")
    lines.append("")
    if not (added_edges or removed_edges):
        lines.append("_No changes._")
    else:
        any_change = True
        if added_edges:
            lines.append("**Added edges:**")
            for p, c, t in added_edges:
                lines.append(f"- `{p}` → `{c}` (via `{t}`)")
            lines.append("")
        if removed_edges:
            lines.append("**Removed edges:**")
            for p, c, t in removed_edges:
                lines.append(f"- `{p}` → `{c}` (via `{t}`)")
            lines.append("")
    lines.append("")

    # --------------------------------------------------------------
    # Summary
    # --------------------------------------------------------------
    lines.insert(4, f"**Status:** {'changed' if any_change else 'no changes'}  ")
    lines.insert(5, "")

    return "\n".join(lines)


def _collect_per_query_files(root: Path, filename: str) -> List[str]:
    """Return relative query-folder paths (forward-slash, str) for
    every folder under ``root`` that contains the given filename.

    In the per-query layout each query folder holds a fixed set of
    files (optimized.sql, mapping.bfm.yaml, ...). The diff key is
    the query folder path — everything up to but not including the
    filename.
    """
    if not root.is_dir():
        return []
    out: List[str] = []
    for p in sorted(root.rglob(filename)):
        if p.is_file():
            folder = p.parent.relative_to(root)
            out.append(str(folder).replace("\\", "/"))
    return out


def _load_bfm_mappings(output_root: Path) -> Dict[str, Dict[str, Any]]:
    """Load every ``mapping.bfm.yaml`` under the per-query layout,
    keyed by the query folder path relative to the output root."""
    if not output_root.is_dir():
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for p in sorted(output_root.rglob("mapping.bfm.yaml")):
        folder = p.parent.relative_to(output_root)
        rel = str(folder).replace("\\", "/")
        with open(p, "r", encoding="utf-8") as f:
            out[rel] = yaml.safe_load(f) or {}
    return out


def _diff_bfm_mapping(prev: Dict[str, Any], curr: Dict[str, Any]) -> List[str]:
    """Return human-readable bullet points describing per-attribute changes."""
    deltas: List[str] = []

    prev_attrs = {a["target"]: a for a in (prev.get("attributes") or [])}
    curr_attrs = {a["target"]: a for a in (curr.get("attributes") or [])}

    added = sorted(set(curr_attrs) - set(prev_attrs))
    removed = sorted(set(prev_attrs) - set(curr_attrs))
    common = sorted(set(prev_attrs) & set(curr_attrs))

    for col in added:
        deltas.append(f"+ added column `{col}`")
    for col in removed:
        deltas.append(f"- removed column `{col}`")
    for col in common:
        prev_a, curr_a = prev_attrs[col], curr_attrs[col]
        if prev_a.get("derivation") != curr_a.get("derivation"):
            deltas.append(
                f"~ `{col}` derivation: "
                f"{prev_a.get('derivation')} -> {curr_a.get('derivation')}"
            )
        prev_srcs = sorted(
            (s.get("entity"), s.get("attribute"))
            for s in (prev_a.get("sources") or [])
        )
        curr_srcs = sorted(
            (s.get("entity"), s.get("attribute"))
            for s in (curr_a.get("sources") or [])
        )
        if prev_srcs != curr_srcs:
            deltas.append(
                f"~ `{col}` sources changed "
                f"({len(prev_srcs)} -> {len(curr_srcs)} source(s))"
            )

    # Top-level changes other than attributes.
    if prev.get("source_entities") != curr.get("source_entities"):
        prev_se = set(prev.get("source_entities") or [])
        curr_se = set(curr.get("source_entities") or [])
        if curr_se - prev_se:
            deltas.append(f"+ source entities: {sorted(curr_se - prev_se)}")
        if prev_se - curr_se:
            deltas.append(f"- source entities: {sorted(prev_se - curr_se)}")

    return deltas


def _load_stitching(lineage_path: Path) -> Set[Tuple[str, str, str]]:
    if not lineage_path.is_file():
        return set()
    try:
        data = json.loads(lineage_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    edges = (data.get("stitching") or {}).get("edges") or []
    return {
        (e.get("producer_job", ""), e.get("consumer_job", ""), e.get("via_table", ""))
        for e in edges
    }


# =============================================================================
# 8. ORCHESTRATOR
# =============================================================================


def write_yaml(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _select_input_files(
    input_dir: Path,
    recursive: bool,
    include: List[str],
    exclude: List[str],
) -> List[Path]:
    """Apply include/exclude glob filters to enumerate input SQL files.

    Behaviour:
      - ``include`` empty -> every ``*.sql`` file under input_dir.
      - ``include`` non-empty -> union of files matching any pattern.
      - ``exclude`` is applied AFTER include (a file matching both is
        excluded).
      - All patterns are relative to ``input_dir`` and use forward
        slashes per pathlib's glob conventions.
    """
    pattern = "**/*.sql" if recursive else "*.sql"
    if not include:
        candidates = sorted(input_dir.glob(pattern))
    else:
        seen: Set[Path] = set()
        for inc in include:
            for p in input_dir.glob(inc):
                if p.suffix == ".sql" and p.is_file():
                    seen.add(p)
        candidates = sorted(seen)

    if exclude:
        excluded: Set[Path] = set()
        for exc in exclude:
            for p in input_dir.glob(exc):
                excluded.add(p)
        candidates = [p for p in candidates if p not in excluded]

    return candidates


def process_folder(
    input_dir: Path,
    output_dir: Path,
    recursive: bool = False,
    metadata_path: Optional[Path] = None,
    diff_against: Optional[Path] = None,
    movement_config: Optional[MovementConfig] = None,
    customer_config: Optional[CustomerRuleConfig] = None,
    output_config: Optional[OutputConfig] = None,
    optimize_config: Optional[OptimizeConfig] = None,
    file_config: Optional[FileConfig] = None,
    detection_only: bool = False,
    profile: bool = False,
) -> int:
    """Run the pipeline. Returns the number of files processed.

    If ``metadata_path`` points at a ``_metadata.yaml`` file, the
    schema is loaded and passed to sqlglot's qualify() pass for each
    SQL file. If ``metadata_path`` is None, the script auto-discovers
    ``<input_dir>/_metadata.yaml``. Pass an empty/missing path to
    skip qualification entirely.

    If ``diff_against`` points at a previous output folder, the run
    additionally writes ``diff.md`` summarising what changed between
    the two snapshots.
    """
    out_cfg = output_config or OutputConfig()
    opt_cfg = optimize_config or OptimizeConfig()
    file_cfg = file_config or FileConfig()
    union_separator = opt_cfg.union_separator

    sql_files = _select_input_files(
        input_dir,
        recursive=recursive,
        include=file_cfg.include,
        exclude=file_cfg.exclude,
    )
    if not sql_files:
        print(f"No *.sql files found in {input_dir}", file=sys.stderr)
        return 0

    metadata = load_metadata(input_dir, metadata_path)
    if metadata:
        print(
            f"Metadata loaded: {len(metadata.tables)} table(s) — "
            f"sqlglot.qualify() will run per file."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    parsed_files: List[ParsedFile] = []
    findings_per_file: Dict[str, List[QualityFinding]] = {}
    # When profiling, collect per-file elapsed seconds for parse,
    # quality, auto-fix, write phases plus a total. Surfaced in
    # report.md's `## Profile` section.
    timings_per_file: Dict[str, Dict[str, float]] = {}
    global _PROFILE_ENABLED
    _PROFILE_ENABLED = profile
    _reset_profile()

    for sql_path in sql_files:
        # Preserve subfolder structure to avoid filename collisions
        # in recursive mode. Each query gets its own folder named
        # after the SQL filename stem, under any preserved input
        # subfolder structure.
        rel = sql_path.relative_to(input_dir)
        rel_key = str(rel).replace("\\", "/")
        query_dir = output_dir / rel.parent / rel.stem

        file_t0 = time.perf_counter() if profile else 0.0

        t_parse = time.perf_counter() if profile else 0.0
        pf = parse_file(
            sql_path,
            metadata=metadata if metadata else None,
            union_separator=union_separator,
        )
        parsed_files.append(pf)
        elapsed_parse = (time.perf_counter() - t_parse) if profile else 0.0

        t_quality = time.perf_counter() if profile else 0.0
        findings = run_quality_checks(pf, customer_config=customer_config)
        elapsed_quality = (time.perf_counter() - t_quality) if profile else 0.0

        t_fix = time.perf_counter() if profile else 0.0
        optimized_sql, findings, transform_log = apply_auto_fixes(
            pf.raw_sql, findings,
            customer_config=customer_config,
            optimize_config=opt_cfg,
            detection_only=detection_only,
            entity_hint=pf.entity_name,
            order_by_annotation=pf.annotations.entity.get("order-by"),
        )
        elapsed_fix = (time.perf_counter() - t_fix) if profile else 0.0
        findings_per_file[rel_key] = findings

        if profile:
            timings_per_file[rel_key] = {
                "parse":     elapsed_parse,
                "quality":   elapsed_quality,
                "auto_fix":  elapsed_fix,
                "total":     time.perf_counter() - file_t0,
            }

        # Customer rule 8: pre-pend the conversion-summary comment
        # header, auto-filled from the TransformLog. Only when at
        # least one transform fired.
        header = emit_comment_header(pf, transform_log, output_filename="optimized.sql")
        if header:
            optimized_sql = header + "\n" + optimized_sql.lstrip()

        # Re-attach the original header annotation block (SQL-First
        # @mdde-* tags) above the optimized SQL so the metadata
        # round-trips.
        if pf.annotations.header_block.strip():
            optimized_sql = pf.annotations.header_block + "\n" + optimized_sql.lstrip()

        # Per-query folder contents (always emitted):
        write_text(query_dir / "optimized.sql", optimized_sql)
        write_text(query_dir / "genie.md", emit_genie_prompt(
            pf, customer_config=customer_config, optimize_config=opt_cfg,
        ))
        write_text(query_dir / "findings.md", emit_findings_md(pf, findings))
        meta_blacklist = customer_config.metadata_blacklist if customer_config else None
        write_text(query_dir / "movement.csv",
                   emit_movement_csv(pf, movement_config or MovementConfig(),
                                     metadata_blacklist=meta_blacklist))

        # Optional artefacts (opt-in via config).
        if out_cfg.bfm_mapping:
            write_yaml(query_dir / "mapping.bfm.yaml", emit_bfm_mapping(pf))
        if out_cfg.cte_mapping:
            write_yaml(query_dir / "mapping.cte.yaml", emit_cte_mapping(pf))
        if out_cfg.annotation_entity:
            write_yaml(query_dir / "annotation.entity.yaml", emit_entity_yaml(pf))

    # Roll-up artefacts
    lineage = emit_openlineage(parsed_files)
    with open(output_dir / "lineage.json", "w", encoding="utf-8") as f:
        json.dump(lineage, f, indent=2)

    # Run-level movement.csv: header once, every per-query row
    # concatenated in input order.
    write_text(
        output_dir / "movement.csv",
        emit_movement_csv_rollup(
            parsed_files,
            movement_config or MovementConfig(),
            metadata_blacklist=(
                customer_config.metadata_blacklist if customer_config else None
            ),
        ),
    )

    write_text(
        output_dir / "report.md",
        emit_report(
            parsed_files,
            findings_per_file,
            timings_per_file=timings_per_file if profile else None,
        ),
    )

    # Diff against a previous output snapshot if requested.
    if diff_against is not None:
        write_text(
            output_dir / "diff.md",
            emit_diff(diff_against, output_dir),
        )

    return len(parsed_files)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sql_process",
        description="Folder of SQL in, optimised SQL + mapping metadata out.",
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        nargs="?",  # Optional — fall back to run.input_dir from YAML.
        default=None,
        help=(
            "Folder containing *.sql files. Optional when "
            "`run.input_dir` is set in the YAML config."
        ),
    )
    parser.add_argument(
        "--out",
        dest="output_dir",
        type=Path,
        default=None,
        help=(
            "Output folder. Optional when `run.output_dir` is set in "
            "the YAML config; defaults to ./output otherwise."
        ),
    )
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        default=None,  # None = not specified; fall back to YAML.
        help=(
            "Recurse into subdirectories. When omitted, falls back to "
            "`run.recursive` from the YAML config (default: top-level only)."
        ),
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help=(
            "Path to a YAML config file (see "
            "sql_process.config.yaml). When omitted, the script "
            "auto-loads sql_process.config.yaml from its own folder "
            "if present. Pass 'none' to disable config-file loading."
        ),
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=None,
        help=(
            "Path to a YAML file describing source-table schemas. "
            "Enables sqlglot.qualify(). If omitted, the script looks "
            "for <input_dir>/_metadata.yaml."
        ),
    )
    parser.add_argument(
        "--diff",
        dest="diff_against",
        type=Path,
        default=None,
        help=(
            "Path to a previous output folder. When set, the run "
            "additionally writes diff.md summarising what changed."
        ),
    )
    # Movement / customer-rule overrides. Empty string means "use
    # config value". None (not passed) leaves the config value.
    parser.add_argument("--target-model", default=None,
                        help="Override movement.target_model_name from the config.")
    parser.add_argument("--source-model", default=None,
                        help="Override movement.source_model_name from the config.")
    parser.add_argument("--dependency-type", default=None,
                        help="Override movement.dependency_type from the config.")
    parser.add_argument("--granularity", default=None, choices=[None, "column", "table"],
                        help="Override movement.granularity (column or table).")
    parser.add_argument("--legacy-schemas", default=None,
                        help="Override rules.legacy_schemas (comma-separated).")
    parser.add_argument("--replacement-schema", default=None,
                        help="Override rules.replacement_schema.")
    parser.add_argument("--date-variable", default=None,
                        help="Override rules.date_variable.")
    parser.add_argument("--table-qualifier", default=None,
                        help="Override optimize.table_qualifier (empty disables).")
    parser.add_argument(
        "--detection-only",
        action="store_true",
        help=(
            "Skip every auto-fix transform — run as a pure linter. "
            "Use this as a safety fallback when an auto-fix "
            "misbehaves on real customer SQL."
        ),
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Collect per-file timing (parse / quality / auto-fix / "
            "total) and surface phase totals + the slowest 10 files "
            "in `report.md` under a `## Profile` section. Useful when "
            "scaling to thousands of files."
        ),
    )
    args = parser.parse_args(argv)

    # Load YAML config FIRST so its `run` section can supply
    # input_dir / output_dir / recursive when the CLI omits them.
    if args.config and args.config.lower() == "none":
        cfg = PipelineConfig()
    elif args.config:
        cfg = load_config(Path(args.config))
    else:
        cfg = load_config(None)

    # Resolve run-time paths. CLI > YAML > default.
    input_dir: Optional[Path] = args.input_dir or cfg.run.input_dir
    if input_dir is None:
        print(
            "No input directory: pass one as positional arg or set "
            "`run.input_dir` in the YAML config.",
            file=sys.stderr,
        )
        return 2
    if not input_dir.is_dir():
        print(f"Input folder not found: {input_dir}", file=sys.stderr)
        return 2

    output_dir: Path = (
        args.output_dir if args.output_dir is not None
        else cfg.run.output_dir if cfg.run.output_dir is not None
        else Path("output")
    )
    recursive: bool = (
        args.recursive if args.recursive is not None
        else cfg.run.recursive
    )

    # Apply CLI overrides (only when explicitly passed).
    def _split_csv(value: str) -> List[str]:
        return [s.strip() for s in value.split(",") if s.strip()]

    if args.target_model is not None:
        cfg.movement.target_model_name = args.target_model
    if args.source_model is not None:
        cfg.movement.source_model_name = args.source_model
    if args.dependency_type is not None:
        cfg.movement.dependency_type = args.dependency_type
    if args.granularity is not None:
        cfg.movement.granularity = args.granularity
    if args.legacy_schemas is not None:
        cfg.rules.legacy_schemas = _split_csv(args.legacy_schemas)
    if args.replacement_schema is not None:
        cfg.rules.replacement_schema = args.replacement_schema
    if args.date_variable is not None:
        cfg.rules.date_variable = args.date_variable
    if args.table_qualifier is not None:
        cfg.optimize.table_qualifier = args.table_qualifier or None

    n = process_folder(
        input_dir,
        output_dir,
        recursive,
        metadata_path=args.metadata,
        diff_against=args.diff_against,
        movement_config=cfg.movement,
        customer_config=cfg.rules,
        output_config=cfg.outputs,
        optimize_config=cfg.optimize,
        file_config=cfg.files,
        detection_only=args.detection_only,
        profile=args.profile,
    )

    # List the optional artefacts that were actually emitted, so the
    # banner reflects what the user actually got.
    optional_outputs = []
    if cfg.outputs.bfm_mapping:
        optional_outputs.append("mapping.bfm.yaml")
    if cfg.outputs.cte_mapping:
        optional_outputs.append("mapping.cte.yaml")
    if cfg.outputs.annotation_entity:
        optional_outputs.append("annotation.entity.yaml")

    print(f"Processed {n} file(s) -> {output_dir}")
    print(f"  {n} per-query folder(s) — each with:")
    print(f"      optimized.sql, genie.md, findings.md, movement.csv")
    if optional_outputs:
        print(f"      + {', '.join(optional_outputs)}")
    print(f"  lineage.json  OpenLineage roll-up")
    print(f"  movement.csv  Run-level mapping rollup")
    print(f"  report.md     Run summary")
    if args.diff_against is not None:
        print(f"  diff.md      Diff against {args.diff_against}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
