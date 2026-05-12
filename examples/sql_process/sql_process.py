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
    r"--\s*@mdde-(?P<key>entity|layer|stereotype|description|domain)\s*:\s*"
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


def parse_file(
    path: Path,
    metadata: Optional[MetadataSchema] = None,
) -> ParsedFile:
    """Read a SQL file, extract annotations, parse with sqlglot.

    If ``metadata`` is provided and non-empty, run sqlglot's
    ``qualify()`` pass on the AST. That resolves bare column refs to
    their owning table, expands ``SELECT *`` into explicit columns,
    and validates that referenced columns exist. On failure (e.g.
    ambiguous columns, columns not in the schema), falls back to
    the unqualified AST and records the reason in ``qualify_error``.
    """
    raw_sql = path.read_text(encoding="utf-8")
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
    except sqlglot.errors.ParseError as exc:
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
#
# Out-of-scope (would change semantics — left inline, flagged in the report):
#   - Correlated subqueries (any column reference into an outer scope)
#   - WHERE IN (SELECT ...) / WHERE EXISTS (SELECT ...) — boolean predicates,
#     not table-like; a CTE rewrite needs a join + DISTINCT that we can't
#     synthesise safely.


def _is_correlated(subq: exp.Expression, outer_scopes: List[exp.Expression]) -> bool:
    """A subquery is correlated when it references a table not defined
    inside itself. Compute the set of tables/aliases reachable from
    within the subquery, then check every Column for a table qualifier
    pointing outside that set.
    """
    inner_names: Set[str] = set()
    for tbl in subq.find_all(exp.Table):
        if tbl.alias:
            inner_names.add(tbl.alias)
        if tbl.name:
            inner_names.add(tbl.name)
    for cte in subq.find_all(exp.CTE):
        inner_names.add(cte.alias_or_name)

    for col in subq.find_all(exp.Column):
        if col.table and col.table not in inner_names:
            return True
    return False


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
    except sqlglot.errors.ParseError:
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

    if target is None or not isinstance(target, (exp.Select, exp.Union)):
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

    # Collect candidates first to avoid mutating during traversal. We
    # only lift subqueries whose direct parent is a From/Join (derived
    # table) or a Select projection (scalar subquery). Subqueries we
    # see but don't pick up here (typically inside IN/EXISTS/predicates)
    # get a SUBQUERY_NOT_LIFTED finding so the user knows the rewrite
    # was intentional, not missed.
    candidates: List[exp.Subquery] = []
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
        # Everything else (In, Exists, comparisons, etc.) — flag and
        # leave inline.
        non_candidates.append(sub)

    for sub in non_candidates:
        findings.append(QualityFinding(
            rule="SUBQUERY_NOT_LIFTED",
            severity="info",
            location="<predicate>",
            message=(
                f"Subquery inside {type(sub.parent).__name__} left inline — "
                "lifting an IN/EXISTS/comparison subquery would require "
                "synthesising a join/DISTINCT and may change row counts."
            ),
        ))

    for sub in candidates:
        # Skip predicates (IN / EXISTS in WHERE/HAVING/ON).
        if _subquery_in_predicate(sub):
            findings.append(QualityFinding(
                rule="SUBQUERY_NOT_LIFTED",
                severity="info",
                location="<predicate>",
                message=(
                    "Subquery inside WHERE/HAVING/ON predicate left inline — "
                    "lifting would require synthesising a join/DISTINCT and "
                    "may change row counts."
                ),
            ))
            continue

        # Skip correlated subqueries — moving them into a CTE changes
        # semantics because the outer reference would no longer resolve.
        if _is_correlated(sub, []):
            findings.append(QualityFinding(
                rule="SUBQUERY_NOT_LIFTED",
                severity="info",
                location="<correlated>",
                message=(
                    "Correlated subquery left inline — references an outer "
                    "scope that a CTE cannot see."
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
# Rewrites the outer SELECT so that single-table column picks, renames,
# derivations, and filters move into a per-source CTE named
# ``<table>_filtered`` (when a WHERE predicate is also pushed) or
# ``<table>_prepared`` (projections only, no filter). Matches the
# customer naming convention (CUSTOMER_RULES.md rule 7). The outer
# SELECT keeps only cross-table work
# (joins, multi-table CASE, aggregates, window functions).
#
# Skipped sources:
#   - CTEs already defined in this file's WITH — leave existing
#     CTE-shaped code alone.
#   - Tables with nothing to push (no renames, no derivations, no
#     single-table predicates) — avoid identity CTEs.


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
    """True if a projection is the kind we push down.

    Pushable: bare columns, renames, single-table derivations,
    CASE/CAST/string/arith on one table's columns.

    Not pushable: anything containing an aggregate, window function,
    or subquery — those reshape rows and must stay outer.
    """
    if any(proj.find_all(exp.AggFunc)):
        return False
    if any(proj.find_all(exp.Window)):
        return False
    if any(proj.find_all(exp.Subquery)):
        return False
    if any(proj.find_all(exp.Star)):
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
) -> Tuple[str, List[QualityFinding]]:
    """Rewrite the outermost SELECT so that single-table projections
    and filters move into ``<table>_filtered`` / ``<table>_prepared``
    CTEs (matching the customer naming convention).

    Safe-by-default: anything ambiguous, cross-table, aggregate,
    windowed, or sourced from an existing CTE is left in place.
    """
    try:
        statements = sqlglot.parse(sql, read=None)
    except sqlglot.errors.ParseError:
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

    # Existing CTE names — these are off-limits as pushdown targets.
    existing_with = target.args.get("with_")
    existing_cte_names: Set[str] = set()
    if existing_with:
        for cte in existing_with.expressions:
            existing_cte_names.add(cte.alias_or_name)

    # Collect base-table sources (table + alias) from the outer FROM
    # and JOINs. Skip references to existing CTEs. sqlglot stores
    # FROM under "from_" (trailing underscore, like "with_").
    from_clause = target.args.get("from_") or target.args.get("from")
    if not from_clause:
        return sql, findings

    sources: List[Tuple[str, str, exp.Table]] = []  # (table_name, alias, Table node)
    other_aliases: Set[str] = set()

    def collect_table(t: exp.Table) -> None:
        name = t.name
        if not name:
            return
        if name in existing_cte_names:
            other_aliases.add(t.alias or name)
            return
        alias = t.alias or name
        sources.append((name, alias, t))
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

    plans: Dict[str, PushPlan] = {
        alias: PushPlan(table_name=tn, alias=alias, table_node=tnode)
        for tn, alias, tnode in sources
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

    # ------------------------------------------------------------------
    # Apply: build per-source CTEs and rewrite the outer SELECT.
    # Naming follows the customer convention (see CUSTOMER_RULES.md
    # rule 7): ``<table>_filtered`` when a WHERE predicate is pushed,
    # ``<table>_prepared`` when only projections are pushed (no
    # filter).
    # ------------------------------------------------------------------
    new_ctes: List[exp.CTE] = []
    for plan in plans.values():
        if not (plan.projections or plan.predicates):
            continue

        # Pick a unique CTE name.
        suffix = "_filtered" if plan.predicates else "_prepared"
        base = f"{plan.table_name}{suffix}"
        cte_name = base
        counter = 2
        while cte_name in used_cte_names:
            cte_name = f"{base}_{counter}"
            counter += 1
        used_cte_names.add(cte_name)

        # Rewrite the pushed projections to drop the source alias —
        # inside the CTE, everything is one table.
        cte_projections: List[exp.Expression] = []
        for original, col_name in plan.projections:
            inner = _strip_alias(original, plan.alias)
            if isinstance(original, exp.Alias) or not _is_simple_column_named(inner, col_name):
                inner = exp.alias_(inner, col_name)
            cte_projections.append(inner)

        # Filter-only CTE (predicates pushed, no projections) — must
        # still expose columns. Default to `SELECT *` so downstream
        # references to outer columns still resolve.
        if not cte_projections:
            cte_projections.append(exp.Star())

        # Rewrite the pushed predicates similarly.
        cte_predicates = [_strip_alias(p, plan.alias) for p in plan.predicates]

        cte_select = exp.Select(expressions=cte_projections).from_(
            exp.Table(this=exp.to_identifier(plan.table_name))
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

    # Apply the outer SELECT projection rewrite.
    target.set("expressions", new_outer_projections)

    # Apply the outer WHERE rewrite (or strip the WHERE entirely).
    if where is not None:
        new_where = _rebuild_and(new_where_parts)
        if new_where is None:
            target.set("where", None)
        else:
            where.set("this", new_where)

    # Merge new CTEs into the WITH clause (preserve existing order, new
    # _proj CTEs sort first so they're declared before they're used).
    if existing_with:
        existing_with.set(
            "expressions",
            new_ctes + list(existing_with.expressions),
        )
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


def _is_simple_column_named(node: exp.Expression, name: str) -> bool:
    return isinstance(node, exp.Column) and node.name == name


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
    schema_replacements: List[Tuple[str, str]] = field(default_factory=list)
    obsolete_cte_names: List[str] = field(default_factory=list)


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
    except sqlglot.errors.ParseError:
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
    except sqlglot.errors.ParseError:
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

    if log.obsolete_ctes_removed:
        if log.obsolete_cte_names:
            obsolete = ", ".join(f"`{n}`" for n in log.obsolete_cte_names)
            summary.append(f"  - Removed obsolete CTEs ({obsolete}).")
        else:
            summary.append("  - Removed obsolete timeline-related CTEs.")
        checklist.append("- [X] Obsolete logic removed.")

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


def apply_auto_fixes(
    sql: str,
    findings: List[QualityFinding],
    customer_config: Optional["CustomerRuleConfig"] = None,
    detection_only: bool = False,
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

    if not detection_only:
        for f in findings:
            if f.rule == "WHERE_1_EQUALS_1":
                new = _AUTOFIX_WHERE_TRUE.sub("WHERE ", out)
                new = _AUTOFIX_LEADING_WHERE_TRUE.sub("\n", new)
                if new != out:
                    out = new
                    f.auto_fixed = True
                    log.where_true_removed = True

        # Customer rule 1: legacy schema replacement (opt-in via config).
        if cfg.legacy_schemas:
            out = apply_schema_replacement(
                out, findings, cfg.legacy_schemas, cfg.replacement_schema, log,
            )

        # Customer rule 2.2: legacy date-variable normalisation.
        out = apply_legacy_date_variable_replacement(out, findings, cfg.date_variable, log)

        # Customer rule 3: obsolete-CTE removal.
        out = remove_obsolete_ctes(out, findings, cfg.obsolete_cte_names, log)

        # Lift inline subqueries into named CTEs (in-scope shapes only).
        # Done before format-normalisation so the final pretty-print covers
        # the rewritten AST in one pass.
        before = out
        out, findings = lift_subqueries_to_ctes(out, findings)
        if out != before:
            log.subqueries_lifted = True

        # Push single-table projections and filters into per-source CTEs.
        # Runs after the subquery lift so any derived tables that became
        # CTEs are correctly excluded from pushdown targets.
        before = out
        out, findings = push_projections_to_source_ctes(out, findings)
        if out != before:
            log.projections_pushed = True

    # Format-normalise via sqlglot. Preserves semantics; produces
    # consistent indentation across all output files. Runs even in
    # detection-only mode so the output isn't byte-identical to the
    # input (analysts can still see something happened).
    try:
        formatted = sqlglot.transpile(out, pretty=True)[0]
        # Re-attach trailing semicolon if the original had one.
        if out.rstrip().endswith(";") and not formatted.rstrip().endswith(";"):
            formatted = formatted.rstrip() + ";"
        out = formatted
    except sqlglot.errors.ParseError:
        pass

    return out, findings, log


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


def emit_genie_prompt(pf: ParsedFile) -> str:
    """Build a concise Genie prompt from the ORIGINAL parsed query.

    The output is one self-contained markdown block — no metadata,
    no headers Genie would have to skip past. Just the instructions
    needed to reproduce the query's result.
    """
    if pf.parsed is None:
        return f"Reproduce the SQL output for `{pf.entity_name}`.\n"

    # Find the outermost SELECT (after any CTEs).
    outer = _outermost_select(pf.parsed)
    if outer is None:
        return f"Reproduce the SQL output for `{pf.entity_name}`.\n"

    lines: List[str] = []

    # 1. Goal line.
    target = pf.entity_name
    lines.append(f"Produce a result set equivalent to `{target}`.")
    lines.append("")

    # 2. Source tables (unique, in encounter order).
    if pf.source_tables:
        lines.append("Sources:")
        for src in pf.source_tables:
            lines.append(f"- `{src}`")
        lines.append("")

    # 3. Joins (only when non-trivial).
    joins = outer.args.get("joins") or []
    if joins:
        lines.append("Joins:")
        for j in joins:
            kind = (j.args.get("kind") or "").upper()
            side = (j.args.get("side") or "").upper()
            jt = " ".join(p for p in (side, kind, "JOIN") if p)
            target_tbl = _unquoted_sql(j.this) if j.this else "?"
            on = j.args.get("on")
            if on is not None:
                lines.append(f"- {jt} `{target_tbl}` on `{_unquoted_sql(on)}`")
            elif j.args.get("using"):
                using_cols = ", ".join(_unquoted_sql(u) for u in j.args["using"])
                lines.append(f"- {jt} `{target_tbl}` using ({using_cols})")
            else:
                lines.append(f"- {jt} `{target_tbl}`")
        lines.append("")

    # 4. Filters (split AND-tree into bullets).
    where = outer.args.get("where")
    if where is not None and where.this is not None:
        parts = _split_and(where.this)
        lines.append("Filters:")
        for p in parts:
            lines.append(f"- {_humanise_predicate(p)}")
        lines.append("")

    # 5. Group by / aggregation hint.
    group = outer.args.get("group")
    if group is not None:
        group_cols = ", ".join(_unquoted_sql(g) for g in group.expressions)
        lines.append(f"Group by: {group_cols}")
        lines.append("")

    # 6. Output columns (with renames + derivations spelled out).
    lines.append("Return columns:")
    for proj in outer.expressions:
        if isinstance(proj, exp.Star):
            lines.append("- all columns")
            continue
        if isinstance(proj, exp.Alias):
            inner_desc = _humanise_expression(proj.this)
            lines.append(f"- `{proj.alias}` = {inner_desc}")
        elif isinstance(proj, exp.Column):
            lines.append(f"- `{proj.name}` from {_column_label(proj)}")
        else:
            lines.append(f"- {_unquoted_sql(proj)}")
    lines.append("")

    # 7. Sort.
    order = outer.args.get("order")
    if order is not None:
        order_cols = []
        for o in order.expressions:
            direction = "DESC" if o.args.get("desc") else "ASC"
            order_cols.append(f"{_unquoted_sql(o.this)} {direction}")
        lines.append(f"Sort: {', '.join(order_cols)}")
        lines.append("")

    # 8. Limit.
    limit = outer.args.get("limit")
    if limit is not None:
        limit_expr = limit.expression if limit.expression else limit
        lines.append(f"Limit: {_unquoted_sql(limit_expr)}")
        lines.append("")

    # Trim trailing blank line.
    while lines and lines[-1] == "":
        lines.pop()
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
]


@dataclass
class MovementConfig:
    """Customer-overridable values for movement.csv.

    The three knobs the customer site asked to control: which logical
    model the target belongs to, which model the sources belong to,
    and what dependency strength to declare. Defaults match the
    customer's current spreadsheet.
    """
    target_model_name: str = "SSF"
    source_model_name: str = "SSF_SOURCE"
    dependency_type: str = "strict"


@dataclass
class CustomerRuleConfig:
    """Customer-overridable values for the migration rule pack.

    All four knobs are optional. ``legacy_schemas`` defaults to empty
    (so schema replacement is off by default; the customer opts in via
    CLI). ``date_variable`` defaults to ``process_date`` per the
    customer's master rule pack. ``metadata_blacklist`` defaults to
    the customer's published 9-column list. ``obsolete_cte_names``
    defaults to the customer's published 3-CTE blacklist.
    """
    legacy_schemas: List[str] = field(default_factory=list)
    replacement_schema: str = "automatically_inferred_qualifier"
    date_variable: str = "process_date"
    metadata_blacklist: List[str] = field(default_factory=lambda: [
        "snapshot_date", "insert_dts", "update_dts",
        "current_flag", "delete_flag", "delta_flag",
        "create_timestamp", "start_dts", "end_dts",
    ])
    obsolete_cte_names: List[str] = field(default_factory=lambda: [
        "extract_dates", "create_timeline", "finalize_timeline",
    ])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "legacy_schemas": list(self.legacy_schemas),
            "replacement_schema": self.replacement_schema,
            "date_variable": self.date_variable,
            "metadata_blacklist": list(self.metadata_blacklist),
            "obsolete_cte_names": list(self.obsolete_cte_names),
        }


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


def _resolve_source_table(
    alias_or_name: str,
    alias_map: Dict[str, str],
) -> str:
    """Resolve a FROM-side alias to the underlying table name."""
    if not alias_or_name:
        return ""
    return alias_map.get(alias_or_name, alias_or_name)


def _build_alias_map(pf: ParsedFile) -> Dict[str, str]:
    """Build an ``alias -> source_table`` map by walking the parsed
    AST. Used to resolve qualified column refs (``c.email``) back to
    their table (``stg_customers``).

    Falls back to identity (alias -> alias) when the alias is itself
    a base-table name without a separate alias clause.
    """
    out: Dict[str, str] = {}
    if pf.parsed is None:
        return out
    for tbl in pf.parsed.find_all(exp.Table):
        name = tbl.name
        if not name:
            continue
        alias = tbl.alias or name
        # Build qualified name when schema/catalog is present (so the
        # CSV row carries the same qualified name the rest of the
        # pipeline does).
        parts: List[str] = []
        if tbl.args.get("catalog"):
            parts.append(tbl.args["catalog"].name)
        if tbl.args.get("db"):
            parts.append(tbl.args["db"].name)
        parts.append(name)
        out[alias] = ".".join(parts)
    return out


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
    except sqlglot.errors.ParseError:
        pass
    # Collapse runs of whitespace.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def emit_movement_csv_rows(
    pf: ParsedFile,
    config: MovementConfig,
) -> List[List[str]]:
    """Build the per-query movement.csv rows for ``pf``.

    One row per ``(target_column, source_column)`` pair from the
    lineage extraction, plus one row per join-only source table
    (a table referenced in FROM/JOIN but contributing no projection).
    """
    rows: List[List[str]] = []
    target_table = _target_table_name(pf)
    alias_map = _build_alias_map(pf)

    sources_with_projections: Set[str] = set()

    for lin in pf.lineage:
        if lin.output_column == "*":
            continue
        derived = "true" if _is_derived_lineage(lin) else "false"
        expression = _clean_movement_expression(lin.expression)
        if lin.source_columns:
            for tbl, col in lin.source_columns:
                source_table = _resolve_source_table(tbl, alias_map)
                sources_with_projections.add(source_table)
                rows.append([
                    config.target_model_name,
                    target_table,
                    lin.output_column,
                    config.source_model_name,
                    source_table,
                    col,
                    derived,
                    expression,
                    config.dependency_type,
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
            ])

    # Join-only sources: tables referenced via FROM/JOIN that no
    # projection actually reads. Emit one row with empty column slots
    # so the dependency is still recorded.
    for src in pf.source_tables:
        if src in sources_with_projections:
            continue
        # Also check the unqualified last segment — alias_map values
        # may carry qualifiers (catalog.db.table) while lineage
        # source_columns use the bare alias.
        if src.split(".")[-1] in sources_with_projections:
            continue
        rows.append([
            config.target_model_name,
            target_table,
            "",
            config.source_model_name,
            src,
            "",
            "false",
            "",
            config.dependency_type,
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


def emit_movement_csv(pf: ParsedFile, config: MovementConfig) -> str:
    """Per-query movement.csv text."""
    return _rows_to_csv(emit_movement_csv_rows(pf, config))


def emit_movement_csv_rollup(
    parsed_files: List[ParsedFile],
    config: MovementConfig,
) -> str:
    """Run-level rollup: header once, all per-query rows concatenated
    in input order."""
    all_rows: List[List[str]] = []
    for pf in parsed_files:
        if pf.parse_error:
            continue
        all_rows.extend(emit_movement_csv_rows(pf, config))
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
) -> str:
    """Markdown summary of the run.

    Timestamp is omitted so the report is byte-identical across
    re-runs on the same input. CI consumers can stamp it externally
    if they need provenance.
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


def process_folder(
    input_dir: Path,
    output_dir: Path,
    recursive: bool = False,
    metadata_path: Optional[Path] = None,
    diff_against: Optional[Path] = None,
    movement_config: Optional[MovementConfig] = None,
    customer_config: Optional[CustomerRuleConfig] = None,
    detection_only: bool = False,
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
    pattern = "**/*.sql" if recursive else "*.sql"
    sql_files = sorted(input_dir.glob(pattern))
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

    for sql_path in sql_files:
        # Preserve subfolder structure to avoid filename collisions
        # in recursive mode. Each query gets its own folder named
        # after the SQL filename stem, under any preserved input
        # subfolder structure.
        rel = sql_path.relative_to(input_dir)
        rel_key = str(rel).replace("\\", "/")
        query_dir = output_dir / rel.parent / rel.stem

        pf = parse_file(sql_path, metadata=metadata if metadata else None)
        parsed_files.append(pf)

        findings = run_quality_checks(pf, customer_config=customer_config)
        optimized_sql, findings, transform_log = apply_auto_fixes(
            pf.raw_sql, findings,
            customer_config=customer_config,
            detection_only=detection_only,
        )
        findings_per_file[rel_key] = findings

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

        # Per-query folder contents:
        #   optimized.sql            — rewritten SQL with header annotations
        #   mapping.bfm.yaml         — Business-Friendly Mapping shape
        #   mapping.cte.yaml         — CTE-notebook shape
        #   annotation.entity.yaml   — SQL-First entity YAML
        #   genie.md                 — Genie prompt from the ORIGINAL query
        #   findings.md              — this file's quality findings
        write_text(query_dir / "optimized.sql", optimized_sql)
        write_yaml(query_dir / "mapping.bfm.yaml", emit_bfm_mapping(pf))
        write_yaml(query_dir / "mapping.cte.yaml", emit_cte_mapping(pf))
        write_yaml(query_dir / "annotation.entity.yaml", emit_entity_yaml(pf))
        write_text(query_dir / "genie.md", emit_genie_prompt(pf))
        write_text(query_dir / "findings.md", emit_findings_md(pf, findings))
        write_text(query_dir / "movement.csv",
                   emit_movement_csv(pf, movement_config or MovementConfig()))

    # Roll-up artefacts
    lineage = emit_openlineage(parsed_files)
    with open(output_dir / "lineage.json", "w", encoding="utf-8") as f:
        json.dump(lineage, f, indent=2)

    # Run-level movement.csv: header once, every per-query row
    # concatenated in input order.
    write_text(
        output_dir / "movement.csv",
        emit_movement_csv_rollup(parsed_files, movement_config or MovementConfig()),
    )

    write_text(
        output_dir / "report.md",
        emit_report(parsed_files, findings_per_file),
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
        help="Folder containing *.sql files",
    )
    parser.add_argument(
        "--out",
        dest="output_dir",
        type=Path,
        default=Path("output"),
        help="Output folder (default: ./output)",
    )
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Recurse into subdirectories (default: top-level only)",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=None,
        help=(
            "Path to a YAML file describing source-table schemas "
            "(see README for format). Enables sqlglot.qualify() — "
            "resolves bare column refs to their owning table and "
            "expands SELECT * into explicit column lists. If omitted, "
            "the script looks for <input_dir>/_metadata.yaml."
        ),
    )
    parser.add_argument(
        "--diff",
        dest="diff_against",
        type=Path,
        default=None,
        help=(
            "Path to a previous output folder. When set, the run "
            "additionally writes diff.md summarising what changed "
            "between the two snapshots (added/removed files, mapping "
            "column changes, stitching edge changes)."
        ),
    )
    parser.add_argument(
        "--target-model",
        default="SSF",
        help=(
            "target_model_name value in every movement.csv row. "
            "Default: SSF."
        ),
    )
    parser.add_argument(
        "--source-model",
        default="SSF_SOURCE",
        help=(
            "source_model_name value in every movement.csv row. "
            "Default: SSF_SOURCE."
        ),
    )
    parser.add_argument(
        "--dependency-type",
        default="strict",
        help=(
            "dependency_type value in every movement.csv row. "
            "Default: strict."
        ),
    )
    parser.add_argument(
        "--legacy-schemas",
        default="",
        help=(
            "Comma-separated list of legacy schema names to replace "
            "(rule 1). Off by default; example: "
            "--legacy-schemas bodm,csz,hz,cz"
        ),
    )
    parser.add_argument(
        "--replacement-schema",
        default="automatically_inferred_qualifier",
        help=(
            "Schema name to substitute for any legacy schema match "
            "(rule 1). Default: automatically_inferred_qualifier"
        ),
    )
    parser.add_argument(
        "--date-variable",
        default="process_date",
        help=(
            "Template variable name to use for SCD2 point-in-time "
            "predicates (rule 2). Legacy `{reporting_date}` is "
            "auto-rewritten to this. Default: process_date"
        ),
    )
    parser.add_argument(
        "--metadata-blacklist",
        default=(
            "snapshot_date,insert_dts,update_dts,current_flag,"
            "delete_flag,delta_flag,create_timestamp,start_dts,end_dts"
        ),
        help=(
            "Comma-separated list of metadata column names that must "
            "not appear in CTE/final outputs (rule 4)."
        ),
    )
    parser.add_argument(
        "--obsolete-ctes",
        default="extract_dates,create_timeline,finalize_timeline",
        help=(
            "Comma-separated list of CTE names to treat as obsolete "
            "and auto-remove (rule 3)."
        ),
    )
    parser.add_argument(
        "--detection-only",
        action="store_true",
        help=(
            "Skip every auto-fix transform — run as a pure linter. "
            "The optimised SQL is just the format-normalised input; "
            "findings.md and movement.csv still emit. Use this as a "
            "safety fallback when an auto-fix misbehaves on real "
            "customer SQL."
        ),
    )
    args = parser.parse_args(argv)

    if not args.input_dir.is_dir():
        print(f"Input folder not found: {args.input_dir}", file=sys.stderr)
        return 2

    movement_config = MovementConfig(
        target_model_name=args.target_model,
        source_model_name=args.source_model,
        dependency_type=args.dependency_type,
    )

    def _split_csv(value: str) -> List[str]:
        return [s.strip() for s in value.split(",") if s.strip()]

    customer_config = CustomerRuleConfig(
        legacy_schemas=_split_csv(args.legacy_schemas),
        replacement_schema=args.replacement_schema,
        date_variable=args.date_variable,
        metadata_blacklist=_split_csv(args.metadata_blacklist),
        obsolete_cte_names=_split_csv(args.obsolete_ctes),
    )

    n = process_folder(
        args.input_dir,
        args.output_dir,
        args.recursive,
        metadata_path=args.metadata,
        diff_against=args.diff_against,
        movement_config=movement_config,
        customer_config=customer_config,
        detection_only=args.detection_only,
    )
    print(f"Processed {n} file(s) -> {args.output_dir}")
    print(f"  {n} per-query folder(s) — each with:")
    print(f"      optimized.sql, mapping.bfm.yaml, mapping.cte.yaml,")
    print(f"      annotation.entity.yaml, genie.md, findings.md,")
    print(f"      movement.csv")
    print(f"  lineage.json  OpenLineage roll-up")
    print(f"  movement.csv  Run-level mapping rollup")
    print(f"  report.md     Run summary")
    if args.diff_against is not None:
        print(f"  diff.md      Diff against {args.diff_against}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
