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
  5. Emits per-file outputs: optimised SQL with original annotations
     re-attached, two flavours of mapping YAML (BFM-shape and CTE-
     notebook-shape), and an entity YAML
  6. Rolls up cross-file lineage into an OpenLineage event JSON
  7. Writes a ``report.md`` summarising what was processed, what was
     fixed, what's still flagged, and where the mapping is incomplete

Usage::

    python sql_process.py input/ --out output/

No DuckDB. No metadata persistence between runs. Each invocation is
self-contained and idempotent — running twice on the same input
produces byte-identical output.
"""

from __future__ import annotations

import argparse
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
from sqlglot import exp


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


def parse_file(path: Path) -> ParsedFile:
    """Read a SQL file, extract annotations, parse with sqlglot."""
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


def run_quality_checks(pf: ParsedFile) -> List[QualityFinding]:
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

    diagnostics = lite_optimizer.analyze_sql(pf.raw_sql, pf.path.name)
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


def apply_auto_fixes(
    sql: str,
    findings: List[QualityFinding],
) -> Tuple[str, List[QualityFinding]]:
    """Rewrite the SQL to fix the safe issues. Mutates findings in
    place, marking the auto-fixed ones."""
    out = sql

    for f in findings:
        if f.rule == "WHERE_1_EQUALS_1":
            new = _AUTOFIX_WHERE_TRUE.sub("WHERE ", out)
            new = _AUTOFIX_LEADING_WHERE_TRUE.sub("\n", new)
            if new != out:
                out = new
                f.auto_fixed = True

    # Format-normalise via sqlglot. Preserves semantics; produces
    # consistent indentation across all output files.
    try:
        formatted = sqlglot.transpile(out, pretty=True)[0]
        # Re-attach trailing semicolon if the original had one.
        if out.rstrip().endswith(";") and not formatted.rstrip().endswith(";"):
            formatted = formatted.rstrip() + ";"
        out = formatted
    except sqlglot.errors.ParseError:
        pass

    return out, findings


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
    return {"events": events}


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
    lines.append("| File | Entity | Layer | CTEs | Sources | Output cols | Quality issues |")
    lines.append("|---|---|---|---|---|---|---|")
    for pf in parsed_files:
        findings = findings_per_file.get(pf.path.name, [])
        n_issues = len(findings)
        lines.append(
            f"| `{pf.path.name}` | {pf.entity_name} | "
            f"{pf.annotations.entity.get('layer', '?')} | "
            f"{len(pf.cte_names)} | {len(pf.source_tables)} | "
            f"{len([l for l in pf.lineage if l.output_column != '*'])} | "
            f"{n_issues} |"
        )
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
# 7. ORCHESTRATOR
# =============================================================================


import yaml  # noqa: E402


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
) -> int:
    """Run the pipeline. Returns the number of files processed."""
    pattern = "**/*.sql" if recursive else "*.sql"
    sql_files = sorted(input_dir.glob(pattern))
    if not sql_files:
        print(f"No *.sql files found in {input_dir}", file=sys.stderr)
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "optimized").mkdir(exist_ok=True)
    (output_dir / "mapping").mkdir(exist_ok=True)
    (output_dir / "annotations").mkdir(exist_ok=True)

    parsed_files: List[ParsedFile] = []
    findings_per_file: Dict[str, List[QualityFinding]] = {}

    for sql_path in sql_files:
        # Preserve subfolder structure to avoid filename collisions
        # in recursive mode.
        rel = sql_path.relative_to(input_dir)
        rel_key = str(rel).replace("\\", "/")
        rel_stem_parts = list(rel.with_suffix("").parts)

        pf = parse_file(sql_path)
        parsed_files.append(pf)

        findings = run_quality_checks(pf)
        optimized_sql, findings = apply_auto_fixes(pf.raw_sql, findings)
        findings_per_file[rel_key] = findings

        # Re-attach the original header annotation block to the
        # optimized SQL so SQL-First metadata round-trips.
        if pf.annotations.header_block.strip():
            optimized_sql = pf.annotations.header_block + "\n" + optimized_sql.lstrip()
        write_text(output_dir / "optimized" / rel, optimized_sql)

        # Mappings (both shapes), under preserved subfolders.
        mapping_dir = output_dir / "mapping" / rel.parent
        bfm_name = rel_stem_parts[-1] + ".bfm.yaml"
        cte_name = rel_stem_parts[-1] + ".cte.yaml"
        write_yaml(mapping_dir / bfm_name, emit_bfm_mapping(pf))
        write_yaml(mapping_dir / cte_name, emit_cte_mapping(pf))

        # Entity annotation YAML
        ann_name = rel_stem_parts[-1] + ".entity.yaml"
        write_yaml(output_dir / "annotations" / rel.parent / ann_name,
                   emit_entity_yaml(pf))

    # Roll-up artefacts
    lineage = emit_openlineage(parsed_files)
    with open(output_dir / "lineage.json", "w", encoding="utf-8") as f:
        json.dump(lineage, f, indent=2)

    write_text(
        output_dir / "report.md",
        emit_report(parsed_files, findings_per_file),
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
    args = parser.parse_args(argv)

    if not args.input_dir.is_dir():
        print(f"Input folder not found: {args.input_dir}", file=sys.stderr)
        return 2

    n = process_folder(args.input_dir, args.output_dir, args.recursive)
    print(f"Processed {n} file(s) -> {args.output_dir}")
    print(f"  optimized/   {n} SQL files")
    print(f"  mapping/     {n*2} YAML files (BFM + CTE shapes)")
    print(f"  annotations/ {n} entity YAML files")
    print(f"  lineage.json OpenLineage roll-up")
    print(f"  report.md    Run summary")
    return 0


if __name__ == "__main__":
    sys.exit(main())
