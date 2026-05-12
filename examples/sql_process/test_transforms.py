"""Edge-case tests for the customer rule-pack auto-fix transforms.

Covers four transforms:
1. apply_schema_replacement
2. apply_legacy_date_variable_replacement
3. remove_obsolete_ctes
4. emit_comment_header (transform log scoping)

Run with: pytest test_transforms.py -v
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import pytest

# Make sibling modules importable when pytest is invoked from a
# different cwd.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from sql_process import (  # noqa: E402
    CustomerRuleConfig,
    FileAnnotations,
    ParsedFile,
    QualityFinding,
    TransformLog,
    apply_auto_fixes,
    apply_legacy_date_variable_replacement,
    apply_schema_replacement,
    emit_comment_header,
    remove_obsolete_ctes,
)


# =============================================================================
# Helpers
# =============================================================================


def _parsed_file(filename: str = "test.sql") -> ParsedFile:
    """Minimal ParsedFile for tests that only need ``path`` to be set."""
    return ParsedFile(
        path=Path(filename),
        raw_sql="",
        entity_name="test",
        annotations=FileAnnotations(),
    )


def _strip(sql: str) -> str:
    """Whitespace-normalise SQL for robust string comparison."""
    return " ".join(sql.split())


# =============================================================================
# 1. Schema replacement
# =============================================================================


class TestSchemaReplacement:

    def test_simple_replacement(self):
        sql = "SELECT id FROM bodm.customers"
        log = TransformLog()
        out = apply_schema_replacement(
            sql, [], ["bodm"], "new_schema", log,
        )
        assert "new_schema.customers" in out
        assert "bodm.customers" not in out
        assert log.schema_replaced is True
        assert log.schema_replacements == [("bodm", "new_schema")]

    def test_multiple_legacy_schemas(self):
        """Multiple distinct legacy schemas in one query — all replaced."""
        sql = """
        SELECT c.id, o.total
        FROM bodm.customers c
        JOIN csz.orders o ON o.cid = c.id
        """
        log = TransformLog()
        out = apply_schema_replacement(
            sql, [], ["bodm", "csz", "hz", "cz"], "aiq", log,
        )
        assert "aiq.customers" in out
        assert "aiq.orders" in out
        assert log.schema_replaced is True
        assert set(log.schema_replacements) == {("bodm", "aiq"), ("csz", "aiq")}

    def test_same_schema_used_twice(self):
        """Same legacy schema referenced from two tables — deduplicated."""
        sql = "SELECT * FROM bodm.a JOIN bodm.b ON a.id = b.id"
        log = TransformLog()
        out = apply_schema_replacement(sql, [], ["bodm"], "aiq", log)
        assert _strip(out).count("aiq.") == 2
        assert log.schema_replacements == [("bodm", "aiq")]

    def test_no_match_is_noop(self):
        """When no table uses a legacy schema, output is unchanged."""
        sql = "SELECT id FROM modern.customers"
        log = TransformLog()
        out = apply_schema_replacement(sql, [], ["bodm"], "aiq", log)
        # AST-rendered SQL is allowed to differ in whitespace; check
        # that the table reference is unchanged and the log is empty.
        assert "modern.customers" in out
        assert log.schema_replaced is False

    def test_empty_legacy_list_skipped(self):
        """Empty legacy list returns input verbatim — opt-in behaviour."""
        sql = "SELECT id FROM bodm.customers"
        log = TransformLog()
        out = apply_schema_replacement(sql, [], [], "aiq", log)
        assert out == sql  # Truly unchanged, not just AST-equivalent
        assert log.schema_replaced is False

    def test_parse_error_falls_through_silently(self):
        """Garbage input doesn't crash — returns unchanged string."""
        sql = "this is not SQL at all"
        log = TransformLog()
        out = apply_schema_replacement(sql, [], ["bodm"], "aiq", log)
        assert out == sql
        assert log.schema_replaced is False

    def test_finding_marked_autofixed(self):
        """Matching LEGACY_SCHEMA findings get auto_fixed=True."""
        sql = "SELECT id FROM bodm.customers"
        log = TransformLog()
        findings = [
            QualityFinding(
                rule="LEGACY_SCHEMA",
                severity="warning",
                location="<file>",
                message="Legacy schema 'bodm'",
            ),
            QualityFinding(
                rule="OTHER_RULE",
                severity="info",
                location="<file>",
                message="unrelated",
            ),
        ]
        apply_schema_replacement(sql, findings, ["bodm"], "aiq", log)
        assert findings[0].auto_fixed is True
        assert findings[1].auto_fixed is False  # Unrelated rule untouched


# =============================================================================
# 2. Legacy date variable replacement
# =============================================================================


class TestLegacyDateVariableReplacement:

    def test_simple_replacement(self):
        sql = "WHERE CAST('{reporting_date}' AS DATE) >= _valid_from"
        log = TransformLog()
        out = apply_legacy_date_variable_replacement(
            sql, [], "process_date", log,
        )
        assert "{process_date}" in out
        assert "{reporting_date}" not in out
        assert log.legacy_date_replaced is True

    def test_multiple_occurrences(self):
        """All occurrences replaced, not just the first."""
        sql = """
        WHERE CAST('{reporting_date}' AS DATE) >= _valid_from
          AND CAST('{reporting_date}' AS DATE) <  COALESCE(_valid_to, ...)
        """
        log = TransformLog()
        out = apply_legacy_date_variable_replacement(
            sql, [], "process_date", log,
        )
        assert out.count("{process_date}") == 2
        assert "{reporting_date}" not in out

    def test_target_already_present_skipped(self):
        """When the target is already the legacy name (configured),
        no replacement happens — avoids infinite loop / self-rewrite."""
        sql = "WHERE CAST('{reporting_date}' AS DATE) >= _valid_from"
        log = TransformLog()
        # Configured target IS 'reporting_date' — leave it alone.
        out = apply_legacy_date_variable_replacement(
            sql, [], "reporting_date", log,
        )
        assert out == sql
        assert log.legacy_date_replaced is False

    def test_no_legacy_present_is_noop(self):
        sql = "WHERE CAST('{process_date}' AS DATE) >= _valid_from"
        log = TransformLog()
        out = apply_legacy_date_variable_replacement(
            sql, [], "process_date", log,
        )
        assert out == sql
        assert log.legacy_date_replaced is False

    def test_partial_match_not_replaced(self):
        """`{reporting_date_extended}` should NOT be rewritten —
        only exact `{reporting_date}` match."""
        sql = "WHERE x = '{reporting_date_extended}'"
        log = TransformLog()
        out = apply_legacy_date_variable_replacement(
            sql, [], "process_date", log,
        )
        # Current implementation uses substring match, which IS a
        # potential issue. Document the actual behaviour so we know.
        # The needle is "{reporting_date}" which is NOT a substring
        # of "{reporting_date_extended}" because of the closing brace.
        assert out == sql
        assert log.legacy_date_replaced is False

    def test_finding_marked_autofixed(self):
        sql = "WHERE x = '{reporting_date}'"
        log = TransformLog()
        findings = [
            QualityFinding(
                rule="LEGACY_DATE_VARIABLE",
                severity="warning",
                location="<file>",
                message="Legacy date variable",
            ),
        ]
        apply_legacy_date_variable_replacement(sql, findings, "process_date", log)
        assert findings[0].auto_fixed is True


# =============================================================================
# 3. Obsolete CTE removal
# =============================================================================


class TestRemoveObsoleteCtes:

    def test_simple_removal(self):
        sql = """
        WITH
        extract_dates AS (SELECT id FROM base),
        kept AS (SELECT id FROM base)
        SELECT * FROM kept
        """
        log = TransformLog()
        out = remove_obsolete_ctes(
            sql, [], ["extract_dates"], log,
        )
        assert "extract_dates" not in _strip(out)
        assert "kept" in out
        assert log.obsolete_ctes_removed is True
        assert log.obsolete_cte_names == ["extract_dates"]

    def test_preserved_when_referenced_downstream(self):
        """Obsolete CTE that's actually used downstream is preserved
        — conservative behaviour to avoid breaking the query."""
        sql = """
        WITH
        extract_dates AS (SELECT id, dt FROM base),
        downstream AS (SELECT * FROM extract_dates WHERE dt > 0)
        SELECT * FROM downstream
        """
        log = TransformLog()
        out = remove_obsolete_ctes(
            sql, [], ["extract_dates"], log,
        )
        # The CTE is referenced by `downstream` so we must keep it.
        assert "extract_dates" in _strip(out)
        assert log.obsolete_ctes_removed is False

    def test_referenced_in_final_select_preserved(self):
        """Obsolete CTE referenced in the final SELECT is preserved."""
        sql = """
        WITH
        extract_dates AS (SELECT id FROM base),
        other AS (SELECT * FROM base)
        SELECT * FROM extract_dates
        """
        log = TransformLog()
        out = remove_obsolete_ctes(
            sql, [], ["extract_dates"], log,
        )
        assert "extract_dates" in _strip(out)
        assert log.obsolete_ctes_removed is False

    def test_multiple_obsolete_all_removed(self):
        sql = """
        WITH
        extract_dates AS (SELECT id FROM base),
        create_timeline AS (SELECT id FROM base),
        finalize_timeline AS (SELECT id FROM base),
        kept AS (SELECT id FROM base)
        SELECT * FROM kept
        """
        log = TransformLog()
        out = remove_obsolete_ctes(
            sql, [], ["extract_dates", "create_timeline", "finalize_timeline"], log,
        )
        stripped = _strip(out)
        assert "extract_dates" not in stripped
        assert "create_timeline" not in stripped
        assert "finalize_timeline" not in stripped
        assert "kept" in stripped
        assert set(log.obsolete_cte_names) == {
            "extract_dates", "create_timeline", "finalize_timeline",
        }

    def test_empty_blacklist_is_noop(self):
        sql = "WITH extract_dates AS (SELECT 1) SELECT * FROM extract_dates"
        log = TransformLog()
        out = remove_obsolete_ctes(sql, [], [], log)
        assert out == sql
        assert log.obsolete_ctes_removed is False

    def test_obsolete_referenced_only_by_another_obsolete(self):
        """If A is obsolete and only B (also obsolete) references it,
        and B isn't used downstream, both should be removable.

        Note: current implementation processes one CTE at a time and
        may not handle this transitive case. This test documents the
        behaviour."""
        sql = """
        WITH
        extract_dates AS (SELECT id FROM base),
        create_timeline AS (SELECT * FROM extract_dates),
        kept AS (SELECT 1 AS x)
        SELECT * FROM kept
        """
        log = TransformLog()
        out = remove_obsolete_ctes(
            sql, [], ["extract_dates", "create_timeline"], log,
        )
        # Both should ideally be removed since neither is used
        # downstream. Current implementation: create_timeline references
        # extract_dates so extract_dates is preserved (the check sees
        # the sibling reference). Document actual behaviour.
        # Both ARE obsolete-listed so they get checked together —
        # ideally both removed.
        assert log.obsolete_ctes_removed is True
        # At minimum create_timeline should be gone (no downstream ref).
        assert "create_timeline" not in _strip(out)

    def test_parse_error_falls_through(self):
        sql = "this is not SQL"
        log = TransformLog()
        out = remove_obsolete_ctes(sql, [], ["extract_dates"], log)
        assert out == sql
        assert log.obsolete_ctes_removed is False


# =============================================================================
# 4. Comment header
# =============================================================================


class TestCommentHeader:

    def test_no_transforms_returns_empty(self):
        pf = _parsed_file("test.sql")
        log = TransformLog()  # nothing fired
        out = emit_comment_header(pf, log, "optimized.sql")
        assert out == ""

    def test_schema_replacement_only(self):
        pf = _parsed_file("test.sql")
        log = TransformLog()
        log.schema_replaced = True
        log.schema_replacements = [("bodm", "aiq")]
        out = emit_comment_header(pf, log, "optimized.sql")
        assert "Migration Details:" in out
        assert "Original SQL File: test.sql" in out
        assert "Target SQL File:" in out
        assert "Replaced legacy schemas" in out
        assert "[X] Schema replacement completed." in out
        # Other transforms not present — their bullets must not appear.
        assert "Pushed single-table" not in out
        assert "Lifted inline subqueries" not in out
        assert "Removed obsolete CTEs" not in out

    def test_all_transforms_fired(self):
        pf = _parsed_file("test.sql")
        log = TransformLog()
        log.schema_replaced = True
        log.schema_replacements = [("bodm", "aiq")]
        log.legacy_date_replaced = True
        log.subqueries_lifted = True
        log.projections_pushed = True
        log.obsolete_ctes_removed = True
        log.obsolete_cte_names = ["extract_dates"]
        log.where_true_removed = True
        out = emit_comment_header(pf, log, "optimized.sql")
        # All 5 distinct sections should appear (where_true is part
        # of the summary but has no separate checklist entry).
        assert "Replaced legacy schemas" in out
        assert "Replaced legacy `{reporting_date}`" in out
        assert "Lifted inline subqueries" in out
        assert "Pushed single-table projections" in out
        assert "Removed obsolete CTEs (`extract_dates`)" in out
        assert "Removed `WHERE 1=1`" in out
        # Validation checklist with 4 entries (where_true doesn't get one).
        for entry in [
            "Schema replacement completed",
            "Date variable normalised",
            "Subqueries encapsulated",
            "Modular CTE structure",
            "Obsolete logic removed",
        ]:
            assert entry in out

    def test_header_wrapped_in_block_comment(self):
        pf = _parsed_file("test.sql")
        log = TransformLog()
        log.projections_pushed = True
        out = emit_comment_header(pf, log, "optimized.sql")
        assert out.startswith("/*")
        assert out.rstrip().endswith("*/")

    def test_filename_propagated_correctly(self):
        pf = _parsed_file("very_specific_name.sql")
        log = TransformLog()
        log.projections_pushed = True
        out = emit_comment_header(pf, log, "renamed_output.sql")
        assert "Original SQL File: very_specific_name.sql" in out
        assert "Target SQL File:  renamed_output.sql" in out


# =============================================================================
# 5. End-to-end apply_auto_fixes — integration
# =============================================================================


class TestApplyAutoFixesIntegration:

    def test_detection_only_mode_skips_all_transforms(self):
        """When detection_only=True, no auto-fix runs even if config
        would normally trigger one. Output is just format-normalised."""
        sql = """
        SELECT id FROM bodm.customers
        WHERE CAST('{reporting_date}' AS DATE) >= _valid_from
        """
        cfg = CustomerRuleConfig(legacy_schemas=["bodm"])
        out, _, log = apply_auto_fixes(sql, [], cfg, detection_only=True)
        # All transform flags should be False
        assert log.schema_replaced is False
        assert log.legacy_date_replaced is False
        assert log.projections_pushed is False
        # Original strings preserved
        assert "bodm" in out
        assert "{reporting_date}" in out

    def test_full_pipeline_chains_transforms(self):
        """Schema replacement + date variable + format-normalise all
        fire on the same input without breaking each other."""
        sql = """
        SELECT customer_id FROM bodm.customers
        WHERE CAST('{reporting_date}' AS DATE) >= _valid_from
        """
        cfg = CustomerRuleConfig(legacy_schemas=["bodm"])
        out, _, log = apply_auto_fixes(sql, [], cfg)
        assert log.schema_replaced is True
        assert log.legacy_date_replaced is True
        assert "automatically_inferred_qualifier" in out
        assert "{process_date}" in out
        assert "bodm" not in out
        assert "{reporting_date}" not in out

    def test_idempotency_run_twice(self):
        """Running apply_auto_fixes twice on the same input should
        produce byte-identical output the second time."""
        sql = """
        WITH foo AS (SELECT id FROM bodm.customers)
        SELECT * FROM foo
        """
        cfg = CustomerRuleConfig(legacy_schemas=["bodm"])
        out1, _, _ = apply_auto_fixes(sql, [], cfg)
        out2, _, _ = apply_auto_fixes(out1, [], cfg)
        assert out1 == out2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
