"""
End-to-end demo: parse SQL -> tag with layer/stereotype -> generate dbt project -> generate diagrams.

Run from repo root:
    python examples/sales_layered/run_demo.py

This complements the basic parser/optimizer demos by showing the full
metadata pipeline that downstream generators (dbt, diagrams) depend on.
"""
import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

from src.mdde_lite.schema import create_schema
from src.mdde_lite.parser import parse_directory
from src.mdde_lite.optimizer import analyze_directory
from src.mdde_lite.dbt_generator import generate_dbt_project
from src.mdde_lite.diagrams import generate_erd, generate_dataflow

OUTPUT_DIR = Path("generated/sales_layered")
EXAMPLES_DIR = "examples/sales_layered"
DB_PATH = OUTPUT_DIR / "metadata.duckdb"


LAYER_RULES = [
    ("raw_",  "source",      "src_raw"),
    ("stg_",  "staging",     "stg_cleaned"),
    ("int_",  "integration", "int_master"),
    ("dim_",  "business",    "dim_scd2"),
    ("fact_", "business",    "fact"),
]


def tag_entities_by_naming_convention(conn):
    """Apply layer + stereotype to entities based on filename prefix."""
    updated = 0
    for prefix, layer, stereotype in LAYER_RULES:
        result = conn.execute("""
            UPDATE entity
            SET layer = ?, stereotype = ?
            WHERE name LIKE ?
        """, [layer, stereotype, f"{prefix}%"])
        # DuckDB's execute().fetchone() returns row count for DML in some versions
    rows = conn.execute("""
        SELECT layer, stereotype, name FROM entity ORDER BY layer, name
    """).fetchall()
    return rows


def main():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    print("=" * 60)
    print("MDDE Lite — Layered Sales Demo")
    print("=" * 60)

    print(f"\n[1/5] Creating metadata schema at {DB_PATH}")
    conn = create_schema(str(DB_PATH))

    print(f"\n[2/5] Parsing SQL in {EXAMPLES_DIR}")
    parse_directory(EXAMPLES_DIR, str(DB_PATH))

    print("\n[3/5] Tagging entities by naming convention "
          "(raw_/stg_/int_/dim_/fact_)")
    rows = tag_entities_by_naming_convention(conn)
    for layer, stereotype, name in rows:
        print(f"      {layer or '(none)':12} {stereotype or '(none)':12} {name}")

    print("\n[4/5] Generating dbt project")
    stats = generate_dbt_project(
        conn,
        str(OUTPUT_DIR / "dbt"),
        "sales_layered_demo"
    )
    print(f"      models_generated   = {stats['models_generated']}")
    print(f"      sources_generated  = {stats['sources_generated']}")
    print(f"      schema_files       = {stats['schema_files']}")

    print("\n[5/5] Generating Mermaid ERD + dataflow")
    erd = generate_erd(conn)
    flow = generate_dataflow(conn)
    (OUTPUT_DIR / "erd.md").write_text(
        f"# Sales Layered ERD\n\n```mermaid\n{erd}\n```\n",
        encoding="utf-8"
    )
    (OUTPUT_DIR / "dataflow.md").write_text(
        f"# Sales Layered Dataflow\n\n```mermaid\n{flow}\n```\n",
        encoding="utf-8"
    )

    print("\nDone.")
    print(f"  dbt project:    {OUTPUT_DIR / 'dbt'}")
    print(f"  erd.md:         {OUTPUT_DIR / 'erd.md'}")
    print(f"  dataflow.md:    {OUTPUT_DIR / 'dataflow.md'}")
    print(f"  metadata.duckdb {DB_PATH}")


if __name__ == "__main__":
    main()
