# Databricks notebook source
# MAGIC %md
# MAGIC # sql_process — Databricks notebook
# MAGIC
# MAGIC Reads every `*.sql` file in an input folder and produces:
# MAGIC
# MAGIC - **Optimised SQL** (quality-checked, auto-fixed where safe, format-normalised)
# MAGIC - **Mapping metadata** in two YAML flavours per file (BFM + CTE-notebook shapes)
# MAGIC - **Annotations** YAML per file (SQL-First metadata promoted to entity shape)
# MAGIC - **OpenLineage** roll-up across all files (`lineage.json`)
# MAGIC - **Report** (`report.md`) summarising findings, auto-fixes, and mapping coverage
# MAGIC
# MAGIC ## Setup
# MAGIC
# MAGIC This notebook expects the following files in the same Workspace folder:
# MAGIC
# MAGIC - `sql_process_databricks.py` (this notebook)
# MAGIC - `sql_process.py` (the main pipeline)
# MAGIC - `_optimizer.py` (vendored, no DuckDB)
# MAGIC - `_determinism.py` (vendored, no DuckDB)
# MAGIC
# MAGIC ### Getting the files into Databricks
# MAGIC
# MAGIC Three options, in order of ease:
# MAGIC
# MAGIC 1. **Repos**: clone `mdde-demo` via the Repos UI and navigate to `examples/sql_process/`
# MAGIC 2. **Workspace upload**: upload the four files into a Workspace folder
# MAGIC 3. **Volumes / DBFS**: place under `/Volumes/<catalog>/<schema>/<volume>/sql_process/` and adjust `module_dir` below

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install dependencies
# MAGIC
# MAGIC Two packages: `sqlglot` (SQL parser) and `pyyaml` (output emit). Both light, no native code.

# COMMAND ----------

# MAGIC %pip install sqlglot pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Locate the module folder
# MAGIC
# MAGIC The notebook needs `sql_process.py` + `_optimizer.py` + `_determinism.py` on
# MAGIC `sys.path` so it can import them. By default we look in the same folder as
# MAGIC this notebook. Override `module_dir` if you put the files elsewhere.

# COMMAND ----------

import os
import sys

# Default: same folder as this notebook (works when files are in the same
# Workspace folder, or in the same Repos folder).
module_dir = os.path.dirname(os.path.abspath("."))

# Override if your files are elsewhere — e.g.:
# module_dir = "/Workspace/Users/you@example.com/sql_process"
# module_dir = "/Volumes/main/default/code/sql_process"
# module_dir = "/Workspace/Repos/you@example.com/mdde-demo/examples/sql_process"

if module_dir not in sys.path:
    sys.path.insert(0, module_dir)

print(f"Module folder: {module_dir}")
print(f"Files present: {sorted(f for f in os.listdir(module_dir) if not f.startswith('.'))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure input + output paths
# MAGIC
# MAGIC `input_dir` should contain `*.sql` files. `output_dir` will be created if
# MAGIC it doesn't exist. Both can be:
# MAGIC
# MAGIC - DBFS-mounted paths (`/dbfs/mnt/...`)
# MAGIC - Volumes paths (`/Volumes/<catalog>/<schema>/<volume>/...`)
# MAGIC - Workspace files (`/Workspace/Users/.../sql_dir/`)
# MAGIC - Local cluster paths (`/tmp/...`) — fine for a one-off demo, ephemeral
# MAGIC
# MAGIC Edit the two cells below to point at your data.

# COMMAND ----------

# Use widgets so you can drive the notebook as a Databricks Job too.
dbutils.widgets.text("input_dir", f"{module_dir}/input", "Input folder (SQL files)")
dbutils.widgets.text("output_dir", "/tmp/sql_process_output", "Output folder")
dbutils.widgets.dropdown("recursive", "false", ["true", "false"],
                         "Recurse into subdirs")
dbutils.widgets.text(
    "metadata_path", "",
    "Metadata YAML (optional, blank = auto-discover input/_metadata.yaml)",
)

input_dir = dbutils.widgets.get("input_dir")
output_dir = dbutils.widgets.get("output_dir")
recursive = dbutils.widgets.get("recursive") == "true"
metadata_path = dbutils.widgets.get("metadata_path") or None

print(f"Input:     {input_dir}")
print(f"Output:    {output_dir}")
print(f"Recursive: {recursive}")
print(f"Metadata:  {metadata_path or '(auto-discover)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run the pipeline

# COMMAND ----------

from pathlib import Path
from sql_process import process_folder

n = process_folder(
    Path(input_dir),
    Path(output_dir),
    recursive=recursive,
    metadata_path=Path(metadata_path) if metadata_path else None,
)
print(f"Processed {n} file(s) -> {output_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspect the run report
# MAGIC
# MAGIC Renders `output_dir/report.md` inline so you don't need to navigate
# MAGIC into the file system to see what happened.

# COMMAND ----------

report_path = Path(output_dir) / "report.md"
if report_path.exists():
    displayHTML(
        f"<div style='font-family:sans-serif; padding:1em'>"
        f"<pre style='white-space:pre-wrap'>{report_path.read_text(encoding='utf-8')}</pre>"
        f"</div>"
    )
else:
    print(f"No report at {report_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Browse generated artefacts
# MAGIC
# MAGIC List the output tree. To open a single file, click its path in the
# MAGIC Workspace UI, or read it programmatically as below.

# COMMAND ----------

print("Output tree:")
out_root = Path(output_dir)
for path in sorted(out_root.rglob("*")):
    if path.is_file():
        rel = path.relative_to(out_root)
        size = path.stat().st_size
        print(f"  {rel}  ({size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Open one mapping YAML
# MAGIC
# MAGIC Pick a file from the list above and print its mapping. Adjust the path
# MAGIC to match a file you have.

# COMMAND ----------

# Example — change to a real file in your output.
sample_mapping = next(out_root.glob("mapping/**/*.bfm.yaml"), None)
if sample_mapping is not None:
    print(f"# {sample_mapping.relative_to(out_root)}")
    print()
    print(sample_mapping.read_text(encoding="utf-8"))
else:
    print("No mapping YAML produced — check input/ contains *.sql files.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Open the OpenLineage roll-up
# MAGIC
# MAGIC The lineage roll-up is one OpenLineage event per processed file.
# MAGIC Catalog connectors (Collibra, Atlan, Purview, Unity Catalog) can
# MAGIC consume this format directly.

# COMMAND ----------

import json

lineage_path = out_root / "lineage.json"
if lineage_path.exists():
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    n_events = len(lineage.get("events", []))
    print(f"{n_events} OpenLineage event(s) in {lineage_path}")
    if n_events:
        print("\nFirst event preview:")
        first = lineage["events"][0]
        print(f"  job:    {first['job']['namespace']}.{first['job']['name']}")
        print(f"  inputs: {[i['name'] for i in first['inputs']]}")
        print(f"  output: {first['outputs'][0]['name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC - **No DuckDB, no metadata DB.** Each run is self-contained and idempotent — running twice on the same input produces byte-identical output.
# MAGIC - **Quality checks** come from the same 20-rule set as the standalone `sql_process.py` (vendored as `_optimizer.py` + `_determinism.py`).
# MAGIC - **Auto-fixes** are conservative: only rewrites that preserve semantics on every engine. `ROW_NUMBER()` without `ORDER BY` is flagged but not silently fixed (no inferable ordering key).
# MAGIC - **Workspace files** work as normal POSIX paths under `/Workspace/...` in modern Databricks runtimes (DBR 14+); on older runtimes use DBFS or Volumes.
# MAGIC
# MAGIC See `README.md` in the same folder for the full feature description.
