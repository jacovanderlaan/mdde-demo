# MDDE Demo

Educational demonstration of MDDE concepts - a minimal working implementation.

## Purpose

Provides a runnable demo showing core MDDE functionality:
- SQL parsing to extract metadata
- Metadata storage in DuckDB
- SQL quality analysis
- SQL regeneration and diagrams

## Structure

```
mdde-demo/
├── src/mdde_lite/       # MDDE Lite Python package
│   ├── schema.py        # Minimal metadata schema (5 tables)
│   ├── parser.py        # SQL parser using sqlglot
│   ├── optimizer.py     # SQL quality checks (20 checks)
│   ├── generator.py     # SQL regenerator
│   ├── diagrams.py      # Mermaid diagram generation
│   └── lineage.py       # Column-level lineage
├── examples/            # Sample SQL files
├── models/              # Example MDDE models
├── workspace/           # Demo workspace
└── docs/                # Documentation
```

## Quick Start

```bash
# Parse SQL files
python -m src.mdde_lite.parser examples/sales

# Run optimizer
python -m src.mdde_lite.optimizer examples/sales

# Generate SQL
python -m src.mdde_lite.generator
```

## Part of MDDE Open-Core

| Repository | Description |
|------------|-------------|
| mdde | Core framework |
| mdde-demo | Educational demo (this repo) |
