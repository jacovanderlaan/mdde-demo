"""
MDDE Lite - Educational Edition

A minimal implementation of Metadata-Driven Data Engineering concepts.
This package demonstrates core MDDE functionality without the full framework.

Modules:
- schema: Minimal metadata schema (5 tables)
- parser: Simple SQL parser using sqlglot
- optimizer: SQL quality checks (15 anti-patterns)
- generator: SQL regenerator with dialect support
- diagrams: Mermaid diagram generation (ERD, data flow, lineage)
- lineage: Column-level lineage extraction

Related articles: https://medium.com/@jaco.vanderlaan
Repository: https://github.com/jacovanderlaan/mdde-demo
"""

__version__ = "0.2.0"
__author__ = "Jaco van der Laan"

from .schema import create_schema, get_schema_info
from .optimizer import analyze_sql, analyze_file, analyze_directory, get_all_check_types
from .diagrams import generate_erd, generate_dataflow, generate_lineage
from .lineage import extract_lineage, ColumnLineage
