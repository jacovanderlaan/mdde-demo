"""
MDDE Lite - Educational Edition

A minimal implementation of Metadata-Driven Data Engineering concepts.
This package demonstrates core MDDE functionality without the full framework.

Modules:
- schema: Minimal metadata schema (5 tables)
- parser: Simple SQL parser using sqlglot
- optimizer: Basic SQL quality checks
- generator: Simple SQL regenerator
"""

__version__ = "0.1.0"
__author__ = "Jaco van der Laan"

from .schema import create_schema, get_schema_info
