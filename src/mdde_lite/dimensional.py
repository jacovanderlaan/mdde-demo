"""
MDDE Lite - Dimensional Model Generator

Generate and detect dimensional model patterns:
- Fact tables: Measures with foreign keys to dimensions
- Dimension tables: Descriptive attributes
- Star schema: Fact surrounded by dimensions
- Snowflake schema: Normalized dimensions

Related articles:
- "Generating Dimensional Models Automatically from a Historized 3NF"
- "Solving Many-to-Many & Drill-Across with the Unified Star Schema"

This is a simplified version. The full MDDE framework includes:
- Full star/snowflake generation
- Bridge table generation
- Degenerate dimension handling
- Unified Star Schema support
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum
import re


class DimensionalType(Enum):
    """Dimensional model construct types."""
    FACT = "fact"
    DIMENSION = "dimension"
    BRIDGE = "bridge"
    OUTRIGGER = "outrigger"
    JUNK = "junk"
    DEGENERATE = "degenerate"
    UNKNOWN = "unknown"


class MeasureType(Enum):
    """Types of measures in fact tables."""
    ADDITIVE = "additive"          # Can be summed across all dimensions
    SEMI_ADDITIVE = "semi_additive"  # Can be summed across some dimensions
    NON_ADDITIVE = "non_additive"   # Cannot be summed (ratios, averages)


@dataclass
class Measure:
    """A measure/metric in a fact table."""
    name: str
    data_type: str
    measure_type: MeasureType = MeasureType.ADDITIVE
    aggregation: str = "SUM"  # SUM, AVG, COUNT, MIN, MAX
    description: str = ""


@dataclass
class DimensionAttribute:
    """An attribute in a dimension table."""
    name: str
    data_type: str
    is_surrogate_key: bool = False
    is_natural_key: bool = False
    is_hierarchy: bool = False
    hierarchy_level: Optional[int] = None
    scd_type: int = 1  # 1 or 2


@dataclass
class DimensionalConstruct:
    """A detected dimensional model construct."""
    name: str
    construct_type: DimensionalType
    confidence: float
    surrogate_key: Optional[str] = None
    natural_key: Optional[str] = None
    measures: List[Measure] = field(default_factory=list)
    dimension_keys: List[str] = field(default_factory=list)  # FK to dimensions
    attributes: List[DimensionAttribute] = field(default_factory=list)
    grain: str = ""
    issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


# Patterns for dimensional model detection
DIM_PATTERNS = {
    "fact_table": [
        r"^fact_", r"^fct_", r"^f_", r"_fact$"
    ],
    "dimension_table": [
        r"^dim_", r"^d_", r"_dim$", r"_dimension$"
    ],
    "bridge_table": [
        r"^bridge_", r"^br_", r"_bridge$"
    ],
    "surrogate_key": [
        r".*_sk$", r".*_key$", r"^sk_", r"^key_"
    ],
    "measure": [
        r".*_amount$", r".*_qty$", r".*_quantity$", r".*_count$",
        r".*_total$", r".*_sum$", r".*_avg$", r".*_rate$",
        r"^amt_", r"^qty_", r"^cnt_"
    ],
    "dimension_fk": [
        r".*_sk$", r".*_key$", r".*_id$"
    ]
}


def detect_dimensional_construct(
    table_name: str,
    columns: List[Dict[str, any]]
) -> DimensionalConstruct:
    """
    Detect dimensional model construct type from table structure.

    Args:
        table_name: Name of the table
        columns: List of column definitions

    Returns:
        DimensionalConstruct with detection results
    """
    table_lower = table_name.lower()

    # Detect type from name
    detected_type = DimensionalType.UNKNOWN
    name_confidence = 0.0

    if _matches_pattern(table_lower, DIM_PATTERNS["fact_table"]):
        detected_type = DimensionalType.FACT
        name_confidence = 0.8
    elif _matches_pattern(table_lower, DIM_PATTERNS["dimension_table"]):
        detected_type = DimensionalType.DIMENSION
        name_confidence = 0.8
    elif _matches_pattern(table_lower, DIM_PATTERNS["bridge_table"]):
        detected_type = DimensionalType.BRIDGE
        name_confidence = 0.8

    # Analyze columns
    surrogate_key = None
    natural_key = None
    measures = []
    dimension_keys = []
    attributes = []
    issues = []
    recommendations = []

    sk_count = 0
    measure_count = 0
    fk_count = 0

    for col in columns:
        col_name = col.get("name", "").lower()
        col_type = col.get("data_type", "").upper()
        is_pk = col.get("is_primary_key", False)

        # Detect surrogate key
        if _matches_pattern(col_name, DIM_PATTERNS["surrogate_key"]):
            sk_count += 1
            if is_pk:
                surrogate_key = col_name

            # In fact tables, non-PK surrogate keys are dimension FKs
            if not is_pk:
                dimension_keys.append(col_name)
                fk_count += 1

        # Detect measures
        if _matches_pattern(col_name, DIM_PATTERNS["measure"]) or _is_numeric_type(col_type):
            if not _matches_pattern(col_name, DIM_PATTERNS["surrogate_key"]):
                measure_type = _infer_measure_type(col_name)
                aggregation = _infer_aggregation(col_name)

                measures.append(Measure(
                    name=col_name,
                    data_type=col_type,
                    measure_type=measure_type,
                    aggregation=aggregation
                ))
                measure_count += 1

        # Other columns are attributes
        if not _matches_pattern(col_name, DIM_PATTERNS["surrogate_key"]) and \
           not _matches_pattern(col_name, DIM_PATTERNS["measure"]):
            is_hierarchy = _is_hierarchy_column(col_name)
            hierarchy_level = _get_hierarchy_level(col_name) if is_hierarchy else None

            attributes.append(DimensionAttribute(
                name=col_name,
                data_type=col_type,
                is_surrogate_key=False,
                is_natural_key=is_pk and not _matches_pattern(col_name, DIM_PATTERNS["surrogate_key"]),
                is_hierarchy=is_hierarchy,
                hierarchy_level=hierarchy_level
            ))

    # Infer type from column structure
    column_confidence = 0.0

    if detected_type == DimensionalType.UNKNOWN:
        if fk_count >= 2 and measure_count >= 1:
            detected_type = DimensionalType.FACT
            column_confidence = 0.7
        elif sk_count == 1 and len(attributes) > 2:
            detected_type = DimensionalType.DIMENSION
            column_confidence = 0.6

    # Validate based on type
    if detected_type == DimensionalType.FACT:
        if not dimension_keys:
            issues.append("Fact table should have dimension foreign keys")
            recommendations.append("Add _sk columns for dimension relationships")
        if not measures:
            issues.append("Fact table should have measures")
            recommendations.append("Add numeric measure columns (amount, qty, count)")
        column_confidence = 0.3 * (1 if dimension_keys else 0) + \
                           0.3 * (1 if measures else 0) + \
                           0.2 * (1 if surrogate_key else 0)

    elif detected_type == DimensionalType.DIMENSION:
        if not surrogate_key:
            issues.append("Dimension should have surrogate key")
            recommendations.append("Add a _sk column as primary key")
        if len(attributes) < 2:
            issues.append("Dimension should have descriptive attributes")
        column_confidence = 0.4 * (1 if surrogate_key else 0) + \
                           0.3 * (1 if attributes else 0) + \
                           0.3 * (1 if natural_key or len(attributes) > 2 else 0)

    confidence = min(1.0, name_confidence + column_confidence)

    # Infer grain for fact tables
    grain = ""
    if detected_type == DimensionalType.FACT and dimension_keys:
        grain = " x ".join(dk.replace("_sk", "").replace("_key", "") for dk in dimension_keys[:3])

    return DimensionalConstruct(
        name=table_name,
        construct_type=detected_type,
        confidence=confidence,
        surrogate_key=surrogate_key,
        natural_key=natural_key,
        measures=measures,
        dimension_keys=dimension_keys,
        attributes=attributes,
        grain=grain,
        issues=issues,
        recommendations=recommendations
    )


def _matches_pattern(text: str, patterns: List[str]) -> bool:
    """Check if text matches any pattern."""
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _is_numeric_type(data_type: str) -> bool:
    """Check if data type is numeric."""
    numeric_types = ["INT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "NUMBER", "REAL"]
    return any(t in data_type.upper() for t in numeric_types)


def _infer_measure_type(col_name: str) -> MeasureType:
    """Infer measure type from column name."""
    col_lower = col_name.lower()

    if any(x in col_lower for x in ["ratio", "rate", "pct", "percent", "avg"]):
        return MeasureType.NON_ADDITIVE
    elif any(x in col_lower for x in ["balance", "inventory", "headcount"]):
        return MeasureType.SEMI_ADDITIVE
    else:
        return MeasureType.ADDITIVE


def _infer_aggregation(col_name: str) -> str:
    """Infer aggregation function from column name."""
    col_lower = col_name.lower()

    if "count" in col_lower or "cnt" in col_lower:
        return "COUNT"
    elif "avg" in col_lower or "average" in col_lower:
        return "AVG"
    elif "max" in col_lower:
        return "MAX"
    elif "min" in col_lower:
        return "MIN"
    else:
        return "SUM"


def _is_hierarchy_column(col_name: str) -> bool:
    """Check if column is part of a hierarchy."""
    hierarchy_patterns = [
        r".*_level\d*$", r".*_l\d+$", r".*_parent.*", r".*_child.*",
        r"^level_", r".*_category$", r".*_subcategory$",
        r".*_group$", r".*_subgroup$"
    ]
    return _matches_pattern(col_name, hierarchy_patterns)


def _get_hierarchy_level(col_name: str) -> int:
    """Extract hierarchy level from column name."""
    match = re.search(r"_l(\d+)$|_level(\d+)$|(\d+)$", col_name.lower())
    if match:
        for g in match.groups():
            if g:
                return int(g)
    return 0


def generate_star_schema(
    source_entities: List[Dict[str, any]],
    fact_name: str = "fact_main"
) -> Dict[str, any]:
    """
    Generate a star schema from source entities.

    Args:
        source_entities: List of source entities with columns
        fact_name: Name for the generated fact table

    Returns:
        Star schema definition
    """
    schema = {
        "fact": None,
        "dimensions": [],
        "relationships": []
    }

    # Identify candidate dimensions (entities with descriptive attributes)
    dimensions = []
    fact_measures = []
    fact_keys = []

    for entity in source_entities:
        name = entity.get("name", "")
        columns = entity.get("columns", [])

        # Count numeric vs non-numeric columns
        numeric_cols = [c for c in columns if _is_numeric_type(c.get("data_type", ""))]
        text_cols = [c for c in columns if not _is_numeric_type(c.get("data_type", ""))]

        # Entities with mostly text columns are dimensions
        if len(text_cols) > len(numeric_cols) and len(text_cols) >= 2:
            dim_name = f"dim_{name}" if not name.startswith("dim_") else name

            # Find PK for dimension
            pk = next((c["name"] for c in columns if c.get("is_primary_key")), None)

            dimensions.append({
                "name": dim_name,
                "source": name,
                "surrogate_key": f"{name}_sk",
                "natural_key": pk,
                "attributes": [c["name"] for c in text_cols if c["name"] != pk]
            })

            # Add FK to fact
            fact_keys.append(f"{name}_sk")
        else:
            # Numeric-heavy entities contribute measures
            for col in numeric_cols:
                fact_measures.append({
                    "name": col["name"],
                    "source": f"{name}.{col['name']}",
                    "aggregation": _infer_aggregation(col["name"])
                })

    # Build fact table
    schema["fact"] = {
        "name": fact_name,
        "dimension_keys": fact_keys,
        "measures": fact_measures,
        "grain": " x ".join(d["name"].replace("dim_", "") for d in dimensions[:4])
    }

    schema["dimensions"] = dimensions

    # Generate relationships
    for dim in dimensions:
        schema["relationships"].append({
            "fact": fact_name,
            "dimension": dim["name"],
            "key": f"{dim['source']}_sk",
            "cardinality": "many_to_one"
        })

    return schema


def generate_dimension_ddl(
    dimension: DimensionalConstruct,
    scd_type: int = 2,
    dialect: str = "snowflake"
) -> str:
    """
    Generate DDL for a dimension table.

    Args:
        dimension: DimensionalConstruct
        scd_type: SCD type (1 or 2)
        dialect: SQL dialect

    Returns:
        DDL statement
    """
    lines = [
        f"-- DIMENSION: {dimension.name}",
        f"-- SCD Type: {scd_type}",
        f"CREATE TABLE {dimension.name} ("
    ]

    # Surrogate key
    sk = dimension.surrogate_key or f"{dimension.name.replace('dim_', '')}_sk"
    lines.append(f"    {sk} INTEGER NOT NULL,")

    # Natural key
    if dimension.natural_key:
        lines.append(f"    {dimension.natural_key} VARCHAR NOT NULL,")

    # Attributes
    for attr in dimension.attributes:
        null = "" if attr.is_natural_key else ""
        lines.append(f"    {attr.name} {attr.data_type or 'VARCHAR'}{null},")

    # SCD2 columns
    if scd_type == 2:
        lines.append("    valid_from TIMESTAMP NOT NULL,")
        lines.append("    valid_to TIMESTAMP,")
        lines.append("    is_current BOOLEAN DEFAULT TRUE,")

    # Audit columns
    lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,")
    lines.append("    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,")

    # Primary key
    lines.append(f"    PRIMARY KEY ({sk})")
    lines.append(");")

    return "\n".join(lines)


def generate_fact_ddl(
    fact: DimensionalConstruct,
    dialect: str = "snowflake"
) -> str:
    """
    Generate DDL for a fact table.

    Args:
        fact: DimensionalConstruct
        dialect: SQL dialect

    Returns:
        DDL statement
    """
    lines = [
        f"-- FACT: {fact.name}",
        f"-- Grain: {fact.grain}",
        f"CREATE TABLE {fact.name} ("
    ]

    # Surrogate key (optional for fact)
    if fact.surrogate_key:
        lines.append(f"    {fact.surrogate_key} INTEGER NOT NULL,")

    # Dimension keys
    for dk in fact.dimension_keys:
        lines.append(f"    {dk} INTEGER NOT NULL,")

    # Degenerate dimensions (date, etc.)
    lines.append("    transaction_date DATE NOT NULL,")

    # Measures
    for measure in fact.measures:
        lines.append(f"    {measure.name} {measure.data_type or 'DECIMAL(18,2)'},")

    # Audit
    lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    # Composite key or surrogate
    if fact.dimension_keys:
        pk_cols = ", ".join(fact.dimension_keys[:3])  # Limit for readability
        lines.append(f"    -- Composite key: ({pk_cols}, transaction_date)")

    lines.append(");")

    return "\n".join(lines)


def validate_star_schema(
    fact: DimensionalConstruct,
    dimensions: List[DimensionalConstruct]
) -> Dict[str, any]:
    """
    Validate a star schema.

    Args:
        fact: Fact table construct
        dimensions: List of dimension constructs

    Returns:
        Validation report
    """
    report = {
        "valid": True,
        "issues": [],
        "warnings": [],
        "metrics": {
            "dimension_count": len(dimensions),
            "measure_count": len(fact.measures),
            "grain": fact.grain
        }
    }

    # Check fact has dimension keys
    if not fact.dimension_keys:
        report["issues"].append("Fact table has no dimension foreign keys")
        report["valid"] = False

    # Check fact has measures
    if not fact.measures:
        report["issues"].append("Fact table has no measures")
        report["valid"] = False

    # Check all FK references exist
    dim_names = {d.name.lower() for d in dimensions}
    for dk in fact.dimension_keys:
        # Extract dimension name from key
        dim_name = dk.replace("_sk", "").replace("_key", "")
        if not any(dim_name in d for d in dim_names):
            report["warnings"].append(
                f"Dimension key '{dk}' has no matching dimension"
            )

    # Check dimensions have surrogate keys
    for dim in dimensions:
        if not dim.surrogate_key:
            report["warnings"].append(
                f"Dimension '{dim.name}' missing surrogate key"
            )

    # Check for conformed dimensions (dimensions used by multiple facts)
    # Simplified: just check if dimensions have enough attributes
    for dim in dimensions:
        if len(dim.attributes) < 2:
            report["warnings"].append(
                f"Dimension '{dim.name}' has few attributes - consider enriching"
            )

    return report


if __name__ == "__main__":
    print("MDDE Lite - Dimensional Model Generator")
    print("=" * 60)

    # Test case 1: Fact table detection
    print("\n--- Test 1: Fact Table Detection ---")
    fact_columns = [
        {"name": "sale_sk", "data_type": "INTEGER", "is_primary_key": True},
        {"name": "customer_sk", "data_type": "INTEGER", "is_primary_key": False},
        {"name": "product_sk", "data_type": "INTEGER", "is_primary_key": False},
        {"name": "date_sk", "data_type": "INTEGER", "is_primary_key": False},
        {"name": "quantity", "data_type": "INTEGER", "is_primary_key": False},
        {"name": "unit_price", "data_type": "DECIMAL(10,2)", "is_primary_key": False},
        {"name": "total_amount", "data_type": "DECIMAL(10,2)", "is_primary_key": False}
    ]

    fact = detect_dimensional_construct("fact_sales", fact_columns)
    print(f"Table: {fact.name}")
    print(f"Type: {fact.construct_type.value}")
    print(f"Confidence: {fact.confidence:.0%}")
    print(f"Dimension Keys: {fact.dimension_keys}")
    print(f"Measures: {[m.name for m in fact.measures]}")
    print(f"Grain: {fact.grain}")

    # Test case 2: Dimension table detection
    print("\n--- Test 2: Dimension Table Detection ---")
    dim_columns = [
        {"name": "customer_sk", "data_type": "INTEGER", "is_primary_key": True},
        {"name": "customer_id", "data_type": "VARCHAR(50)", "is_primary_key": False},
        {"name": "customer_name", "data_type": "VARCHAR(100)", "is_primary_key": False},
        {"name": "email", "data_type": "VARCHAR(255)", "is_primary_key": False},
        {"name": "city", "data_type": "VARCHAR(100)", "is_primary_key": False},
        {"name": "state", "data_type": "VARCHAR(50)", "is_primary_key": False},
        {"name": "country", "data_type": "VARCHAR(50)", "is_primary_key": False}
    ]

    dim = detect_dimensional_construct("dim_customer", dim_columns)
    print(f"Table: {dim.name}")
    print(f"Type: {dim.construct_type.value}")
    print(f"Surrogate Key: {dim.surrogate_key}")
    print(f"Attributes: {[a.name for a in dim.attributes]}")

    # Test case 3: Star schema generation
    print("\n--- Test 3: Star Schema Generation ---")
    source_entities = [
        {
            "name": "customer",
            "columns": [
                {"name": "customer_id", "data_type": "VARCHAR", "is_primary_key": True},
                {"name": "name", "data_type": "VARCHAR", "is_primary_key": False},
                {"name": "email", "data_type": "VARCHAR", "is_primary_key": False},
                {"name": "city", "data_type": "VARCHAR", "is_primary_key": False}
            ]
        },
        {
            "name": "product",
            "columns": [
                {"name": "product_id", "data_type": "VARCHAR", "is_primary_key": True},
                {"name": "product_name", "data_type": "VARCHAR", "is_primary_key": False},
                {"name": "category", "data_type": "VARCHAR", "is_primary_key": False},
                {"name": "price", "data_type": "DECIMAL", "is_primary_key": False}
            ]
        },
        {
            "name": "sales",
            "columns": [
                {"name": "sale_id", "data_type": "INTEGER", "is_primary_key": True},
                {"name": "quantity", "data_type": "INTEGER", "is_primary_key": False},
                {"name": "amount", "data_type": "DECIMAL", "is_primary_key": False},
                {"name": "discount", "data_type": "DECIMAL", "is_primary_key": False}
            ]
        }
    ]

    schema = generate_star_schema(source_entities, "fact_sales")
    print(f"Fact: {schema['fact']['name']}")
    print(f"Grain: {schema['fact']['grain']}")
    print(f"Dimensions: {[d['name'] for d in schema['dimensions']]}")
    print(f"Measures: {[m['name'] for m in schema['fact']['measures']]}")

    # Test case 4: DDL Generation
    print("\n--- Test 4: DDL Generation ---")
    print(generate_dimension_ddl(dim, scd_type=2)[:400] + "...")

    # Test case 5: Validation
    print("\n--- Test 5: Schema Validation ---")
    report = validate_star_schema(fact, [dim])
    print(f"Valid: {report['valid']}")
    print(f"Metrics: {report['metrics']}")
    if report['warnings']:
        print("Warnings:")
        for w in report['warnings']:
            print(f"  - {w}")
