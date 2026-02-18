"""
MDDE Lite - Temporal Pattern Detection

Detects and generates Slowly Changing Dimension (SCD) patterns:
- SCD Type 1: Overwrite (no history)
- SCD Type 2: Add row (full history with valid_from/valid_to)
- Bi-temporal: Business time + system time

Related articles:
- "Data Historization - Making Time a First-Class Citizen"
- "Bi-Temporal SCD2 with Redelivery Support"
- "Time Travel vs Bi-Temporal SCD2"
- "The Dual SCD2 Pattern for Bi-Temporal Data"

This is a simplified version. The full MDDE framework includes:
- Complete bi-temporal support
- Redelivery handling
- PIT (Point-In-Time) table generation
- AS-OF query generation
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum
import re


class SCDType(Enum):
    """SCD classification types."""
    TYPE_0 = "type_0"  # Fixed/immutable
    TYPE_1 = "type_1"  # Overwrite
    TYPE_2 = "type_2"  # Add row with history
    TYPE_3 = "type_3"  # Add column (previous value)
    TYPE_4 = "type_4"  # Mini-dimension
    TYPE_6 = "type_6"  # Hybrid (1+2+3)


@dataclass
class TemporalColumn:
    """Represents a temporal column detection."""
    name: str
    column_type: str  # valid_from, valid_to, is_current, business_date, system_date
    data_type: str
    confidence: float  # 0.0 to 1.0


@dataclass
class SCDPattern:
    """Represents a detected SCD pattern."""
    entity_name: str
    scd_type: SCDType
    confidence: float
    temporal_columns: List[TemporalColumn]
    natural_key_columns: List[str]
    surrogate_key_column: Optional[str]
    tracking_columns: List[str]  # Columns that trigger new versions
    is_bi_temporal: bool
    recommendations: List[str]


# Common temporal column patterns
TEMPORAL_PATTERNS = {
    "valid_from": [
        r"^valid_from$", r"^eff_date$", r"^effective_date$",
        r"^start_date$", r"^from_date$", r"^begin_date$",
        r"^valid_start$", r"^eff_start$", r"^date_from$"
    ],
    "valid_to": [
        r"^valid_to$", r"^exp_date$", r"^expiry_date$",
        r"^end_date$", r"^to_date$", r"^expire_date$",
        r"^valid_end$", r"^eff_end$", r"^date_to$"
    ],
    "is_current": [
        r"^is_current$", r"^current_flag$", r"^is_active$",
        r"^active_flag$", r"^current_ind$", r"^is_latest$"
    ],
    "business_date": [
        r"^business_date$", r"^as_of_date$", r"^reporting_date$",
        r"^snapshot_date$", r"^data_date$"
    ],
    "system_date": [
        r"^system_date$", r"^load_date$", r"^insert_date$",
        r"^created_at$", r"^loaded_at$", r"^_load_ts$"
    ],
    "surrogate_key": [
        r".*_sk$", r".*_key$", r"^sk_.*", r"^surrogate_key$"
    ]
}


def detect_temporal_columns(
    column_names: List[str],
    data_types: Optional[Dict[str, str]] = None
) -> List[TemporalColumn]:
    """
    Detect temporal columns from a list of column names.

    Args:
        column_names: List of column names to analyze
        data_types: Optional dict mapping column names to data types

    Returns:
        List of detected temporal columns
    """
    data_types = data_types or {}
    detected = []

    for col_name in column_names:
        col_lower = col_name.lower()

        for col_type, patterns in TEMPORAL_PATTERNS.items():
            for pattern in patterns:
                if re.match(pattern, col_lower, re.IGNORECASE):
                    confidence = 0.9 if pattern.startswith("^") and pattern.endswith("$") else 0.7

                    # Boost confidence if data type matches
                    data_type = data_types.get(col_name, "unknown")
                    if col_type in ("valid_from", "valid_to", "business_date", "system_date"):
                        if any(t in data_type.lower() for t in ["date", "time", "timestamp"]):
                            confidence = min(1.0, confidence + 0.1)

                    detected.append(TemporalColumn(
                        name=col_name,
                        column_type=col_type,
                        data_type=data_type,
                        confidence=confidence
                    ))
                    break
            else:
                continue
            break

    return detected


def detect_scd_pattern(
    entity_name: str,
    column_names: List[str],
    primary_key_columns: Optional[List[str]] = None,
    data_types: Optional[Dict[str, str]] = None
) -> SCDPattern:
    """
    Detect the SCD pattern for an entity.

    Args:
        entity_name: Name of the entity/table
        column_names: List of all column names
        primary_key_columns: List of primary key column names
        data_types: Optional dict mapping column names to data types

    Returns:
        Detected SCD pattern with recommendations
    """
    temporal_cols = detect_temporal_columns(column_names, data_types)

    # Categorize temporal columns
    has_valid_from = any(c.column_type == "valid_from" for c in temporal_cols)
    has_valid_to = any(c.column_type == "valid_to" for c in temporal_cols)
    has_is_current = any(c.column_type == "is_current" for c in temporal_cols)
    has_business_date = any(c.column_type == "business_date" for c in temporal_cols)
    has_system_date = any(c.column_type == "system_date" for c in temporal_cols)
    has_surrogate = any(c.column_type == "surrogate_key" for c in temporal_cols)

    # Determine SCD type
    recommendations = []

    if has_valid_from and has_valid_to:
        scd_type = SCDType.TYPE_2
        confidence = 0.9

        if not has_is_current:
            recommendations.append("Consider adding is_current flag for query optimization")

        if has_business_date and has_system_date:
            is_bi_temporal = True
            confidence = 0.95
            recommendations.append("Bi-temporal pattern detected - supports both business and system time")
        else:
            is_bi_temporal = False
            if not has_system_date:
                recommendations.append("Consider adding system_date for audit trail")

    elif has_valid_from and not has_valid_to:
        scd_type = SCDType.TYPE_2
        confidence = 0.7
        is_bi_temporal = False
        recommendations.append("Missing valid_to column - add for proper SCD2 implementation")
        recommendations.append("Use NULL or '9999-12-31' for current records in valid_to")

    elif has_is_current and not has_valid_from:
        scd_type = SCDType.TYPE_2
        confidence = 0.6
        is_bi_temporal = False
        recommendations.append("Has is_current flag but missing valid_from/valid_to")
        recommendations.append("Add valid_from and valid_to for full SCD2 support")

    else:
        scd_type = SCDType.TYPE_1
        confidence = 0.8
        is_bi_temporal = False
        recommendations.append("No temporal columns detected - classified as SCD Type 1")
        recommendations.append("Add valid_from, valid_to, is_current for history tracking")

    # Identify keys
    surrogate_col = None
    for col in temporal_cols:
        if col.column_type == "surrogate_key":
            surrogate_col = col.name
            break

    if not surrogate_col:
        for col in column_names:
            if col.lower().endswith("_sk") or col.lower() == "sk":
                surrogate_col = col
                break

    natural_keys = primary_key_columns or []
    if surrogate_col and surrogate_col in natural_keys:
        # If PK is surrogate, we need to find natural key
        natural_keys = [c for c in natural_keys if c != surrogate_col]

    if not natural_keys and not surrogate_col:
        recommendations.append("Consider adding surrogate key for SCD2 pattern")

    # Identify tracking columns (non-temporal, non-key columns)
    temporal_names = {c.name for c in temporal_cols}
    key_names = set(natural_keys) | ({surrogate_col} if surrogate_col else set())
    tracking_cols = [c for c in column_names
                     if c not in temporal_names and c not in key_names]

    return SCDPattern(
        entity_name=entity_name,
        scd_type=scd_type,
        confidence=confidence,
        temporal_columns=temporal_cols,
        natural_key_columns=natural_keys,
        surrogate_key_column=surrogate_col,
        tracking_columns=tracking_cols,
        is_bi_temporal=is_bi_temporal,
        recommendations=recommendations
    )


def generate_scd2_merge(
    target_table: str,
    source_table: str,
    natural_key_columns: List[str],
    tracking_columns: List[str],
    surrogate_key_column: str = "sk",
    valid_from_column: str = "valid_from",
    valid_to_column: str = "valid_to",
    is_current_column: str = "is_current",
    dialect: str = "snowflake"
) -> str:
    """
    Generate SCD Type 2 MERGE statement.

    Args:
        target_table: Target dimension table
        source_table: Source staging table
        natural_key_columns: Natural key columns
        tracking_columns: Columns that trigger new versions
        surrogate_key_column: Name of surrogate key column
        valid_from_column: Name of valid_from column
        valid_to_column: Name of valid_to column
        is_current_column: Name of is_current flag column
        dialect: SQL dialect (snowflake, databricks, bigquery)

    Returns:
        Generated MERGE SQL statement
    """
    natural_key_join = " AND ".join(
        f"t.{col} = s.{col}" for col in natural_key_columns
    )

    tracking_compare = " OR ".join(
        f"t.{col} <> s.{col}" for col in tracking_columns
    )

    all_columns = natural_key_columns + tracking_columns
    select_columns = ", ".join(all_columns)
    insert_columns = f"{surrogate_key_column}, " + ", ".join(all_columns) + f", {valid_from_column}, {valid_to_column}, {is_current_column}"

    if dialect == "snowflake":
        sk_expr = f"(SELECT COALESCE(MAX({surrogate_key_column}), 0) + ROW_NUMBER() OVER (ORDER BY {natural_key_columns[0]}) FROM {target_table})"
        end_date = "'9999-12-31 23:59:59'::TIMESTAMP"
    elif dialect == "databricks":
        sk_expr = f"(SELECT COALESCE(MAX({surrogate_key_column}), 0) FROM {target_table}) + ROW_NUMBER() OVER (ORDER BY {natural_key_columns[0]})"
        end_date = "CAST('9999-12-31 23:59:59' AS TIMESTAMP)"
    else:
        sk_expr = f"(SELECT COALESCE(MAX({surrogate_key_column}), 0) + 1 FROM {target_table})"
        end_date = "TIMESTAMP '9999-12-31 23:59:59'"

    return f"""-- SCD Type 2 MERGE for {target_table}
-- Generated by MDDE Lite

MERGE INTO {target_table} AS t
USING (
    SELECT {select_columns}
    FROM {source_table}
) AS s
ON {natural_key_join} AND t.{is_current_column} = TRUE

-- Update existing current records (close them)
WHEN MATCHED AND ({tracking_compare}) THEN
    UPDATE SET
        {valid_to_column} = CURRENT_TIMESTAMP,
        {is_current_column} = FALSE

-- Insert new records (for changed or new rows)
WHEN NOT MATCHED THEN
    INSERT ({insert_columns})
    VALUES (
        {sk_expr},
        s.{", s.".join(all_columns)},
        CURRENT_TIMESTAMP,
        {end_date},
        TRUE
    );

-- Second pass: Insert new versions for updated records
INSERT INTO {target_table} ({insert_columns})
SELECT
    {sk_expr},
    s.{", s.".join(all_columns)},
    CURRENT_TIMESTAMP,
    {end_date},
    TRUE
FROM {source_table} s
JOIN {target_table} t
    ON {natural_key_join}
WHERE t.{is_current_column} = FALSE
    AND t.{valid_to_column} = CURRENT_TIMESTAMP
    AND NOT EXISTS (
        SELECT 1 FROM {target_table} t2
        WHERE {" AND ".join(f"t2.{col} = s.{col}" for col in natural_key_columns)}
            AND t2.{is_current_column} = TRUE
    );
"""


def generate_scd2_view(
    base_table: str,
    view_name: Optional[str] = None,
    valid_from_column: str = "valid_from",
    valid_to_column: str = "valid_to",
    is_current_column: str = "is_current"
) -> str:
    """
    Generate a view for querying SCD2 data with AS-OF semantics.

    Args:
        base_table: Base SCD2 table
        view_name: Name for the view (default: {base_table}_current)
        valid_from_column: Name of valid_from column
        valid_to_column: Name of valid_to column
        is_current_column: Name of is_current flag column

    Returns:
        Generated CREATE VIEW statement
    """
    view_name = view_name or f"{base_table}_current"

    return f"""-- Current records view for {base_table}
CREATE OR REPLACE VIEW {view_name} AS
SELECT *
FROM {base_table}
WHERE {is_current_column} = TRUE;

-- Point-in-time query function (use as template)
-- SELECT * FROM {base_table}
-- WHERE @as_of_date >= {valid_from_column}
--   AND @as_of_date < {valid_to_column};
"""


def classify_columns_for_scd(
    columns: List[str],
    data_types: Optional[Dict[str, str]] = None
) -> Dict[str, List[str]]:
    """
    Classify columns into SCD categories.

    Args:
        columns: List of column names
        data_types: Optional mapping of column names to data types

    Returns:
        Dictionary with keys: 'natural_key', 'surrogate_key', 'type1', 'type2', 'temporal', 'audit'
    """
    data_types = data_types or {}
    classification = {
        "natural_key": [],
        "surrogate_key": [],
        "type1": [],      # Overwrite columns
        "type2": [],      # History tracking columns
        "temporal": [],   # Date/time columns
        "audit": []       # Audit columns
    }

    temporal_cols = detect_temporal_columns(columns, data_types)
    temporal_names = {c.name for c in temporal_cols}

    for col in columns:
        col_lower = col.lower()

        if col in temporal_names:
            classification["temporal"].append(col)
        elif col_lower.endswith("_sk") or col_lower == "sk" or col_lower == "surrogate_key":
            classification["surrogate_key"].append(col)
        elif col_lower.endswith("_id") or col_lower == "id":
            classification["natural_key"].append(col)
        elif any(audit in col_lower for audit in ["created", "updated", "modified", "load", "insert"]):
            classification["audit"].append(col)
        elif any(t1 in col_lower for t1 in ["status", "flag", "code", "type"]):
            # Status-like columns often Type 1
            classification["type1"].append(col)
        else:
            # Default to Type 2 (track history)
            classification["type2"].append(col)

    return classification


if __name__ == "__main__":
    print("MDDE Lite - Temporal Pattern Detection")
    print("=" * 50)

    # Test case 1: SCD2 pattern
    print("\n--- Test 1: SCD2 Pattern Detection ---")
    columns1 = [
        "customer_sk", "customer_id", "customer_name", "email", "region",
        "valid_from", "valid_to", "is_current", "_load_ts"
    ]

    pattern1 = detect_scd_pattern(
        "dim_customer",
        columns1,
        primary_key_columns=["customer_sk"],
        data_types={
            "valid_from": "TIMESTAMP",
            "valid_to": "TIMESTAMP",
            "is_current": "BOOLEAN"
        }
    )

    print(f"Entity: {pattern1.entity_name}")
    print(f"SCD Type: {pattern1.scd_type.value}")
    print(f"Confidence: {pattern1.confidence:.2f}")
    print(f"Bi-temporal: {pattern1.is_bi_temporal}")
    print(f"Surrogate Key: {pattern1.surrogate_key_column}")
    print(f"Tracking Columns: {pattern1.tracking_columns}")
    print("Temporal Columns:")
    for tc in pattern1.temporal_columns:
        print(f"  - {tc.name} ({tc.column_type}, confidence: {tc.confidence:.2f})")

    # Test case 2: Bi-temporal pattern
    print("\n--- Test 2: Bi-Temporal Detection ---")
    columns2 = [
        "contract_sk", "contract_id", "amount", "status",
        "business_date", "valid_from", "valid_to", "is_current", "system_date"
    ]

    pattern2 = detect_scd_pattern("dim_contract", columns2)
    print(f"SCD Type: {pattern2.scd_type.value}")
    print(f"Bi-temporal: {pattern2.is_bi_temporal}")

    # Test case 3: No temporal columns (Type 1)
    print("\n--- Test 3: Type 1 Detection ---")
    columns3 = ["product_id", "product_name", "category", "price"]

    pattern3 = detect_scd_pattern("dim_product", columns3, ["product_id"])
    print(f"SCD Type: {pattern3.scd_type.value}")
    print("Recommendations:")
    for rec in pattern3.recommendations:
        print(f"  - {rec}")

    # Test case 4: Generate MERGE
    print("\n--- Test 4: SCD2 MERGE Generation ---")
    merge_sql = generate_scd2_merge(
        target_table="dim_customer",
        source_table="stg_customers",
        natural_key_columns=["customer_id"],
        tracking_columns=["customer_name", "email", "region"],
        dialect="snowflake"
    )
    print(merge_sql[:500] + "...")

    # Test case 5: Column classification
    print("\n--- Test 5: Column Classification ---")
    cols = ["order_sk", "order_id", "customer_id", "amount", "status",
            "valid_from", "valid_to", "created_at", "updated_at"]
    classified = classify_columns_for_scd(cols)
    for category, col_list in classified.items():
        if col_list:
            print(f"  {category}: {col_list}")
