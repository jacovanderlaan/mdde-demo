"""
MDDE Lite - Data Vault Pattern Detection

Detect and validate Data Vault patterns:
- Hub: Business keys with surrogate key
- Link: Relationships between Hubs
- Satellite: Descriptive attributes with history

Related articles:
- "From DataVault Tooling to a Bi-Temporal SCD2 Framework"
- "Inmon, Data Vault, and Dimensional: Navigating Patterns"

This is a simplified version. The full MDDE framework includes:
- Full Data Vault 2.0 support
- Hash key generation
- Business Vault patterns
- PIT and Bridge table generation
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum
import re


class DVConstructType(Enum):
    """Data Vault construct types."""
    HUB = "hub"
    LINK = "link"
    SATELLITE = "satellite"
    REFERENCE = "reference"
    PIT = "pit"          # Point-In-Time
    BRIDGE = "bridge"
    UNKNOWN = "unknown"


@dataclass
class DVColumn:
    """Represents a column in a Data Vault table."""
    name: str
    data_type: str
    role: str  # hash_key, business_key, load_date, record_source, attribute, fk
    is_primary_key: bool = False


@dataclass
class DVConstruct:
    """Detected Data Vault construct."""
    name: str
    construct_type: DVConstructType
    confidence: float
    columns: List[DVColumn]
    business_keys: List[str] = field(default_factory=list)
    hash_key: Optional[str] = None
    load_date_column: Optional[str] = None
    record_source_column: Optional[str] = None
    linked_hubs: List[str] = field(default_factory=list)
    parent_hub: Optional[str] = None
    issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


# Common Data Vault column patterns
DV_PATTERNS = {
    "hash_key": [
        r"^h[kK]_", r".*_hk$", r".*_hash$", r"^hash_",
        r".*_sk$"  # Sometimes used as hash key
    ],
    "business_key": [
        r".*_bk$", r"^bk_", r".*_code$", r".*_number$",
        r".*_id$"  # Often natural key
    ],
    "load_date": [
        r"^load_d[at]te?$", r"^ldts$", r"^load_ts$",
        r"^dv_load_date$", r"^effective_from$"
    ],
    "record_source": [
        r"^record_source$", r"^rsrc$", r"^rec_src$",
        r"^source_system$", r"^dv_record_source$"
    ],
    "hash_diff": [
        r"^hash_diff$", r"^hdiff$", r"^h[dD]iff$",
        r".*_hashdiff$"
    ],
    "valid_from": [
        r"^valid_from$", r"^start_date$", r"^effective_from$"
    ],
    "valid_to": [
        r"^valid_to$", r"^end_date$", r"^effective_to$"
    ]
}

# Table naming patterns
DV_TABLE_PATTERNS = {
    DVConstructType.HUB: [
        r"^hub_", r"^h_", r"_hub$"
    ],
    DVConstructType.LINK: [
        r"^link_", r"^l_", r"_link$", r"^lnk_"
    ],
    DVConstructType.SATELLITE: [
        r"^sat_", r"^s_", r"_sat$", r"_satellite$"
    ],
    DVConstructType.REFERENCE: [
        r"^ref_", r"^r_", r"_ref$"
    ],
    DVConstructType.PIT: [
        r"^pit_", r"_pit$"
    ],
    DVConstructType.BRIDGE: [
        r"^bridge_", r"^br_", r"_bridge$"
    ]
}


def detect_dv_construct(
    table_name: str,
    columns: List[Dict[str, any]],
    strict: bool = False
) -> DVConstruct:
    """
    Detect Data Vault construct type from table name and columns.

    Args:
        table_name: Name of the table
        columns: List of column definitions with 'name', 'data_type', 'is_primary_key'
        strict: If True, require all mandatory columns

    Returns:
        DVConstruct with detection results
    """
    table_lower = table_name.lower()

    # Detect type from table name
    detected_type = DVConstructType.UNKNOWN
    name_confidence = 0.0

    for construct_type, patterns in DV_TABLE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, table_lower):
                detected_type = construct_type
                name_confidence = 0.8
                break
        if detected_type != DVConstructType.UNKNOWN:
            break

    # Analyze columns
    dv_columns = []
    hash_key = None
    business_keys = []
    load_date = None
    record_source = None
    hash_diff = None
    linked_hubs = []

    for col in columns:
        col_name = col.get("name", "").lower()
        col_type = col.get("data_type", "")
        is_pk = col.get("is_primary_key", False)

        role = _detect_column_role(col_name)

        dv_columns.append(DVColumn(
            name=col_name,
            data_type=col_type,
            role=role,
            is_primary_key=is_pk
        ))

        if role == "hash_key":
            # First hash key found (usually PK) is THE hash key
            if hash_key is None and is_pk:
                hash_key = col_name
            elif hash_key is None:
                hash_key = col_name
        elif role == "business_key":
            business_keys.append(col_name)
        elif role == "load_date":
            load_date = col_name
        elif role == "record_source":
            record_source = col_name
        elif role == "hash_diff":
            hash_diff = col_name

    # Second pass: detect linked hubs from non-PK hash key columns
    for col in columns:
        col_name = col.get("name", "").lower()
        is_pk = col.get("is_primary_key", False)

        if col_name.startswith("hk_") and col_name != hash_key and not is_pk:
            hub_match = re.search(r"^hk_(\w+)$", col_name)
            if hub_match:
                linked_hubs.append(hub_match.group(1))

    # Validate and adjust detection
    issues = []
    recommendations = []
    column_confidence = 0.0

    if detected_type == DVConstructType.HUB:
        if hash_key:
            column_confidence += 0.3
        else:
            issues.append("Hub missing hash key column")
            recommendations.append("Add hash key column (e.g., hk_customer)")

        if business_keys:
            column_confidence += 0.3
        else:
            issues.append("Hub missing business key column(s)")
            recommendations.append("Add business key column(s)")

        if load_date:
            column_confidence += 0.2
        else:
            issues.append("Missing load_date column")
            recommendations.append("Add load_date or ldts column")

        if record_source:
            column_confidence += 0.2
        else:
            issues.append("Missing record_source column")
            recommendations.append("Add record_source or rsrc column")

    elif detected_type == DVConstructType.LINK:
        if hash_key:
            column_confidence += 0.25

        if len(linked_hubs) >= 2:
            column_confidence += 0.35
        elif len(linked_hubs) == 1:
            column_confidence += 0.15
            issues.append("Link should connect at least 2 Hubs")

        if load_date:
            column_confidence += 0.2

        if record_source:
            column_confidence += 0.2

    elif detected_type == DVConstructType.SATELLITE:
        if hash_key:
            column_confidence += 0.25

        if load_date:
            column_confidence += 0.25

        if hash_diff:
            column_confidence += 0.25

        if record_source:
            column_confidence += 0.25

        # Satellites should have descriptive attributes
        attr_count = sum(1 for c in dv_columns if c.role == "attribute")
        if attr_count == 0:
            issues.append("Satellite has no descriptive attributes")

    # Infer type from columns if name didn't match
    if detected_type == DVConstructType.UNKNOWN:
        if hash_key and business_keys and not linked_hubs:
            detected_type = DVConstructType.HUB
            name_confidence = 0.5
            recommendations.append(f"Consider renaming to hub_{table_name}")
        elif linked_hubs and len(linked_hubs) >= 2:
            detected_type = DVConstructType.LINK
            name_confidence = 0.5
            recommendations.append(f"Consider renaming to link_{table_name}")
        elif hash_diff and load_date:
            detected_type = DVConstructType.SATELLITE
            name_confidence = 0.5
            recommendations.append(f"Consider renaming to sat_{table_name}")

    # Calculate overall confidence
    confidence = min(1.0, name_confidence + column_confidence)

    # Determine parent hub for satellites
    parent_hub = None
    if detected_type == DVConstructType.SATELLITE and linked_hubs:
        parent_hub = linked_hubs[0]

    return DVConstruct(
        name=table_name,
        construct_type=detected_type,
        confidence=confidence,
        columns=dv_columns,
        business_keys=business_keys,
        hash_key=hash_key,
        load_date_column=load_date,
        record_source_column=record_source,
        linked_hubs=linked_hubs,
        parent_hub=parent_hub,
        issues=issues,
        recommendations=recommendations
    )


def _detect_column_role(col_name: str) -> str:
    """Detect the role of a column based on naming patterns."""
    col_lower = col_name.lower()

    for role, patterns in DV_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, col_lower):
                return role

    # Check for foreign key to hub
    if re.search(r"^hk_", col_lower):
        return "fk"

    return "attribute"


def validate_dv_model(
    constructs: List[DVConstruct]
) -> Dict[str, any]:
    """
    Validate a complete Data Vault model.

    Args:
        constructs: List of detected constructs

    Returns:
        Validation report
    """
    report = {
        "valid": True,
        "hubs": [],
        "links": [],
        "satellites": [],
        "issues": [],
        "warnings": []
    }

    hubs = {c.name: c for c in constructs if c.construct_type == DVConstructType.HUB}
    links = {c.name: c for c in constructs if c.construct_type == DVConstructType.LINK}
    satellites = {c.name: c for c in constructs if c.construct_type == DVConstructType.SATELLITE}

    report["hubs"] = list(hubs.keys())
    report["links"] = list(links.keys())
    report["satellites"] = list(satellites.keys())

    # Validate Links reference existing Hubs
    for link_name, link in links.items():
        for hub_ref in link.linked_hubs:
            # Try to find matching hub
            found = any(hub_ref in h.lower() for h in hubs.keys())
            if not found:
                report["issues"].append(
                    f"Link '{link_name}' references Hub '{hub_ref}' which was not found"
                )
                report["valid"] = False

    # Validate Satellites have parent Hub or Link
    for sat_name, sat in satellites.items():
        if sat.parent_hub:
            found = any(sat.parent_hub in h.lower() for h in hubs.keys())
            found = found or any(sat.parent_hub in l.lower() for l in links.keys())
            if not found:
                report["warnings"].append(
                    f"Satellite '{sat_name}' parent '{sat.parent_hub}' not found in model"
                )

    # Check for orphan Hubs (no satellites)
    hubs_with_sats = set()
    for sat in satellites.values():
        if sat.parent_hub:
            hubs_with_sats.add(sat.parent_hub)

    for hub_name in hubs.keys():
        if not any(hub_name.lower() in h for h in hubs_with_sats):
            report["warnings"].append(
                f"Hub '{hub_name}' has no associated Satellites"
            )

    # Check for common issues
    for construct in constructs:
        for issue in construct.issues:
            report["issues"].append(f"{construct.name}: {issue}")

    if report["issues"]:
        report["valid"] = False

    return report


def generate_dv_ddl(
    construct: DVConstruct,
    dialect: str = "snowflake"
) -> str:
    """
    Generate DDL for a Data Vault construct.

    Args:
        construct: DVConstruct to generate DDL for
        dialect: SQL dialect

    Returns:
        DDL statement
    """
    lines = [f"-- {construct.construct_type.value.upper()}: {construct.name}"]

    if construct.construct_type == DVConstructType.HUB:
        lines.append(f"CREATE TABLE {construct.name} (")

        # Hash key
        if construct.hash_key:
            lines.append(f"    {construct.hash_key} BINARY(32) NOT NULL,")

        # Business keys
        for bk in construct.business_keys:
            lines.append(f"    {bk} VARCHAR NOT NULL,")

        # Standard columns
        lines.append(f"    {construct.load_date_column or 'load_date'} TIMESTAMP NOT NULL,")
        lines.append(f"    {construct.record_source_column or 'record_source'} VARCHAR NOT NULL,")

        # Primary key
        lines.append(f"    PRIMARY KEY ({construct.hash_key or 'hash_key'})")
        lines.append(");")

    elif construct.construct_type == DVConstructType.LINK:
        lines.append(f"CREATE TABLE {construct.name} (")

        # Link hash key
        if construct.hash_key:
            lines.append(f"    {construct.hash_key} BINARY(32) NOT NULL,")

        # Hub foreign keys
        for hub in construct.linked_hubs:
            lines.append(f"    hk_{hub} BINARY(32) NOT NULL,")

        # Standard columns
        lines.append(f"    {construct.load_date_column or 'load_date'} TIMESTAMP NOT NULL,")
        lines.append(f"    {construct.record_source_column or 'record_source'} VARCHAR NOT NULL,")

        # Primary key
        lines.append(f"    PRIMARY KEY ({construct.hash_key or 'hash_key'})")
        lines.append(");")

    elif construct.construct_type == DVConstructType.SATELLITE:
        lines.append(f"CREATE TABLE {construct.name} (")

        # Parent hash key
        if construct.hash_key:
            lines.append(f"    {construct.hash_key} BINARY(32) NOT NULL,")

        # Load date (part of PK for satellites)
        lines.append(f"    {construct.load_date_column or 'load_date'} TIMESTAMP NOT NULL,")

        # Attributes
        for col in construct.columns:
            if col.role == "attribute":
                lines.append(f"    {col.name} {col.data_type or 'VARCHAR'},")

        # Hash diff
        lines.append("    hash_diff BINARY(32),")

        # Record source
        lines.append(f"    {construct.record_source_column or 'record_source'} VARCHAR NOT NULL,")

        # Primary key (hash_key + load_date)
        pk_cols = f"{construct.hash_key or 'hash_key'}, {construct.load_date_column or 'load_date'}"
        lines.append(f"    PRIMARY KEY ({pk_cols})")
        lines.append(");")

    return "\n".join(lines)


def suggest_dv_structure(
    entities: List[Dict[str, any]]
) -> List[Dict[str, any]]:
    """
    Suggest Data Vault structure from entity list.

    Args:
        entities: List of entities with 'name', 'columns', 'relationships'

    Returns:
        Suggested DV structure
    """
    suggestions = []

    for entity in entities:
        name = entity.get("name", "")
        columns = entity.get("columns", [])

        # Entities with natural keys become Hubs
        pk_columns = [c for c in columns if c.get("is_primary_key")]
        if pk_columns:
            hub_name = f"hub_{name}"
            suggestions.append({
                "type": "hub",
                "name": hub_name,
                "business_keys": [c["name"] for c in pk_columns],
                "source_entity": name
            })

            # Descriptive attributes go to Satellite
            attr_columns = [c for c in columns if not c.get("is_primary_key")]
            if attr_columns:
                sat_name = f"sat_{name}"
                suggestions.append({
                    "type": "satellite",
                    "name": sat_name,
                    "parent_hub": hub_name,
                    "attributes": [c["name"] for c in attr_columns]
                })

        # Relationships become Links
        relationships = entity.get("relationships", [])
        for rel in relationships:
            if rel.get("cardinality") == "many_to_many":
                link_name = f"link_{name}_{rel.get('target', 'unknown')}"
                suggestions.append({
                    "type": "link",
                    "name": link_name,
                    "linked_hubs": [f"hub_{name}", f"hub_{rel.get('target')}"]
                })

    return suggestions


if __name__ == "__main__":
    print("MDDE Lite - Data Vault Pattern Detection")
    print("=" * 60)

    # Test case 1: Hub detection
    print("\n--- Test 1: Hub Detection ---")
    hub_columns = [
        {"name": "hk_customer", "data_type": "BINARY(32)", "is_primary_key": True},
        {"name": "customer_bk", "data_type": "VARCHAR(50)", "is_primary_key": False},
        {"name": "load_date", "data_type": "TIMESTAMP", "is_primary_key": False},
        {"name": "record_source", "data_type": "VARCHAR(100)", "is_primary_key": False}
    ]

    hub = detect_dv_construct("hub_customer", hub_columns)
    print(f"Table: {hub.name}")
    print(f"Type: {hub.construct_type.value}")
    print(f"Confidence: {hub.confidence:.0%}")
    print(f"Hash Key: {hub.hash_key}")
    print(f"Business Keys: {hub.business_keys}")

    # Test case 2: Link detection
    print("\n--- Test 2: Link Detection ---")
    link_columns = [
        {"name": "hk_customer_order", "data_type": "BINARY(32)", "is_primary_key": True},
        {"name": "hk_customer", "data_type": "BINARY(32)", "is_primary_key": False},
        {"name": "hk_order", "data_type": "BINARY(32)", "is_primary_key": False},
        {"name": "load_date", "data_type": "TIMESTAMP", "is_primary_key": False},
        {"name": "record_source", "data_type": "VARCHAR(100)", "is_primary_key": False}
    ]

    link = detect_dv_construct("link_customer_order", link_columns)
    print(f"Table: {link.name}")
    print(f"Type: {link.construct_type.value}")
    print(f"Linked Hubs: {link.linked_hubs}")

    # Test case 3: Satellite detection
    print("\n--- Test 3: Satellite Detection ---")
    sat_columns = [
        {"name": "hk_customer", "data_type": "BINARY(32)", "is_primary_key": False},
        {"name": "load_date", "data_type": "TIMESTAMP", "is_primary_key": True},
        {"name": "customer_name", "data_type": "VARCHAR(100)", "is_primary_key": False},
        {"name": "email", "data_type": "VARCHAR(255)", "is_primary_key": False},
        {"name": "hash_diff", "data_type": "BINARY(32)", "is_primary_key": False},
        {"name": "record_source", "data_type": "VARCHAR(100)", "is_primary_key": False}
    ]

    sat = detect_dv_construct("sat_customer_details", sat_columns)
    print(f"Table: {sat.name}")
    print(f"Type: {sat.construct_type.value}")
    print(f"Parent Hub: {sat.parent_hub}")

    # Test case 4: Validation
    print("\n--- Test 4: Model Validation ---")
    constructs = [hub, link, sat]
    report = validate_dv_model(constructs)
    print(f"Valid: {report['valid']}")
    print(f"Hubs: {report['hubs']}")
    print(f"Links: {report['links']}")
    print(f"Satellites: {report['satellites']}")
    if report['warnings']:
        print("Warnings:")
        for w in report['warnings']:
            print(f"  - {w}")

    # Test case 5: DDL Generation
    print("\n--- Test 5: DDL Generation ---")
    ddl = generate_dv_ddl(hub)
    print(ddl)
