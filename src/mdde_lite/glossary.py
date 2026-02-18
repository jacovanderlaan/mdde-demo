"""
MDDE Lite - Business Glossary

Manage business terms and link them to technical metadata:
- Define business terms with descriptions
- Map terms to entities and attributes
- Generate glossary documentation
- Cross-reference lineage with business meaning

Related articles:
- "Integrating the Business Glossary into Model-Driven Data Engineering"
- "The Semantic Layer of Metadata"
- "BIRD and GenAI: Building a Comprehensive Reporting Dictionary"

This is a simplified version. The full MDDE framework includes:
- Full glossary schema with categories and hierarchies
- Synonym and acronym management
- Data stewardship integration
- GenAI-assisted term extraction
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Any
from enum import Enum
import re
import duckdb


class TermStatus(Enum):
    """Status of a glossary term."""
    DRAFT = "draft"
    APPROVED = "approved"
    DEPRECATED = "deprecated"


class TermCategory(Enum):
    """Category of a glossary term."""
    BUSINESS = "business"      # Business concepts
    TECHNICAL = "technical"    # Technical terms
    METRIC = "metric"          # KPIs and metrics
    DIMENSION = "dimension"    # Dimensional attributes
    ENTITY = "entity"          # Business entities


@dataclass
class GlossaryTerm:
    """A business glossary term."""
    term_id: str
    name: str
    definition: str
    category: TermCategory = TermCategory.BUSINESS
    status: TermStatus = TermStatus.DRAFT
    synonyms: List[str] = field(default_factory=list)
    related_terms: List[str] = field(default_factory=list)
    data_steward: Optional[str] = None
    examples: List[str] = field(default_factory=list)
    source: Optional[str] = None


@dataclass
class TermMapping:
    """Mapping between glossary term and technical metadata."""
    term_id: str
    entity_id: Optional[str] = None
    attribute_id: Optional[str] = None
    mapping_type: str = "exact"  # exact, partial, derived
    confidence: float = 1.0
    notes: str = ""


class BusinessGlossary:
    """
    Manages business glossary terms and their mappings to metadata.
    """

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        """
        Initialize the glossary.

        Args:
            conn: Optional DuckDB connection for metadata integration
        """
        self.terms: Dict[str, GlossaryTerm] = {}
        self.mappings: List[TermMapping] = []
        self.conn = conn

        if conn:
            self._ensure_glossary_tables()

    def _ensure_glossary_tables(self):
        """Create glossary tables if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS glossary_term (
                term_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                definition VARCHAR,
                category VARCHAR DEFAULT 'business',
                status VARCHAR DEFAULT 'draft',
                synonyms VARCHAR,  -- JSON array
                related_terms VARCHAR,  -- JSON array
                data_steward VARCHAR,
                source VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS glossary_mapping (
                mapping_id VARCHAR PRIMARY KEY,
                term_id VARCHAR NOT NULL,
                entity_id VARCHAR,
                attribute_id VARCHAR,
                mapping_type VARCHAR DEFAULT 'exact',
                confidence FLOAT DEFAULT 1.0,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def add_term(self, term: GlossaryTerm) -> None:
        """Add a term to the glossary."""
        self.terms[term.term_id] = term

        if self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO glossary_term
                (term_id, name, definition, category, status, synonyms, data_steward, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                term.term_id, term.name, term.definition,
                term.category.value, term.status.value,
                ",".join(term.synonyms), term.data_steward, term.source
            ])

    def get_term(self, term_id: str) -> Optional[GlossaryTerm]:
        """Get a term by ID."""
        return self.terms.get(term_id)

    def search_terms(
        self,
        query: str,
        include_synonyms: bool = True,
        category: Optional[TermCategory] = None
    ) -> List[GlossaryTerm]:
        """
        Search for terms matching a query.

        Args:
            query: Search string
            include_synonyms: Also search in synonyms
            category: Filter by category

        Returns:
            List of matching terms
        """
        query_lower = query.lower()
        results = []

        for term in self.terms.values():
            if category and term.category != category:
                continue

            # Match name
            if query_lower in term.name.lower():
                results.append(term)
                continue

            # Match definition
            if query_lower in term.definition.lower():
                results.append(term)
                continue

            # Match synonyms
            if include_synonyms:
                for syn in term.synonyms:
                    if query_lower in syn.lower():
                        results.append(term)
                        break

        return results

    def add_mapping(self, mapping: TermMapping) -> None:
        """Add a term-to-metadata mapping."""
        self.mappings.append(mapping)

        if self.conn:
            mapping_id = f"{mapping.term_id}_{mapping.entity_id or ''}_{mapping.attribute_id or ''}"
            self.conn.execute("""
                INSERT OR REPLACE INTO glossary_mapping
                (mapping_id, term_id, entity_id, attribute_id, mapping_type, confidence, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                mapping_id, mapping.term_id, mapping.entity_id,
                mapping.attribute_id, mapping.mapping_type,
                mapping.confidence, mapping.notes
            ])

    def get_mappings_for_term(self, term_id: str) -> List[TermMapping]:
        """Get all mappings for a term."""
        return [m for m in self.mappings if m.term_id == term_id]

    def get_terms_for_entity(self, entity_id: str) -> List[GlossaryTerm]:
        """Get all terms mapped to an entity."""
        term_ids = {m.term_id for m in self.mappings if m.entity_id == entity_id}
        return [self.terms[tid] for tid in term_ids if tid in self.terms]

    def get_terms_for_attribute(self, attribute_id: str) -> List[GlossaryTerm]:
        """Get all terms mapped to an attribute."""
        term_ids = {m.term_id for m in self.mappings if m.attribute_id == attribute_id}
        return [self.terms[tid] for tid in term_ids if tid in self.terms]

    def auto_map_terms(
        self,
        min_confidence: float = 0.7
    ) -> List[TermMapping]:
        """
        Automatically map glossary terms to metadata based on name matching.

        Args:
            min_confidence: Minimum confidence threshold

        Returns:
            List of suggested mappings
        """
        if not self.conn:
            return []

        suggestions = []

        # Get entities
        entities = self.conn.execute(
            "SELECT entity_id, name, description FROM entity"
        ).fetchall()

        # Get attributes
        attributes = self.conn.execute(
            "SELECT attribute_id, entity_id, name, description FROM attribute"
        ).fetchall()

        for term in self.terms.values():
            term_words = set(term.name.lower().split())
            term_words.update(s.lower() for s in term.synonyms)

            # Match entities
            for entity_id, ent_name, ent_desc in entities:
                confidence = _calculate_match_confidence(
                    term_words, term.name, ent_name, ent_desc
                )
                if confidence >= min_confidence:
                    suggestions.append(TermMapping(
                        term_id=term.term_id,
                        entity_id=entity_id,
                        mapping_type="auto",
                        confidence=confidence,
                        notes="Auto-mapped based on name similarity"
                    ))

            # Match attributes
            for attr_id, ent_id, attr_name, attr_desc in attributes:
                confidence = _calculate_match_confidence(
                    term_words, term.name, attr_name, attr_desc
                )
                if confidence >= min_confidence:
                    suggestions.append(TermMapping(
                        term_id=term.term_id,
                        entity_id=ent_id,
                        attribute_id=attr_id,
                        mapping_type="auto",
                        confidence=confidence,
                        notes="Auto-mapped based on name similarity"
                    ))

        return suggestions

    def generate_glossary_markdown(
        self,
        include_mappings: bool = True,
        group_by_category: bool = True
    ) -> str:
        """
        Generate markdown documentation for the glossary.

        Args:
            include_mappings: Include technical mappings
            group_by_category: Group terms by category

        Returns:
            Markdown string
        """
        lines = [
            "# Business Glossary",
            "",
            f"*{len(self.terms)} terms defined*",
            ""
        ]

        if group_by_category:
            # Group by category
            by_category: Dict[TermCategory, List[GlossaryTerm]] = {}
            for term in sorted(self.terms.values(), key=lambda t: t.name):
                cat = term.category
                if cat not in by_category:
                    by_category[cat] = []
                by_category[cat].append(term)

            for category in TermCategory:
                if category in by_category:
                    lines.append(f"## {category.value.title()} Terms")
                    lines.append("")

                    for term in by_category[category]:
                        lines.extend(self._format_term_markdown(term, include_mappings))
                    lines.append("")
        else:
            # Alphabetical list
            for term in sorted(self.terms.values(), key=lambda t: t.name):
                lines.extend(self._format_term_markdown(term, include_mappings))

        return "\n".join(lines)

    def _format_term_markdown(
        self,
        term: GlossaryTerm,
        include_mappings: bool
    ) -> List[str]:
        """Format a single term as markdown."""
        lines = [
            f"### {term.name}",
            "",
            f"**Definition:** {term.definition}",
            ""
        ]

        if term.synonyms:
            lines.append(f"**Synonyms:** {', '.join(term.synonyms)}")
            lines.append("")

        if term.examples:
            lines.append("**Examples:**")
            for ex in term.examples:
                lines.append(f"- {ex}")
            lines.append("")

        if term.data_steward:
            lines.append(f"**Data Steward:** {term.data_steward}")
            lines.append("")

        if include_mappings:
            mappings = self.get_mappings_for_term(term.term_id)
            if mappings:
                lines.append("**Technical Mappings:**")
                for m in mappings:
                    if m.attribute_id:
                        lines.append(f"- `{m.entity_id}.{m.attribute_id}` ({m.mapping_type}, {m.confidence:.0%})")
                    elif m.entity_id:
                        lines.append(f"- `{m.entity_id}` ({m.mapping_type}, {m.confidence:.0%})")
                lines.append("")

        lines.append(f"*Status: {term.status.value}*")
        lines.append("")

        return lines

    def export_to_yaml(self) -> str:
        """Export glossary to YAML format."""
        import yaml

        data = {
            "glossary": {
                "terms": []
            }
        }

        for term in self.terms.values():
            term_data = {
                "term_id": term.term_id,
                "name": term.name,
                "definition": term.definition,
                "category": term.category.value,
                "status": term.status.value
            }
            if term.synonyms:
                term_data["synonyms"] = term.synonyms
            if term.related_terms:
                term_data["related_terms"] = term.related_terms
            if term.data_steward:
                term_data["data_steward"] = term.data_steward
            if term.examples:
                term_data["examples"] = term.examples

            data["glossary"]["terms"].append(term_data)

        return yaml.dump(data, default_flow_style=False, sort_keys=False)


def _calculate_match_confidence(
    term_words: Set[str],
    term_name: str,
    target_name: str,
    target_desc: Optional[str]
) -> float:
    """Calculate matching confidence between term and target."""
    target_lower = target_name.lower()
    term_lower = term_name.lower()

    # Exact match
    if target_lower == term_lower:
        return 1.0

    # Substring match
    if term_lower in target_lower or target_lower in term_lower:
        return 0.9

    # Word overlap
    target_words = set(re.split(r'[_\s]+', target_lower))
    overlap = term_words & target_words

    if overlap:
        return 0.7 * len(overlap) / max(len(term_words), len(target_words))

    # Description match
    if target_desc and term_lower in target_desc.lower():
        return 0.6

    return 0.0


def extract_terms_from_metadata(
    conn: duckdb.DuckDBPyConnection,
    category: TermCategory = TermCategory.BUSINESS
) -> List[GlossaryTerm]:
    """
    Extract potential glossary terms from metadata.

    Args:
        conn: DuckDB connection with metadata
        category: Category for extracted terms

    Returns:
        List of suggested glossary terms
    """
    terms = []

    # Extract from entity descriptions
    entities = conn.execute("""
        SELECT entity_id, name, description
        FROM entity
        WHERE description IS NOT NULL AND description != ''
    """).fetchall()

    for entity_id, name, description in entities:
        # Convert entity name to term
        term_name = name.replace("_", " ").title()

        terms.append(GlossaryTerm(
            term_id=f"term_{entity_id}",
            name=term_name,
            definition=description,
            category=category,
            status=TermStatus.DRAFT,
            source=f"entity:{entity_id}"
        ))

    # Extract from attribute descriptions
    attributes = conn.execute("""
        SELECT a.attribute_id, a.name, a.description, e.name as entity_name
        FROM attribute a
        JOIN entity e ON a.entity_id = e.entity_id
        WHERE a.description IS NOT NULL AND a.description != ''
    """).fetchall()

    for attr_id, name, description, entity_name in attributes:
        term_name = name.replace("_", " ").title()

        terms.append(GlossaryTerm(
            term_id=f"term_{attr_id}",
            name=term_name,
            definition=description,
            category=category,
            status=TermStatus.DRAFT,
            source=f"attribute:{attr_id}"
        ))

    return terms


if __name__ == "__main__":
    print("MDDE Lite - Business Glossary Demo")
    print("=" * 50)

    # Create glossary
    glossary = BusinessGlossary()

    # Add sample terms
    glossary.add_term(GlossaryTerm(
        term_id="customer",
        name="Customer",
        definition="An individual or organization that purchases goods or services",
        category=TermCategory.ENTITY,
        status=TermStatus.APPROVED,
        synonyms=["Client", "Buyer", "Account"],
        examples=["Retail customer", "B2B customer"],
        data_steward="Sales Team"
    ))

    glossary.add_term(GlossaryTerm(
        term_id="revenue",
        name="Revenue",
        definition="Total income generated from sales before any deductions",
        category=TermCategory.METRIC,
        status=TermStatus.APPROVED,
        synonyms=["Sales", "Income", "Turnover"],
        related_terms=["gross_revenue", "net_revenue"],
        data_steward="Finance Team"
    ))

    glossary.add_term(GlossaryTerm(
        term_id="churn_rate",
        name="Churn Rate",
        definition="Percentage of customers who stop using the service over a period",
        category=TermCategory.METRIC,
        status=TermStatus.APPROVED,
        synonyms=["Attrition Rate", "Customer Turnover"],
        examples=["Monthly churn = (Lost customers / Start customers) * 100"]
    ))

    glossary.add_term(GlossaryTerm(
        term_id="region",
        name="Region",
        definition="Geographic area for sales and reporting purposes",
        category=TermCategory.DIMENSION,
        status=TermStatus.APPROVED,
        synonyms=["Territory", "Area", "Zone"]
    ))

    # Add mappings
    glossary.add_mapping(TermMapping(
        term_id="customer",
        entity_id="dim_customer",
        mapping_type="exact",
        confidence=1.0
    ))

    glossary.add_mapping(TermMapping(
        term_id="revenue",
        entity_id="fct_sales",
        attribute_id="total_amount",
        mapping_type="derived",
        confidence=0.9,
        notes="Revenue = SUM(total_amount)"
    ))

    # Search demo
    print("\n--- Search for 'sales' ---")
    results = glossary.search_terms("sales")
    for term in results:
        print(f"  Found: {term.name} ({term.category.value})")

    # Generate markdown
    print("\n--- Generated Glossary (excerpt) ---")
    md = glossary.generate_glossary_markdown()
    print(md[:800] + "...")

    # Export YAML
    print("\n--- YAML Export (excerpt) ---")
    yaml_out = glossary.export_to_yaml()
    print(yaml_out[:500] + "...")
