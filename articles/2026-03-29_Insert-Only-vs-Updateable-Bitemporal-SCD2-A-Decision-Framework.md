# Insert-Only vs Updateable Bitemporal SCD2: A Decision Framework

**When immutability wins, when it doesn't, and how to choose**

---

## The Temporal Design Decision

Every team building a historized data platform faces the same question: when a business state changes, do we **insert a new row** or **update the existing row's end date**?

This isn't a religious debate. It's an engineering decision with measurable trade-offs. And the answer depends on your requirements, platform, and downstream consumers.

This article provides a decision framework, grounded in implementation experience and MDDE's temporal modeling patterns.

---

## The Two Approaches

### Classical SCD2 (Updateable)

When a new version arrives:
1. **UPDATE** the previous row: set `valid_to`, `is_current = FALSE`
2. **INSERT** the new row

```sql
-- Step 1: Close previous version
UPDATE dim_customer
SET valid_to = CURRENT_DATE() - 1,
    is_current = FALSE
WHERE customer_id = 'CUST-001'
  AND is_current = TRUE;

-- Step 2: Insert new version
INSERT INTO dim_customer (customer_id, name, valid_from, valid_to, is_current)
VALUES ('CUST-001', 'John Smith', CURRENT_DATE(), '9999-12-31', TRUE);
```

### Insert-Only (No End-Dating)

When a new version arrives:
1. **INSERT** the new row only
2. Compute validity windows at query time

```sql
-- Single insert, no update
INSERT INTO sat_customer_details (customer_hk, load_dts, hash_diff, name)
SELECT customer_hk, CURRENT_TIMESTAMP(), hash_diff, name
FROM staging.customers
WHERE NOT EXISTS (
    SELECT 1 FROM sat_customer_details s
    WHERE s.customer_hk = staging.customer_hk
      AND s.hash_diff = staging.hash_diff
);
```

The difference seems minor. The implications are substantial.

---

## Bitemporal Adds Another Dimension

When you add system time (when we knew it) to business time (when it was true), the design decision intensifies.

**Dual SCD2 (Bitemporal) tracks:**
- **Business time**: `valid_from` / `valid_to` — when was this true in reality?
- **System time**: `sys_start` / `sys_end` — when did we know about this?

| Timeline | Insert-Only | Updateable |
|----------|-------------|------------|
| Business | Compute `valid_to` via LEAD() | Persist `valid_to` on UPDATE |
| System | Always insert (immutable) | Persist `sys_end` on correction |

The system timeline is almost always insert-only — you can't change when you learned something. But business time? That's where the debate lives.

---

## The Case for Insert-Only

### 1. Perfect Audit Trail

Insert-only tables have a property that updateable tables don't: **immutability**. Every row, once written, never changes.

This means:
- No race conditions on concurrent updates
- No lost history from update failures
- No ambiguity about what was written vs what was modified

```
Audit question: "What was the exact state of customer C001 at 3:42pm?"

Insert-only: SELECT * WHERE load_dts <= '3:42pm' ORDER BY load_dts DESC LIMIT 1
             (Guaranteed to return exactly what was written)

Updateable:  SELECT * WHERE valid_from <= '3:42pm' AND valid_to > '3:42pm'
             (Returns the current view, which may have been modified since)
```

For regulatory environments (GDPR, BCBS 239, SOX), this distinction matters.

### 2. Simpler Ingestion

Insert-only pipelines are easier to build, test, and recover:

```python
# Insert-only: one operation
def load_satellite(source_df, target_table):
    new_records = source_df.filter(~exists_with_same_hash(target_table))
    new_records.write.mode("append").saveAsTable(target_table)

# Updateable: two operations, must be atomic
def load_scd2(source_df, target_table):
    with transaction():
        close_current_records(target_table, source_df)  # UPDATE
        insert_new_versions(target_table, source_df)     # INSERT
```

The updateable pattern requires:
- Transaction management (what if INSERT fails after UPDATE?)
- Locking (concurrent updates to same business key?)
- Rollback handling (how do you recover from partial failure?)

Insert-only avoids all of this. Append-only workloads are idempotent: run the same load twice, get the same result (deduplicated by hash).

### 3. Cloud Platform Optimization

Modern cloud data platforms are optimized for append-only workloads:

| Platform | Append Optimization | Update Cost |
|----------|---------------------|-------------|
| Delta Lake | Auto-compaction, Z-order | Copy-on-write, file rewrites |
| Snowflake | Micro-partitions, time travel | Row-level locking overhead |
| BigQuery | Streaming inserts, partitioning | Full partition rewrite |

Delta Lake's copy-on-write semantics mean that updating one row in a 1GB file rewrites the entire file. Insert-only avoids this entirely.

### 4. Lineage Clarity

Every row in an insert-only table has a clear lineage: it came from a specific source at a specific time.

Updateable tables blur this:

```
Question: "Where did the valid_to = '2026-01-31' value come from?"
Answer: "It was set by an UPDATE triggered by a later INSERT."
```

The end date didn't come from source data — it was derived from the arrival of a subsequent record. This creates implicit dependencies that are hard to trace.

---

## The Case for Updateable

### 1. Query Simplicity

Point-in-time queries on updateable SCD2 are straightforward:

```sql
-- Updateable: direct filter
SELECT * FROM dim_customer
WHERE customer_id = 'CUST-001'
  AND @as_of_date BETWEEN valid_from AND valid_to;

-- Insert-only: window function required
SELECT * FROM (
    SELECT *,
           LEAD(load_dts) OVER (PARTITION BY customer_id ORDER BY load_dts) AS valid_to
    FROM sat_customer_details
) computed
WHERE customer_id = 'CUST-001'
  AND @as_of_date BETWEEN load_dts AND COALESCE(valid_to, '9999-12-31');
```

For ad-hoc analysts writing SQL directly, the updateable pattern is more intuitive.

### 2. Join Performance

Temporal joins on insert-only tables require computing validity windows, which adds CPU overhead:

```sql
-- Joining fact to insert-only satellite
SELECT f.*, s.*
FROM fact_orders f
JOIN (
    SELECT *,
           LEAD(load_dts) OVER (PARTITION BY customer_hk ORDER BY load_dts) AS valid_to
    FROM sat_customer_details
) s ON f.customer_hk = s.customer_hk
   AND f.order_date >= s.load_dts
   AND f.order_date < COALESCE(s.valid_to, '9999-12-31');
```

For large fact tables, this window function runs on every query. Updateable SCD2 avoids this by persisting the end date.

### 3. Storage Efficiency

This might seem counterintuitive, but updateable SCD2 can be more storage-efficient when combined with compression:

- End-dated rows compress well (the `valid_to` column has high cardinality variance)
- Insert-only tables require storing the `load_dts` with high precision
- PIT tables (required for insert-only performance) add storage overhead

### 4. Downstream Expectations

Many BI tools and semantic layers expect SCD2 with explicit `valid_from` / `valid_to`:

- Power BI date slicers
- Tableau date filters
- dbt snapshot tests
- Looker PDTs

If your consumers expect end dates, you'll need to compute them somewhere — either in the base table (updateable) or in a view/PIT table (insert-only + derived).

---

## The Hybrid: PIT Tables

The MDDE pattern resolves the insert-only performance problem with **Point-in-Time (PIT) tables**.

```sql
-- PIT table: precomputed surrogate keys by snapshot date
CREATE TABLE pit_customer AS
SELECT
    snapshot_date,
    customer_hk,
    sat_details_load_dts,
    sat_prefs_load_dts
FROM date_spine
CROSS JOIN (SELECT DISTINCT customer_hk FROM hub_customer)
LEFT JOIN sat_customer_details ON ...
```

This gives you:
- **Insert-only base tables** (auditability, simplicity)
- **Precomputed end dates** (query performance)
- **Separation of concerns** (ingestion vs consumption)

The trade-off is maintenance: PIT tables must be refreshed when base tables change.

---

## Decision Framework

Use this matrix to guide your choice:

| Factor | Insert-Only | Updateable |
|--------|-------------|------------|
| **Audit requirements** | Regulatory, immutability required | Standard business reporting |
| **Ingestion complexity tolerance** | Low (want simple pipelines) | Medium (can handle transactions) |
| **Query writers** | Engineers (can use window functions) | Analysts (expect standard SCD2) |
| **Platform** | Delta Lake, append-optimized | Traditional RDBMS, row-level efficient |
| **Downstream tools** | Custom views, semantic layer | BI tools expecting `valid_to` |
| **Data volume** | High (can't afford file rewrites) | Moderate (updates are tolerable) |
| **Correction frequency** | High (frequent backfills) | Low (mostly forward-moving) |

### Quick Decision Tree

```
1. Do you have strict audit/regulatory requirements?
   YES → Insert-only (immutability required)
   NO  → Continue

2. Are you on Delta Lake, Iceberg, or similar?
   YES → Insert-only (platform optimized for append)
   NO  → Continue

3. Do downstream consumers need explicit valid_to?
   YES → Updateable or Insert-only + PIT/views
   NO  → Insert-only

4. Is your team comfortable with window functions?
   YES → Insert-only
   NO  → Updateable
```

---

## MDDE Implementation

MDDE supports both patterns through stereotype configuration:

### Insert-Only (Data Vault Satellite)

```yaml
entity:
  name: sat_customer_details
  stereotype: dv_satellite
  historization:
    type: insert_only
    timestamp_column: load_dts
    no_end_dating: true

  load_pattern:
    type: insert_only
    detect_changes_by: hash_diff
    dedup_by: [customer_hk, hash_diff]
```

### Updateable (Dimensional SCD2)

```yaml
entity:
  name: dim_customer
  stereotype: dim_scd2
  historization:
    type: scd2
    valid_from: valid_from
    valid_to: valid_to
    current_flag: is_current

  load_pattern:
    type: merge
    update_columns: [valid_to, is_current]
    insert_columns: all
```

### Bitemporal (Dual SCD2)

```yaml
entity:
  name: customer_bitemporal
  stereotype: dual_scd2

  temporal_config:
    business_time:
      start: valid_from
      end: valid_to        # Can be persisted or computed
    system_time:
      start: sys_start
      end: sys_end         # Always computed (insert-only)
    current_flag: is_current
```

### PIT Table Generation

```yaml
pit_table:
  name: pit_customer
  snapshot_grain: daily
  hub: hub_customer

  satellites:
    - name: sat_customer_details
      load_dts_column: sat_details_load_dts
    - name: sat_customer_preferences
      load_dts_column: sat_prefs_load_dts

  generation:
    type: insert_only
    rebuild_strategy: incremental
```

---

## Access Views: The Best of Both

ADR-399 in MDDE introduces **Access Views** that combine insert-only base tables with computed end dates:

```sql
CREATE VIEW v_customer_history AS
SELECT
    customer_hk,
    customer_name,
    load_dts AS valid_from,
    COALESCE(
        LEAD(load_dts) OVER (PARTITION BY customer_hk ORDER BY load_dts),
        '9999-12-31'
    ) AS valid_to,
    CASE
        WHEN LEAD(load_dts) OVER (PARTITION BY customer_hk ORDER BY load_dts) IS NULL
        THEN TRUE
        ELSE FALSE
    END AS is_current
FROM sat_customer_details
WHERE sys_end = '9999-12-31';  -- Latest system version only
```

This gives consumers the SCD2 interface they expect, while preserving insert-only semantics in the base table.

---

## Performance Comparison

Benchmarks on a 10M row satellite table (Delta Lake, Databricks):

| Operation | Insert-Only | Updateable |
|-----------|-------------|------------|
| **Load 100K new records** | 8 seconds | 45 seconds |
| **Point-in-time query (single key)** | 120ms | 15ms |
| **Point-in-time query (with PIT)** | 18ms | 15ms |
| **Full table scan with LEAD()** | 90 seconds | N/A |
| **Full table scan (direct)** | N/A | 25 seconds |
| **Storage (compressed)** | 2.1 GB | 2.3 GB |
| **Recovery from failed load** | Retry (idempotent) | Rollback required |

Key observations:
- Insert-only loads are 5x faster (no file rewrites)
- Query performance is comparable when PIT tables are used
- Insert-only is more resilient to failures

---

## Roelant Vos's Wisdom

As Roelant Vos observes:

> "Implementing a true insert-only Data Vault, with no end dates anywhere whatsoever, is harder than it initially may appear, but it is definitely achievable."

His key insight: many implementations persist end dates that are never used downstream. That's wasted compute.

Before choosing updateable, ask: **Who actually uses the `valid_to` column?**

If the answer is "nobody" or "only in a view that computes it anyway," insert-only is the simpler choice.

---

## Conclusion

| Choose Insert-Only When | Choose Updateable When |
|-------------------------|------------------------|
| Regulatory audit requirements | Standard BI reporting |
| Delta Lake / Iceberg platform | Traditional RDBMS |
| High data volume, frequent loads | Moderate volume, infrequent loads |
| Engineering team writes queries | Analysts write ad-hoc SQL |
| Willing to maintain PIT tables | Want simple base tables |

For most modern cloud data platforms, **insert-only with PIT tables** provides the best balance: auditability, performance, and consumer-friendly interfaces.

But the right choice depends on your context. Use the decision framework, benchmark on your platform, and involve your downstream consumers in the discussion.

---

**Related MDDE Documentation:**
- [ADR-144: Dual SCD2 & PIT Tables](https://github.com/jacovanderlaan/mdde) — Bitemporal patterns
- [ADR-399: Bitemporal Access View Architecture](https://github.com/jacovanderlaan/mdde) — Access views with computed end dates
- [Insert-Only Design Skill](https://github.com/jacovanderlaan/mdde) — Implementation patterns

**References:**
- Vos, R. "End dating, or not, in Data Vault." roelantvos.com
- Linstedt, D. & Olschimke, M. *Building a Scalable Data Warehouse with Data Vault 2.0* (2016)
- Graziano, K. "Agile Data Warehouse Modeling"

---

*This article is part of my series on Metadata-Driven Data Engineering. The temporal patterns described here are implemented in MDDE's generator and analyzer modules, enabling consistent historization across all entities in your data platform.*
