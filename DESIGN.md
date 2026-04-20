# Design Document — ETL Data Pipeline

**Author:** Data Engineering  
**Date:** February 2026  
**Version:** 1.0

---

## 1. Overview

This document describes the architecture, data model, incremental strategy, data quality approach,
and known trade-offs for the ETL Data Pipeline. The pipeline ingests three CSV feeds —
companies, monthly revenue, and monthly expenses — cleans and validates them, persists clean
records to a relational store, and computes six financial metrics per company per month.

---

## 2. Architecture

### 2.1 Storage Layers (Medallion Architecture)

The pipeline follows a four-zone medallion pattern. Each zone has a single, well-defined
responsibility and a clear promotion gate between zones.

```
CSV Files (raw input)
      │
      ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE  (resources/BRONZE/<entity>/<entity>_<uuid>.csv)         │
│  Raw data written as-is before any transformation.               │
│  One file per pipeline run (identified by pipeline_load_uuid).   │
│  Never modified after write — immutable audit snapshot.          │
└──────────────────────────────────────────────────────────────────┘
      │  preprocess in parallel with BRONZE write
      ▼
┌──────────────────────────────────────────────────────────────────┐
│  CLEANSE  (in-memory pandas)                                     │
│  Type coercion, normalisation, deduplication, orphan detection.  │
│  Splits rows into df_clean and df_orphan.                        │
└──────────────────────────────────────────────────────────────────┘
      │
      ├────────────────────────────────────────┐
      ▼                                        ▼
┌──────────────────────┐          ┌────────────────────────────────┐
│  SILVER              │          │  QUARANTINE                    │
│  Clean rows only.    │          │  Rejected rows with            │
│  Written in parallel │          │  quarantine_reason column.     │
│  with QUARANTINE.    │          │  Written in parallel with      │
│  Both must succeed   │          │  SILVER. See §5 for reasons.   │
│  before DB write.    │          │                                │
└──────────────────────┘          └────────────────────────────────┘
      │  (gate: SILVER + QUARANTINE must both succeed)
      ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLDEN  (SQLite via SQLAlchemy)                                 │
│  dim_company                    — company master dimension       │
│  fact_company_monthly_financials — grain: (company_id, month)    │
│  Upsert: ON CONFLICT DO UPDATE — idempotent re-runs.             │
│  FK: fact → dim_company enforced via PRAGMA foreign_keys = ON.   │
└──────────────────────────────────────────────────────────────────┘
      │
      ▼
┌──────────────────────────────────────────────────────────────────┐
│  METRICS  (in-memory pandas → written back to GOLDEN)           │
│  POST /financials/calculate triggers FinancialService.           │
│  Computes 6 metrics (see §4) and upserts back to                 │
│  fact_company_monthly_financials.                                │
└──────────────────────────────────────────────────────────────────┘

Every zone transition is recorded in:
┌──────────────────────────────────────────────────────────────────┐
│  dataload_audit  (SQLite table)                                  │
│  One row per (load_uuid, stage) — SUCCESS or FAILURE.            │
│  Queryable via GET /audit with filters on entity/stage/status.   │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 Compute

| Layer | Technology | Role |
|---|---|---|
| API / Orchestration | FastAPI + Uvicorn | HTTP trigger per entity; lifecycle management |
| Data processing | pandas | CSV reading, type coercion, cleanse, dedup, metrics |
| Parallel I/O | `concurrent.futures.ThreadPoolExecutor` | BRONZE+preprocess in parallel; SILVER+QUARANTINE in parallel |
| Storage | SQLite (SQLAlchemy ORM) | In-memory DB for local/dev; swap URL for PostgreSQL in prod |
| Audit | `dataload_audit` table | Per-stage write tracking (SUCCESS/FAILURE, row_count, error_msg) |

### 2.3 Orchestration

Currently triggered manually via HTTP POST endpoints in the order:

```
POST /company          ← must run first (revenue/expense orphan check depends on it)
POST /revenue
POST /expense
POST /fx_rate
POST /financials/calculate
```

This is intentionally minimal. In production the same pipeline steps can be wrapped as
**Airflow DAGs** or **AWS Step Functions** tasks — the service methods are already stateless
and idempotent, requiring no orchestration-side changes.

### 2.4 Data Flow (end-to-end)

```
1. Request arrives at POST /<entity>
2. load_uuid = uuid4()  ← unique run identifier
3. Read CSV → stamp pipeline_load_uuid + pipeline_loaded_at into every row
4. Write BRONZE (raw CSV) in parallel with preprocess()         [BRONZE audit row]
5. cleanse_data() → df_clean + df_orphan (with quarantine_reason)
6. Write SILVER (clean) + QUARANTINE (rejected) in parallel     [SILVER + QUARANTINE audit rows]
   ← pipeline aborts here if either write fails
7. Upsert df_clean → GOLDEN (DB)                               [GOLDEN audit row]
8. POST /financials/calculate → compute metrics → upsert back to DB
```

---

## 3. Data Model

### 3.1 `dim_company` (dimension table)

Grain: one row per unique `company_id`.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | Surrogate key, auto-increment |
| `company_id` | VARCHAR(50) UNIQUE NOT NULL | Natural business key |
| `name` | VARCHAR(255) NOT NULL | |
| `industry` | VARCHAR(100) NOT NULL | |
| `country` | VARCHAR(100) NOT NULL | |
| `created_at` | DATETIME | Set to DB default (CSV has no created_at column) |
| `pipeline_loaded_at` | DATETIME NOT NULL | Timestamp of the pipeline run that wrote this row |
| `pipeline_load_uuid` | VARCHAR(255) NOT NULL | UUID of the pipeline run |

**Indexes:** `idx_dim_company_company_id` on `company_id`

### 3.2 `fact_company_monthly_financials` (fact table)

Grain: one row per `(company_id, month)`.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | Surrogate key |
| `company_id` | VARCHAR(50) NOT NULL | FK → `dim_company(company_id)` |
| `month` | VARCHAR(20) NOT NULL | Format: `YYYY-MM` |
| `revenue` | FLOAT | Loaded by revenue pipeline |
| `expenses` | FLOAT | Loaded by expense pipeline |
| `currency` | VARCHAR(20) | Native currency of revenue row |
| `gross_profit` | FLOAT | Computed: `revenue - expenses` |
| `gross_margin_pct` | FLOAT | Computed: `(gross_profit / revenue) * 100` |
| `mom_revenue_growth_pct` | FLOAT | Computed: MoM % change in revenue per company |
| `rolling_3m_revenue` | FLOAT | Computed: trailing 3-month revenue sum per company |
| `revenue_updated_at` | DATE | `updated_at` from source revenue row |
| `expenses_updated_at` | DATE | `updated_at` from source expense row |
| `revenue_pipeline_loaded_at` | DATETIME | Timestamp of revenue pipeline run |
| `expense_pipeline_loaded_at` | DATETIME | Timestamp of expense pipeline run |
| `revenue_pipeline_load_uuid` | VARCHAR(100) | UUID of revenue pipeline run |
| `expense_pipeline_load_uuid` | VARCHAR(100) | UUID of expense pipeline run |
| `updated_at` | DATETIME | DB-managed last-modified timestamp |

**Constraints:** `UNIQUE(company_id, month)`, `FOREIGN KEY (company_id) REFERENCES dim_company(company_id)`  
**Indexes:** `idx_fact_financials_company_id`, `idx_fact_financials_month`

**Design note:** Revenue and expense are loaded independently by separate pipeline runs.
Each upsert only updates its own columns — revenue never overwrites expense columns and
vice versa. Metrics are computed separately after both are loaded.

### 3.3 `dataload_audit` (operational table)

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `entity` | VARCHAR(50) | `company` / `revenue` / `expense` / `fx_rate` |
| `file_name` | VARCHAR(255) | Source CSV basename |
| `load_uuid` | VARCHAR(100) | Pipeline run UUID |
| `stage` | VARCHAR(20) | `BRONZE` / `SILVER` / `QUARANTINE` / `GOLDEN` |
| `status` | VARCHAR(10) | `SUCCESS` / `FAILURE` |
| `row_count` | INTEGER | Rows written in this stage |
| `error_msg` | TEXT | Populated on FAILURE (includes truncated traceback) |
| `run_at` | DATETIME | Wall-clock time of stage completion |

### 3.4 Partitioning Strategy

**Current (SQLite):** SQLite does not support table partitioning. Queries are accelerated
instead by composite indexes on `(company_id)` and `(month)`. For analytical queries spanning
large month ranges the full index scan is acceptable at the current data volume.

**Production recommendation:** When migrating to PostgreSQL, apply:
```sql
-- Range partition by month — each partition covers one calendar year
CREATE TABLE fact_company_monthly_financials (...)
    PARTITION BY RANGE (month);

CREATE TABLE fact_financials_2023 PARTITION OF fact_company_monthly_financials
    FOR VALUES FROM ('2023-01') TO ('2024-01');
CREATE TABLE fact_financials_2024 PARTITION OF fact_company_monthly_financials
    FOR VALUES FROM ('2024-01') TO ('2025-01');
```

**Production recommendation (Iceberg):** See §7 — Trade-offs.

---

## 4. Metrics

All metrics are computed by `POST /financials/calculate` → `FinancialService.compute_metrics()`.
Metrics are only computed for rows where **both** `revenue` and `expenses` are non-null.

| Metric | Column | Formula |
|---|---|---|
| Revenue | `revenue` | Source value from monthly_revenue CSV |
| Expenses | `expenses` | Source value from monthly_expenses CSV |
| Gross Profit | `gross_profit` | `revenue - expenses` |
| Gross Margin % | `gross_margin_pct` | `(gross_profit / revenue) * 100` — `NULL` when `revenue = 0` |
| MoM Revenue Growth % | `mom_revenue_growth_pct` | `((revenue_t - revenue_t-1) / revenue_t-1) * 100` per company, sorted by month |
| 3-Month Rolling Revenue | `rolling_3m_revenue` | `rolling(window=3, min_periods=1).sum()` per company — `NULL` for non-eligible rows |

**Currency note:** Metrics are computed in the **native currency of each revenue row**.
FX normalisation to a single base currency (e.g. USD) is not applied in this implementation.
The `fx_rate` table is loaded and available for future use. This is a documented trade-off — see §7.

---

## 5. Data Quality & Orphan Handling

### 5.1 Invalid Value Detection

Each service's `cleanse_data()` applies the following checks, with **explicit priority**
(highest priority reason wins per row):

| Priority | Reason | Condition |
|---|---|---|
| 1 (highest) | `ORPHAN_COMPANY_ID` | `company_id` is null **or** not present in `dim_company` |
| 2 | `NULL_REVENUE` / `NULL_EXPENSE` | Revenue or expense value is null/missing |
| 3 | `NEGATIVE_REVENUE` / `NEGATIVE_EXPENSE` | Revenue or expense is negative |
| 4 | `NULL_COMPANY_ID` | company_id is null (company pipeline only) |
| post-dedup | `SUPERSEDED_BY_CORRECTION` | Older duplicate per `(company_id, month)` — latest `updated_at` wins |

`ORPHAN_COMPANY_ID` is the highest priority because an orphan record can **never be fixed**
by changing the financial data — it requires the company master to be loaded first.

### 5.2 Quarantine Zone

Every rejected row is written to:
```
resources/QUARANTINE/<entity>/<entity>_<pipeline_load_uuid>.csv
```

The file includes all original columns **plus** a `quarantine_reason` column. This makes
rejected records fully observable and re-processable without re-reading the source.

**Re-processing path for orphans:**
1. Load (or correct) company master via `POST /company`
2. Re-run `POST /revenue` and/or `POST /expense`
3. On the next run, previously-orphaned rows whose `company_id` now exists in `dim_company`
   will be promoted to SILVER and loaded to GOLDEN

### 5.3 Observability

Every zone write (BRONZE / SILVER / QUARANTINE / GOLDEN) records one row in `dataload_audit`
regardless of outcome. This means:

- A failed BRONZE write is visible in the audit table before any DB is touched
- A failed GOLDEN write is visible alongside the successful SILVER/QUARANTINE entries
  for the same `load_uuid`
- `GET /audit?load_uuid=<uuid>` gives a full stage-by-stage timeline for any run
- `GET /audit?stage=GOLDEN&status=FAILURE` shows all failed DB loads across all runs

---

## 6. Incremental Strategy & Idempotency

### 6.1 Current Approach — Idempotent Full-File Upserts

The current implementation is **re-run safe** rather than truly incremental:

- **Upsert pattern:** `ON CONFLICT (company_id, month) DO UPDATE SET ...` — re-running
  the same file twice produces identical results with no duplicates.
- **Run identity:** Every run generates a `pipeline_load_uuid` (UUID4) stamped into
  every row in the DataFrame and every BRONZE/SILVER/QUARANTINE filename. The GOLDEN
  table stores the UUID of the last run that modified each row, providing row-level lineage.
- **Correction handling:** The `updated_at` field from the source CSV is the tie-breaker.
  If two rows share `(company_id, month)`, the one with the later `updated_at` wins;
  the older row is quarantined with `SUPERSEDED_BY_CORRECTION`.
- **Late-arriving records:** A late record with `updated_at` > the value already in the
  database will overwrite it on the next run. A late record with an older `updated_at`
  is quarantined as superseded.

---

## 7. Trade-offs & Production Roadmap

The following limitations are **intentional and documented** for this prototype.
Each has a clear production upgrade path.

### 7.1 Data Volume — Not Addressed

**Current:** All data is loaded into an in-memory SQLite database. This is appropriate
for the prototype dataset but will not scale beyond a few million rows.

**Production path:** Replace SQLite with **Apache Iceberg backed by S3**:
- Iceberg provides ACID transactions, schema evolution, and efficient large-scale reads
  via predicate pushdown on Parquet files.
- The SQLAlchemy session interface can be swapped for an Iceberg catalog (AWS Glue,
  Nessie, or Hive Metastore) with minimal changes to the service layer.
- Estimated migration effort: replace `session.py` and DDL — service business logic
  is unchanged.

### 7.2 Data Ingestion Performance — Not Addressed

**Current:** CSV loading and pandas transformations run on a single FastAPI worker thread.
For the prototype dataset (~500k rows revenue, ~500k rows expense) this is acceptable.

**Production path:** Replace the pandas ETL with **AWS Glue (Apache Spark)**:
- Glue jobs read from S3 directly, partition data during write, and scale horizontally.
- The medallion zone structure (BRONZE/SILVER/QUARANTINE/GOLDEN) maps directly to
  S3 prefixes — no architectural change needed.
- The cleanse logic (orphan detection, dedup, quarantine reasons) translates directly
  to Spark DataFrame operations.

### 7.3 Time Travel — Not Addressed

**Current:** Overwritten rows are permanently gone from the GOLDEN table. BRONZE files
serve as a manual audit trail but there is no point-in-time query capability.

**Production path:** **Apache Iceberg natively supports time travel**:
```sql
-- Query the state of the table as of a specific timestamp
SELECT * FROM fact_company_monthly_financials
    FOR SYSTEM_TIME AS OF '2026-01-15 00:00:00';
```
BRONZE files become redundant for audit purposes once Iceberg is in place.
The `dataload_audit` table provides run-level metadata; Iceberg snapshots provide
row-level history.

### 7.4 Partitioning — Not Addressed

**Current:** SQLite does not support table partitioning. Composite indexes on
`company_id` and `month` are used to accelerate queries.

**Production path:** Two options depending on the chosen storage layer:

- **PostgreSQL:** `PARTITION BY RANGE (month)` — one partition per calendar year.
  Queries filtered on `month` will only scan the relevant partition.
- **Apache Iceberg on S3:** Partitioning is **built-in and transparent**. Define the
  partition spec at table creation:
  ```python
  partitioned_by=["month"]   # Iceberg partition spec
  ```
  Iceberg handles file layout, partition pruning, and metadata automatically.
  No DDL changes needed when adding new partition values (months).

### 7.5 FX Currency Normalisation — Not Addressed

**Current:** Financial metrics (`gross_profit`, `gross_margin_pct`, etc.) are computed
in the **native currency of each revenue row**. Cross-company comparisons are therefore
not directly meaningful as companies trade in different currencies (USD, GBP, EUR, INR, AED…).

The `fx_rate` table is loaded and available. The `dim_company` → `fact_company_monthly_financials`
join is already in place.

**Production path:** Before computing metrics, join `fact_company_monthly_financials`
with `fx_rate` on `(currency, month)` and normalise all monetary values to a single
base currency (e.g. USD). Update `FinancialService.compute_metrics()` to apply the
FX rate before computing gross profit and margin.

---

## 8. Assumptions

1. **Company master must be loaded before revenue/expense.** The orphan check performs
   a DB lookup against `dim_company`. Running revenue/expense first results in all rows
   being quarantined as `ORPHAN_COMPANY_ID`.

2. **`updated_at` in source CSVs is trusted.** The pipeline uses this field as the
   correction tie-breaker. If the source system does not maintain accurate `updated_at`
   values, deduplication logic will need to be revised.

3. **`month` format is `YYYY-MM`.** The pipeline stores month as a string. Rows with
   non-conforming formats will not participate correctly in MoM growth or rolling
   revenue calculations.

4. **Single writer to SQLite.** SQLite supports only one concurrent writer. The pipeline
   executes all DB writes on the main thread after completing parallel file I/O.
   This is correct for SQLite but would need to be revised for PostgreSQL (connection
   pooling already configured in `session.py`).

5. **Metrics are computed on demand.** `POST /financials/calculate` must be called
   explicitly after loading revenue and expense. Metrics are not auto-computed on each
   data load to keep the load and compute steps independently retriable.


