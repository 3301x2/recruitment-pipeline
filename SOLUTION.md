# Solution Architecture & Decisions

## Approach

I chose a **medallion architecture** (bronze → silver → gold) with **Python for extraction** and **dbt for transformation**. This is the industry-standard ELT pattern: Python handles what it's best at (API calls, file parsing), and dbt handles what it's best at (SQL transformations with built-in testing, lineage, and documentation).

## Data Quality Issues Found & Fixed

The raw data had several quality issues that the silver layer resolves:

| Issue | Example | Fix |
|-------|---------|-----|
| Inconsistent department casing | `OPERATIONS`, `marketing` | `INITCAP()` in dbt |
| Inconsistent location casing | `South africa` | `INITCAP()` in dbt |
| Missing departments | 1 row with empty string | Default to `Unknown` via `COALESCE(NULLIF(...), 'Unknown')` |
| Mixed date formats | `MM/DD/YYYY` alongside `YYYY-MM-DD` | Regex pattern matching with `TO_DATE()` |
| Missing URLs | 3 rows | Left as NULL (valid — not all jobs have public URLs) |
| API vs CSV schema mismatch | API has `departments` (JSONB array), CSV has `department` (text) | Extracted first department from JSONB, unified in `stg_jobs` |

## Key Design Decisions

### 1. Python for ingestion, dbt for transformation

**Why**: Each tool does what it's best at. Python's `requests` library and `csv` module handle extraction cleanly. dbt's SQL-first approach with Jinja templating, `ref()` for lineage, and built-in test framework make transformations more maintainable and testable than equivalent Python code.

**Trade-off**: Two tools to understand, but the separation of concerns makes each layer easier to debug, test, and extend independently.

### 2. Star schema for Gold layer

**Why**: Dimension tables (`dim_department`, `dim_location`, `dim_date`) plus a fact table (`fact_jobs`) is the standard for analytical workloads. It enables efficient joins, supports time-series analysis via the date spine, and pre-computes the `days_to_fill` metric.

**Dimensions**:
- `dim_department` — 10 distinct departments with surrogate keys
- `dim_location` — 2 locations (Cape Town, South Africa)
- `dim_date` — date spine (2015 to two years from today) with year, quarter, month, day-of-week, is_weekend

**Fact**: `fact_jobs` — 320 rows with foreign keys to all dimensions, `is_open` flag, and `days_to_fill` calculated from `close_date - open_date`.

### 3. Separate Python venvs for dbt vs application code

**Why**: dbt 1.10's `mashumaro` dependency crashes on Python 3.14 (`UnserializableField` error). Using Python 3.12 in a dedicated `dbt-venv/` solves this without downgrading the main environment.

**Trade-off**: Slightly more complex local setup, but this mirrors production reality where dbt and application code often run in different containers/environments with different Python versions.

### 4. Defense-in-depth testing strategy

Two complementary test layers catch different categories of issues:

| Layer | Tool | Count | What it catches |
|-------|------|-------|-----------------|
| Schema tests | dbt test | 17 | Nulls, uniqueness, referential integrity, valid enums |
| Integration tests | pytest | 12 | Cross-layer consistency, business rules, data contract violations |

### 5. Airflow orchestration

The DAG defines a linear dependency chain: `bronze_ingest → dbt_run → dbt_test → pytest_integration`. Each step fails fast — if ingestion fails, dbt doesn't run on stale data. Retries (2x with 5-minute delay) handle transient API failures.

The Airflow container installs dbt internally, so the Python version compatibility issue is handled within Docker.

### 6. Docker Compose infrastructure

PostgreSQL serves dual purpose: data warehouse (bronze/silver/gold schemas) and Airflow metadata database. This keeps the infrastructure simple for a reviewer — one `docker-compose up -d` starts everything.

## Assumptions

- The Greenhouse API board token (`offerzen`) is public and doesn't require authentication
- Historical CSV is a one-time load (truncate-and-reload pattern is acceptable)
- API jobs without a `close_date` are considered currently open
- The `updated_at` timestamp from the API is used as `open_date` for API-sourced jobs
- Department names after `INITCAP()` are considered canonical (no fuzzy matching needed)

## What I'd Improve With More Time

1. **Incremental models** — Use dbt's `is_incremental()` to avoid full-refresh on every run. Only process new/changed records.
2. **Source freshness** — Add `dbt source freshness` checks to alert if the API hasn't returned new data.
3. **Deduplication** — Currently handled by `UNION ALL` + `ON CONFLICT` in Python. Would be cleaner as a dbt macro.
4. **Alerting** — Airflow email/Slack notifications on DAG failure.
5. **Dashboard** — Connect Metabase or Superset to the gold schema for self-serve analytics.
6. **CI/CD** — GitHub Actions to run `dbt test` and `pytest` on every PR.
7. **Parameterized board token** — Move the Greenhouse board name to an environment variable.
8. **Data contracts** — Add `dbt-expectations` package for more expressive test assertions (e.g., column value ranges, date bounds).
