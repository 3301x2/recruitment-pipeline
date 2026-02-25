# AI Tools Usage

I used Claude (Anthropic) as a coding assistant throughout this project. Below are the key prompts and my reasoning for each phase.

## 1. Project Scaffolding

**Prompt:**
> I am building a data pipeline for Greenhouse. Here's the public API: `https://api.greenhouse.io/v1/boards/offerzen/jobs?content=true`. I have placed the historical data at `data/offerzen_jobs_history_raw.csv`. The recommended stack is Python, PostgreSQL, dbt, and a medallion architecture with a star schema. Set up the project structure.

**My thinking:** I wanted to start with a clean project layout that separates concerns — ingestion scripts, dbt models, SQL definitions, and tests in their own directories. I gave the AI the full context upfront (API URL, file location, stack) so it could scaffold everything consistently rather than me wiring things together piecemeal.

## 2. Bronze Ingestion Script

**Prompt:**
> Write an ingestion script that pulls from the Greenhouse API and loads the historical CSV into PostgreSQL bronze tables. Use `psycopg2`, handle errors with transaction rollback, and truncate-before-load for idempotency.

**My thinking:** I chose truncate-and-reload over upsert because with only ~320 records, full refresh is simpler and avoids stale data accumulating. I specifically asked for transaction rollback so a partial API failure wouldn't leave the database in an inconsistent state. The `safe_int()` helper was added after I noticed the CSV had some edge cases with empty numeric fields.

## 3. Data Quality Discovery

**Prompt:**
> Examine the CSV data and identify data quality issues that need to be fixed in the silver layer.

**My thinking:** Before writing transformations, I wanted to understand what was actually wrong with the data. This revealed six issues: inconsistent casing (`OPERATIONS` vs `marketing`), mixed date formats (`MM/DD/YYYY` vs `YYYY-MM-DD`), missing departments, blank URLs, and the schema mismatch between the API's JSONB departments array and the CSV's flat text field. Understanding these upfront shaped the entire silver model design.

## 4. dbt Silver Model

**Prompt:**
> Create a dbt staging model `stg_jobs` that merges data from both bronze sources, fixes the data quality issues we found, and tracks the data source (api vs history).

**My thinking:** I chose to handle all cleaning in SQL/dbt rather than Python because: (a) it keeps the bronze layer as a true raw copy, (b) dbt gives me lineage tracking via `ref()` and `source()`, and (c) the fixes are declarative SQL (`INITCAP()`, `COALESCE`, regex date parsing) which is easier to audit than imperative Python. The `UNION ALL` approach with different column mappings for each source felt cleaner than trying to make one generic loader handle both schemas.

## 5. Gold Star Schema

**Prompt:**
> Build a star schema in the gold layer: dim_department, dim_location, dim_date (date spine 2015-2026), and fact_jobs with foreign keys, an is_open flag, and a pre-computed days_to_fill metric.

**My thinking:** A star schema was the natural choice for analytical queries. I specifically asked for a date spine because the assignment mentions "historical and trend analytics" — without a continuous date dimension, time-series queries (seasonal hiring trends, monthly volumes) would have gaps. The `days_to_fill` pre-computation avoids repeated `close_date - open_date` calculations in every downstream query.

## 6. Testing Strategy

**Prompt:**
> Add dbt schema tests (not_null, unique, accepted_values, relationships) and pytest integration tests that validate cross-layer consistency.

**My thinking:** I wanted two complementary test layers. dbt tests catch structural issues (nulls, broken foreign keys, invalid enums) at the model level. pytest tests catch business logic violations that span layers — e.g., "the fact table row count should match silver," "open jobs must not have a close date," "days_to_fill must be non-negative." Together they provide defense-in-depth.

## 7. Airflow Orchestration

**Prompt:**
> Create an Airflow DAG with a linear dependency chain: ingest, dbt run, dbt test, pytest. Include retries and Docker Compose setup.

**My thinking:** Linear dependencies enforce a fail-fast strategy — if bronze ingestion fails, dbt won't run on stale data. I chose 2 retries with 5-minute delay to handle transient API failures without masking real issues. Docker Compose was the simplest way to make the project reproducible — one command starts both PostgreSQL and Airflow.

## 8. Analytics Queries

**Prompt:**
> Write SQL queries demonstrating recruitment metrics: open positions, historical counts by department/location, time-to-fill analysis, seasonal trends, and fill rates.

**My thinking:** The assignment specifically asks for open jobs, department/location counts, and time-to-fill, so those were the baseline. I added seasonal trends (quarterly) and fill rate analysis because these are metrics a real recruitment team would care about — they show whether hiring is accelerating or slowing and which departments close roles fastest.

## 9. Repo Cleanup — Removing Legacy Files

**Prompt:**
> I have untracked files and scripts that shouldn't be here. Check my repo structure and identify what doesn't belong.

**My thinking:** My initial approach built the silver and gold transforms in pure Python (`transform_silver.py`, `transform_gold.py`). When I migrated to dbt, those scripts became dead code but I never cleaned them up. The AI flagged them alongside `run_tests.py` (replaced by dbt tests + pytest) and `run_pipeline.py` (a file from a completely different project that was committed by mistake). I also removed `AI_PROMPTS.md`'s raw chat log and replaced it with this curated version, deleted an empty `dbt_project/macros/` directory, and added `dbt_project/.user.yml` to `.gitignore` since it's a local dbt artifact that shouldn't be tracked.

## How I Used AI Effectively

- **Architecture decisions were mine.** I chose the medallion pattern, star schema, and ELT split based on industry experience. The AI helped implement them faster.
- **I reviewed all generated code.** Several times I caught issues — e.g., the API response structure needed `departments->0->>'name'` JSONB extraction, not simple field access.
- **Debugging was collaborative.** When the Airflow DAG failed in Docker, I diagnosed the root cause (container networking — `localhost` doesn't resolve across containers) and used the AI to apply the fix across all config files.
- **I iterated on quality.** The initial test suite was minimal. I expanded it to 17 dbt tests + 12 pytest tests to cover referential integrity, business rules, and edge cases.
