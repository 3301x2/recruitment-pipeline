# Recruitment Data Pipeline — Greenhouse Job Board Analytics

End-to-end data pipeline that ingests job postings from the [Greenhouse API](https://api.greenhouse.io/v1/boards/offerzen/jobs?content=true) and a historical CSV (2015–2025), transforms them through a medallion architecture (Bronze → Silver → Gold), and produces an analytics-ready star schema in PostgreSQL.

Orchestrated with Apache Airflow. Transformations powered by dbt.

## Architecture

```
Greenhouse API ──┐                    ┌─ dim_department
                 ├─→ Bronze (raw) ─→ Silver (stg_jobs) ─→ Gold ─┤─ dim_location
Historical CSV ──┘                                              ├─ dim_date
                                                                └─ fact_jobs
```

**Tech stack**: Python 3.12 · PostgreSQL 16 · dbt-postgres · Apache Airflow 2.10 · Docker Compose

See [SOLUTION.md](SOLUTION.md) for detailed architecture decisions and trade-offs.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git

### 1. Clone and start

```bash
git clone <repo-url> && cd recruitment-pipeline
docker-compose up -d
```

This starts two containers:
- **recruitment-postgres** — PostgreSQL 16 (data warehouse + Airflow metadata)
- **recruitment-airflow** — Airflow webserver + scheduler with dbt and Python dependencies pre-installed

The database schema (bronze/silver/gold) is automatically created on first boot via `sql/init_schema.sql`.

### 2. Access Airflow UI

Open [http://localhost:8081](http://localhost:8081) and log in:
- **Username**: `admin`
- **Password**: `admin`

### 3. Trigger the pipeline

Either click **Trigger DAG** in the Airflow UI, or run:

```bash
docker exec recruitment-airflow airflow dags trigger recruitment_pipeline
```

The DAG runs 4 tasks in sequence:

| Task | What it does |
|------|-------------|
| `bronze_ingest` | Fetches live jobs from Greenhouse API + loads 316-row historical CSV into bronze tables |
| `dbt_run_models` | Builds silver (`stg_jobs`) and gold (`dim_department`, `dim_location`, `dim_date`, `fact_jobs`) |
| `dbt_test_quality` | Runs 17 dbt schema tests (not_null, unique, relationships, accepted_values) |
| `pytest_integration` | Runs 12 integration tests across all three layers |

### 4. Query the data

```bash
docker exec -it recruitment-postgres psql -U pipeline -d recruitment
```

```sql
-- Currently open positions
SELECT f.title, d.department_name, l.location_name
FROM public_gold.fact_jobs f
JOIN public_gold.dim_department d ON d.department_key = f.department_key
JOIN public_gold.dim_location l ON l.location_key = f.location_key
WHERE f.is_open = true;

-- Average time-to-fill by department
SELECT d.department_name,
       ROUND(AVG(f.days_to_fill), 1) AS avg_days
FROM public_gold.fact_jobs f
JOIN public_gold.dim_department d ON d.department_key = f.department_key
WHERE f.days_to_fill IS NOT NULL
GROUP BY d.department_name
ORDER BY avg_days;
```

More example queries in [sql/analytics_queries.sql](sql/analytics_queries.sql).

## Running Tests Locally

From inside the Airflow container:

```bash
# dbt tests (17 schema tests)
docker exec recruitment-airflow bash -c "cd /opt/airflow/project/dbt_project && dbt test"

# pytest integration tests (12 tests)
docker exec recruitment-airflow bash -c "cd /opt/airflow/project && python3 -m pytest tests/test_pipeline.py -v"
```

## Project Structure

```
recruitment-pipeline/
├── airflow/
│   ├── Dockerfile              # Airflow image with dbt + Python deps
│   └── dags/
│       └── recruitment_pipeline.py  # DAG definition (4 tasks, daily 06:00 UTC)
├── dbt_project/
│   ├── models/
│   │   ├── sources.yml         # Bronze source definitions
│   │   ├── silver/
│   │   │   ├── stg_jobs.sql    # Cleaned & merged staging model
│   │   │   └── schema.yml      # Silver layer tests
│   │   └── gold/
│   │       ├── dim_department.sql
│   │       ├── dim_location.sql
│   │       ├── dim_date.sql    # Date spine 2015–2026
│   │       ├── fact_jobs.sql   # Fact table with pre-computed metrics
│   │       └── schema.yml      # Gold layer tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/
│   └── ingest.py               # Bronze ingestion (API + CSV)
├── sql/
│   ├── init_schema.sql         # Database schema DDL
│   └── analytics_queries.sql   # 8 example analytical queries
├── tests/
│   └── test_pipeline.py        # 12 pytest integration tests
├── data/
│   └── offerzen_jobs_history_raw.csv  # Historical data (316 records)
├── docker-compose.yml
├── requirements.txt
├── SOLUTION.md                 # Architecture decisions & trade-offs
└── .env.example
```

## Tear Down

```bash
docker-compose down -v   # stops containers and removes data volume
```
