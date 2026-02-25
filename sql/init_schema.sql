-- bronze schema for raw data ingestion (Python scripts write here)
-- silver and gold schemas are created and managed by dbt automatically
CREATE SCHEMA IF NOT EXISTS bronze;

-- bronze: raw from greenhouse API
CREATE TABLE IF NOT EXISTS bronze.raw_jobs_api (
    id              BIGINT,
    internal_job_id BIGINT,
    title           TEXT,
    absolute_url    TEXT,
    location_name   TEXT,
    content         TEXT,
    departments     JSONB,
    offices         JSONB,
    updated_at      TIMESTAMP,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- bronze: raw from historical csv
CREATE TABLE IF NOT EXISTS bronze.raw_jobs_history (
    job_id          BIGINT,
    internal_job_id BIGINT,
    absolute_url    TEXT,
    title           TEXT,
    department      TEXT,
    location        TEXT,
    company_name    TEXT,
    open_date       TEXT,
    close_date      TEXT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);
