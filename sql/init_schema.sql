-- medallion architecture: bronze > silver > gold
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

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

-- silver: cleaned and merged
CREATE TABLE IF NOT EXISTS silver.jobs (
    job_id          BIGINT PRIMARY KEY,
    internal_job_id BIGINT,
    title           TEXT NOT NULL,
    absolute_url    TEXT,
    department      TEXT,
    location        TEXT,
    company_name    TEXT DEFAULT 'OfferZen',
    open_date       DATE,
    close_date      DATE,
    source          TEXT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- gold: star schema
CREATE TABLE IF NOT EXISTS gold.dim_department (
    department_key  SERIAL PRIMARY KEY,
    department_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key    SERIAL PRIMARY KEY,
    location_name   TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE UNIQUE NOT NULL,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      TEXT NOT NULL,
    day_of_week     INTEGER NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.fact_jobs (
    job_key         SERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL,
    title           TEXT NOT NULL,
    department_key  INTEGER REFERENCES gold.dim_department(department_key),
    location_key    INTEGER REFERENCES gold.dim_location(location_key),
    open_date_key   INTEGER REFERENCES gold.dim_date(date_key),
    close_date_key  INTEGER REFERENCES gold.dim_date(date_key),
    is_open         BOOLEAN NOT NULL,
    days_to_fill    INTEGER,
    source          TEXT NOT NULL
);
