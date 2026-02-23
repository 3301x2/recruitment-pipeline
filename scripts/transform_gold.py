"""gold layer — build star schema from silver.jobs"""

import os
from datetime import date, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "recruitment"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def build_dim_department(cur):
    """populate dim_department from distinct departments in silver"""
    cur.execute("TRUNCATE TABLE gold.dim_department CASCADE;")
    cur.execute("""
        INSERT INTO gold.dim_department (department_name)
        SELECT DISTINCT department FROM silver.jobs
        WHERE department IS NOT NULL
        ORDER BY department
    """)
    cur.execute("SELECT COUNT(*) FROM gold.dim_department;")
    print(f"  dim_department: {cur.fetchone()[0]} rows")


def build_dim_location(cur):
    """populate dim_location from distinct locations in silver"""
    cur.execute("TRUNCATE TABLE gold.dim_location CASCADE;")
    cur.execute("""
        INSERT INTO gold.dim_location (location_name)
        SELECT DISTINCT location FROM silver.jobs
        WHERE location IS NOT NULL
        ORDER BY location
    """)
    cur.execute("SELECT COUNT(*) FROM gold.dim_location;")
    print(f"  dim_location: {cur.fetchone()[0]} rows")


def build_dim_date(cur):
    """generate date spine from 2015-01-01 to 2026-12-31"""
    cur.execute("TRUNCATE TABLE gold.dim_date CASCADE;")

    start = date(2015, 1, 1)
    end = date(2026, 12, 31)
    d = start
    while d <= end:
        cur.execute(
            """
            INSERT INTO gold.dim_date (
                date_key, full_date, year, quarter, month,
                month_name, day_of_week, is_weekend
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(d.strftime("%Y%m%d")),
                d,
                d.year,
                (d.month - 1) // 3 + 1,
                d.month,
                d.strftime("%B"),
                d.isoweekday(),
                d.isoweekday() >= 6,
            ),
        )
        d += timedelta(days=1)

    cur.execute("SELECT COUNT(*) FROM gold.dim_date;")
    print(f"  dim_date: {cur.fetchone()[0]} rows")


def build_fact_jobs(cur):
    """populate fact_jobs by joining silver.jobs to dimensions"""
    cur.execute("TRUNCATE TABLE gold.fact_jobs CASCADE;")
    cur.execute("""
        INSERT INTO gold.fact_jobs (
            job_id, title, department_key, location_key,
            open_date_key, close_date_key, is_open, days_to_fill, source
        )
        SELECT
            j.job_id,
            j.title,
            d.department_key,
            l.location_key,
            od.date_key,
            cd.date_key,
            j.close_date IS NULL,
            CASE WHEN j.close_date IS NOT NULL
                THEN j.close_date - j.open_date
                ELSE NULL
            END,
            j.source
        FROM silver.jobs j
        LEFT JOIN gold.dim_department d ON d.department_name = j.department
        LEFT JOIN gold.dim_location l ON l.location_name = j.location
        LEFT JOIN gold.dim_date od ON od.full_date = j.open_date
        LEFT JOIN gold.dim_date cd ON cd.full_date = j.close_date
    """)
    cur.execute("SELECT COUNT(*) FROM gold.fact_jobs;")
    print(f"  fact_jobs: {cur.fetchone()[0]} rows")


def main():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            print("[GOLD] building dimensions...")
            build_dim_department(cur)
            build_dim_location(cur)
            build_dim_date(cur)
            print("[GOLD] building fact table...")
            build_fact_jobs(cur)
        conn.commit()
        print("gold transformation complete")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
