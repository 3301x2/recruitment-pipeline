"""data quality checks across all layers"""

import os
import sys

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

TESTS = [
    ("bronze.raw_jobs_api has rows", "SELECT COUNT(*) FROM bronze.raw_jobs_api", lambda r: r > 0),
    ("bronze.raw_jobs_history has 316 rows", "SELECT COUNT(*) FROM bronze.raw_jobs_history", lambda r: r == 316),
    ("silver.jobs has rows", "SELECT COUNT(*) FROM silver.jobs", lambda r: r > 0),
    ("silver has no uppercase departments", "SELECT COUNT(*) FROM silver.jobs WHERE department = UPPER(department) AND LENGTH(department) > 1", lambda r: r == 0),
    ("silver has no null job_ids", "SELECT COUNT(*) FROM silver.jobs WHERE job_id IS NULL", lambda r: r == 0),
    ("gold.fact_jobs matches silver count", """
        SELECT (SELECT COUNT(*) FROM gold.fact_jobs) = (SELECT COUNT(*) FROM silver.jobs)
    """, lambda r: r is True),
    ("gold.dim_department no nulls", "SELECT COUNT(*) FROM gold.dim_department WHERE department_name IS NULL", lambda r: r == 0),
    ("gold.fact_jobs all have department keys", "SELECT COUNT(*) FROM gold.fact_jobs WHERE department_key IS NULL", lambda r: r == 0),
    ("gold.fact_jobs open jobs have no close date", "SELECT COUNT(*) FROM gold.fact_jobs WHERE is_open = TRUE AND close_date_key IS NOT NULL", lambda r: r == 0),
]


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    passed = 0
    failed = 0

    print("DATA QUALITY TESTS")
    print("=" * 50)

    for name, sql, check in TESTS:
        try:
            cur.execute(sql)
            result = cur.fetchone()[0]
            if check(result):
                print(f"  PASS: {name}")
                passed += 1
            else:
                print(f"  FAIL: {name} (got {result})")
                failed += 1
        except Exception as e:
            print(f"  ERROR: {name} — {e}")
            failed += 1
            conn.rollback()

    print("=" * 50)
    print(f"  {passed} passed, {failed} failed")
    conn.close()

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
