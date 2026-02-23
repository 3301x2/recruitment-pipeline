"""silver layer — clean and merge bronze data into silver.jobs"""

import os
import re
from datetime import datetime

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


def clean_department(dept):
    """fix casing: OPERATIONS -> Operations, empty -> Unknown"""
    if not dept or not dept.strip():
        return "Unknown"
    return dept.strip().title()


def clean_location(loc):
    """fix casing: South africa -> South Africa"""
    if not loc or not loc.strip():
        return "Unknown"
    return loc.strip().title()


def parse_date(val):
    """handle YYYY-MM-DD and MM/DD/YYYY formats"""
    if not val or not val.strip():
        return None
    val = val.strip()
    # try YYYY-MM-DD first
    if re.match(r'^\d{4}-\d{2}-\d{2}$', val):
        return datetime.strptime(val, "%Y-%m-%d").date()
    # try MM/DD/YYYY
    if re.match(r'^\d{2}/\d{2}/\d{4}$', val):
        return datetime.strptime(val, "%m/%d/%Y").date()
    return None


def transform(cur):
    cur.execute("TRUNCATE TABLE silver.jobs;")

    # load historical csv data
    cur.execute("SELECT * FROM bronze.raw_jobs_history;")
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        cur.execute(
            """
            INSERT INTO silver.jobs (
                job_id, internal_job_id, title, absolute_url,
                department, location, company_name,
                open_date, close_date, source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_id) DO NOTHING
            """,
            (
                r["job_id"],
                r["internal_job_id"],
                r["title"].strip(),
                r["absolute_url"],
                clean_department(r["department"]),
                clean_location(r["location"]),
                r["company_name"],
                parse_date(r["open_date"]),
                parse_date(r["close_date"]),
                "history",
            ),
        )

    # load api data
    cur.execute("SELECT * FROM bronze.raw_jobs_api;")
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        # extract department from jsonb array
        depts = r["departments"]
        dept_name = depts[0]["name"] if depts else None
        cur.execute(
            """
            INSERT INTO silver.jobs (
                job_id, internal_job_id, title, absolute_url,
                department, location, company_name,
                open_date, close_date, source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_id) DO UPDATE SET
                title = EXCLUDED.title,
                department = EXCLUDED.department,
                location = EXCLUDED.location,
                source = EXCLUDED.source
            """,
            (
                r["id"],
                r["internal_job_id"],
                r["title"].strip(),
                r["absolute_url"],
                clean_department(dept_name),
                clean_location(r["location_name"]),
                "OfferZen",
                r["updated_at"].date() if r["updated_at"] else None,
                None,  # api jobs are still open
                "api",
            ),
        )

    cur.execute("SELECT COUNT(*) FROM silver.jobs;")
    count = cur.fetchone()[0]
    print(f"[SILVER] loaded {count} rows into silver.jobs")


def main():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            transform(cur)
        conn.commit()
        print("silver transformation complete")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
