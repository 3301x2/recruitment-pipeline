"""bronze layer — loads greenhouse api + historical csv into postgres"""

import csv
import json
import os
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://api.greenhouse.io/v1/boards/offerzen/jobs?content=true"
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "offerzen_jobs_history_raw.csv")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "recruitment"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def safe_int(val):
    """convert to int or None if empty"""
    val = val.strip() if val else ""
    return int(val) if val else None


def ingest_api(cur):
    """fetch current jobs from greenhouse and load into bronze.raw_jobs_api"""
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    jobs = resp.json()["jobs"]

    cur.execute("TRUNCATE TABLE bronze.raw_jobs_api;")

    now = datetime.now(timezone.utc)
    for job in jobs:
        cur.execute(
            """
            INSERT INTO bronze.raw_jobs_api (
                id, internal_job_id, title, absolute_url, location_name,
                content, departments, offices, updated_at, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job["id"],
                job["internal_job_id"],
                job["title"],
                job["absolute_url"],
                job.get("location", {}).get("name"),
                job.get("content"),
                json.dumps(job.get("departments", [])),
                json.dumps(job.get("offices", [])),
                job.get("updated_at"),
                now,
            ),
        )

    print(f"[API] loaded {len(jobs)} rows into bronze.raw_jobs_api")


def ingest_csv(cur):
    """load historical job data from csv into bronze.raw_jobs_history"""
    cur.execute("TRUNCATE TABLE bronze.raw_jobs_history;")

    now = datetime.now(timezone.utc)
    count = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO bronze.raw_jobs_history (
                    job_id, internal_job_id, absolute_url, title,
                    department, location, company_name,
                    open_date, close_date, ingested_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    safe_int(row["job_id"]),
                    safe_int(row["internal_job_id"]),
                    row["absolute_url"].strip() or None,
                    row["title"].strip(),
                    row["department"].strip() or None,
                    row["location"].strip(),
                    row["company_name"].strip(),
                    row["open_date"].strip() or None,
                    row["close_date"].strip() or None,
                    now,
                ),
            )
            count += 1

    print(f"[CSV] loaded {count} rows into bronze.raw_jobs_history")


def main():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ingest_api(cur)
            ingest_csv(cur)
        conn.commit()
        print("bronze ingestion complete")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
