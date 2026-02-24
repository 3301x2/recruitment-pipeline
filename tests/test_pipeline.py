"""Pipeline data quality tests using pytest."""
import psycopg2
import pytest

@pytest.fixture(scope="module")
def db():
    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname="recruitment", user="pipeline", password="pipeline123"
    )
    yield conn
    conn.close()

# --- Bronze layer ---
class TestBronze:
    def test_api_has_rows(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM bronze.raw_jobs_api")
        assert cur.fetchone()[0] > 0

    def test_history_has_316_rows(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM bronze.raw_jobs_history")
        assert cur.fetchone()[0] == 316

# --- Silver layer ---
class TestSilver:
    def test_stg_jobs_has_rows(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM public_silver.stg_jobs")
        assert cur.fetchone()[0] > 0

    def test_no_null_job_ids(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM public_silver.stg_jobs WHERE job_id IS NULL")
        assert cur.fetchone()[0] == 0

    def test_departments_are_title_case(self, db):
        cur = db.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM public_silver.stg_jobs
            WHERE department <> INITCAP(department)
        """)
        assert cur.fetchone()[0] == 0

    def test_source_values_valid(self, db):
        cur = db.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM public_silver.stg_jobs
            WHERE source NOT IN ('api', 'history')
        """)
        assert cur.fetchone()[0] == 0

# --- Gold layer ---
class TestGold:
    def test_fact_matches_silver_count(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM public_silver.stg_jobs")
        silver = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM public_gold.fact_jobs")
        gold = cur.fetchone()[0]
        assert gold == silver

    def test_dim_department_no_nulls(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM public_gold.dim_department WHERE department_name IS NULL")
        assert cur.fetchone()[0] == 0

    def test_fact_all_have_department_keys(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM public_gold.fact_jobs WHERE department_key IS NULL")
        assert cur.fetchone()[0] == 0

    def test_fact_all_have_location_keys(self, db):
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM public_gold.fact_jobs WHERE location_key IS NULL")
        assert cur.fetchone()[0] == 0

    def test_open_jobs_have_no_close_date(self, db):
        cur = db.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM public_gold.fact_jobs
            WHERE is_open = true AND close_date_key IS NOT NULL
        """)
        assert cur.fetchone()[0] == 0

    def test_days_to_fill_non_negative(self, db):
        cur = db.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM public_gold.fact_jobs
            WHERE days_to_fill IS NOT NULL AND days_to_fill < 0
        """)
        assert cur.fetchone()[0] == 0
