"""
Recruitment Pipeline DAG
========================
Orchestrates the full medallion pipeline:
  1. Bronze: Ingest from Greenhouse API + historical CSV
  2. Silver + Gold: dbt run (5 models)
  3. Quality: dbt test (17 schema tests)
  4. Integration: pytest (12 pipeline tests)

Schedule: Daily at 06:00 UTC
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt_project"
DBT_BIN = "dbt"
PYTHON_BIN = "python3"

default_args = {
    "owner": "prosper",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="recruitment_pipeline",
    default_args=default_args,
    description="Greenhouse recruitment data pipeline: ingest → dbt → test",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["recruitment", "greenhouse", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} scripts/ingest.py",
        doc="Pull live jobs from Greenhouse API and load historical CSV into bronze tables",
    )

    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run",
        doc="Build silver (stg_jobs) and gold (dimensions + fact) tables via dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_quality",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test",
        doc="Run 17 dbt schema tests: not_null, unique, relationships, accepted_values",
    )

    pytest_run = BashOperator(
        task_id="pytest_integration",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} -m pytest tests/test_pipeline.py -v",
        doc="Run 12 pytest integration tests across bronze, silver, gold layers",
    )

    # Pipeline dependency chain
    ingest >> dbt_run >> dbt_test >> pytest_run
