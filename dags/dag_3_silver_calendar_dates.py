from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text

DB_CONN = "postgresql://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 6),
    "catchup": False,
}

dag_3_silver_calendar_dates = DAG(
    "dag_3_silver_calendar_dates",
    schedule=None,
    default_args=default_args,
    description="Transform bronze_gtfs_calendar_dates into silver. Casts date to DATE, exception_type to INTEGER.",
)

# ---------------------------------------------------------------------------
# Task 1 — Create silver table
# ---------------------------------------------------------------------------

def create_silver_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver_gtfs_calendar_dates (
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                service_id      TEXT        NOT NULL,
                date            DATE        NOT NULL,   -- cast from TEXT 'YYYYMMDD'
                exception_type  INTEGER     NOT NULL    -- 1 = service added
            );
        """))
    engine.dispose()
    print("✓ Table silver_gtfs_calendar_dates ready")


# ---------------------------------------------------------------------------
# Task 2 — Transform and load
# ---------------------------------------------------------------------------

def transform_and_load(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver_gtfs_calendar_dates;"))
        conn.execute(text("""
            INSERT INTO silver_gtfs_calendar_dates (service_id, date, exception_type)
            SELECT
                service_id,
                TO_DATE(date, 'YYYYMMDD'),      -- TEXT '20260617' → DATE
                exception_type::INTEGER
            FROM bronze_gtfs_calendar_dates
            WHERE service_id IS NOT NULL
              AND date IS NOT NULL
              AND exception_type IS NOT NULL;
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM silver_gtfs_calendar_dates;")).scalar()
    engine.dispose()
    print(f"✓ {count:,} rows loaded into silver_gtfs_calendar_dates")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

task_create_table = PythonOperator(
    task_id="task_create_silver_table",
    python_callable=create_silver_table,
    dag=dag_3_silver_calendar_dates,
)

task_transform = PythonOperator(
    task_id="task_transform_and_load",
    python_callable=transform_and_load,
    dag=dag_3_silver_calendar_dates,
)

task_create_table >> task_transform
