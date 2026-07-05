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

dag_3_silver_stop_times = DAG(
    "dag_3_silver_stop_times",
    schedule=None,
    default_args=default_args,
    description="Transform bronze_gtfs_stop_times into silver. Casts numeric columns, keeps times as TEXT (GTFS times can exceed 24h).",
)

def create_silver_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver_gtfs_stop_times (
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                trip_id         TEXT    NOT NULL,
                stop_id         TEXT    NOT NULL,
                stop_sequence   INTEGER NOT NULL,
                arrival_time    TEXT,   -- kept as TEXT: GTFS allows '25:30:00' (next day)
                departure_time  TEXT,
                pickup_type     INTEGER,
                drop_off_type   INTEGER
                -- stop_headsign, shape_dist_traveled dropped (unused in app)
            );
        """))
    engine.dispose()
    print("✓ Table silver_gtfs_stop_times ready")


def transform_and_load(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver_gtfs_stop_times;"))
        conn.execute(text("""
            INSERT INTO silver_gtfs_stop_times
                (trip_id, stop_id, stop_sequence, arrival_time, departure_time, pickup_type, drop_off_type)
            SELECT
                trip_id,
                stop_id,
                stop_sequence::INTEGER,
                arrival_time,
                departure_time,
                pickup_type::INTEGER,
                drop_off_type::INTEGER
            FROM bronze_gtfs_stop_times
            WHERE trip_id IS NOT NULL
              AND stop_id IS NOT NULL
              AND stop_sequence IS NOT NULL;
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM silver_gtfs_stop_times;")).scalar()
    engine.dispose()
    print(f"✓ {count:,} rows loaded into silver_gtfs_stop_times")


task_create_table = PythonOperator(
    task_id="task_create_silver_table",
    python_callable=create_silver_table,
    dag=dag_3_silver_stop_times,
)

task_transform = PythonOperator(
    task_id="task_transform_and_load",
    python_callable=transform_and_load,
    dag=dag_3_silver_stop_times,
)

task_create_table >> task_transform
