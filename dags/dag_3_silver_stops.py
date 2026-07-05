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

dag_3_silver_stops = DAG(
    "dag_3_silver_stops",
    schedule=None,
    default_args=default_args,
    description="Transform bronze_gtfs_stops into silver. Casts lat/lon to FLOAT, location_type to INTEGER, adds GEOGRAPHY point.",
)

def create_silver_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver_gtfs_stops (
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                stop_id         TEXT    NOT NULL,
                stop_name       TEXT,
                stop_lat        DOUBLE PRECISION,
                stop_lon        DOUBLE PRECISION,
                location_type   INTEGER,            -- 1=StopArea (station), 0=StopPoint (platform)
                parent_station  TEXT,
                geom            GEOGRAPHY(Point, 4326)  -- new: built from stop_lon, stop_lat
                -- stop_desc, zone_id, stop_url dropped (unused in app)
            );
        """))
    engine.dispose()
    print("✓ Table silver_gtfs_stops ready")


def transform_and_load(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver_gtfs_stops;"))
        conn.execute(text("""
            INSERT INTO silver_gtfs_stops
                (stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station, geom)
            SELECT
                stop_id,
                stop_name,
                stop_lat::DOUBLE PRECISION,
                stop_lon::DOUBLE PRECISION,
                location_type::INTEGER,
                parent_station,
                -- new GEOGRAPHY column: ST_MakePoint(lon, lat) — note: longitude first
                ST_MakePoint(stop_lon::DOUBLE PRECISION, stop_lat::DOUBLE PRECISION)::GEOGRAPHY
            FROM bronze_gtfs_stops
            WHERE stop_id IS NOT NULL
              AND stop_lat IS NOT NULL
              AND stop_lon IS NOT NULL;
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM silver_gtfs_stops;")).scalar()
    engine.dispose()
    print(f"✓ {count:,} rows loaded into silver_gtfs_stops")


task_create_table = PythonOperator(
    task_id="task_create_silver_table",
    python_callable=create_silver_table,
    dag=dag_3_silver_stops,
)

task_transform = PythonOperator(
    task_id="task_transform_and_load",
    python_callable=transform_and_load,
    dag=dag_3_silver_stops,
)

task_create_table >> task_transform
