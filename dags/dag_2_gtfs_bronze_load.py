from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ZIP_PATH = "/opt/airflow/data/gtfs_sncf.zip"
DB_CONN  = "postgresql://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"

# Each GTFS file → bronze table name + expected columns
# Bronze rule: all columns stored as TEXT, no casting, no business logic
GTFS_FILES = {
    "bronze_gtfs_calendar_dates": {
        "filename": "calendar_dates.txt",
        "columns": ["service_id", "date", "exception_type"],
    },
    "bronze_gtfs_routes": {
        "filename": "routes.txt",
        "columns": ["route_id", "agency_id", "route_short_name", "route_long_name",
                    "route_desc", "route_type", "route_url", "route_color", "route_text_color"],
    },
    "bronze_gtfs_stops": {
        "filename": "stops.txt",
        "columns": ["stop_id", "stop_name", "stop_desc", "stop_lat", "stop_lon",
                    "zone_id", "stop_url", "location_type", "parent_station"],
    },
    "bronze_gtfs_stop_times": {
        "filename": "stop_times.txt",
        "columns": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
                    "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled"],
    },
    "bronze_gtfs_trips": {
        "filename": "trips.txt",
        "columns": ["route_id", "service_id", "trip_id", "trip_headsign",
                    "direction_id", "block_id", "shape_id"],
    },
}

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 6),
    "catchup": False,
}

dag_gtfs_bronze_load = DAG(
    "dag_2_gtfs_bronze_load",
    schedule=None,       # triggered by dag_1_gtfs_download, not on a schedule
    default_args=default_args,
    description="Extract GTFS files from zip and load into PostgreSQL bronze tables",
)

# ---------------------------------------------------------------------------
# Task 1 — Create bronze tables if they don't exist
# ---------------------------------------------------------------------------

def create_bronze_tables(**kwargs):
    """Create all bronze GTFS tables if they don't already exist."""
    engine = create_engine(DB_CONN)

    with engine.begin() as conn:
        for table_name, config in GTFS_FILES.items():
            col_defs = ",\n    ".join(f"{col} TEXT" for col in config["columns"])
            sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    _ingested_at TIMESTAMP DEFAULT NOW(),
                    {col_defs}
                );
            """
            conn.execute(text(sql))
            print(f"✓ Table ready: {table_name}")

    engine.dispose()
    print("✓ All bronze tables created or already exist.")


# ---------------------------------------------------------------------------
# Task 2 — Extract one GTFS file and load into its bronze table
# One function called once per file via op_kwargs
# ---------------------------------------------------------------------------

def load_gtfs_file(table_name, **kwargs):
    """Read one GTFS .txt file from the zip on disk and load it into a bronze table."""
    config       = GTFS_FILES[table_name]
    filename     = config["filename"]
    expected_cols = config["columns"]

    # Read directly from the zip on disk — no XCom, no re-downloading
    with zipfile.ZipFile(ZIP_PATH) as zf:
        matched = [n for n in zf.namelist() if n.endswith(filename)]
        if not matched:
            print(f"⚠ {filename} not found in zip — skipping.")
            return
        with zf.open(matched[0]) as f:
            df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])

    print(f"✓ {filename}: {len(df):,} rows extracted")

    # Align columns to expected schema (add missing ones as None)
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    df = df[expected_cols]

    # TRUNCATE first for idempotency — safe to re-run
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

    df.to_sql(
        table_name,
        engine,
        if_exists="append",   # table already exists — append after truncate
        index=False,
        chunksize=10_000,     # batch inserts — stop_times has 438k rows
    )
    engine.dispose()

    print(f"✓ {len(df):,} rows loaded into {table_name}")


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

task_create_tables = PythonOperator(
    task_id="task_create_bronze_tables",
    python_callable=create_bronze_tables,
    dag=dag_gtfs_bronze_load,
)

# One load task per GTFS file — all run in parallel after table creation
load_tasks = []
for table_name in GTFS_FILES:
    task = PythonOperator(
        task_id=f"task_load_{table_name}",
        python_callable=load_gtfs_file,
        op_kwargs={"table_name": table_name},
        dag=dag_gtfs_bronze_load,
    )
    load_tasks.append(task)

# ---------------------------------------------------------------------------
# Pipeline
#
#   task_create_bronze_tables
#           ↓
#   task_load_bronze_gtfs_calendar_dates  \
#   task_load_bronze_gtfs_routes           |  parallel
#   task_load_bronze_gtfs_stops            |
#   task_load_bronze_gtfs_stop_times       |
#   task_load_bronze_gtfs_trips           /
# ---------------------------------------------------------------------------

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

SILVER_DAGS = [
    "dag_3_silver_calendar_dates",
    "dag_3_silver_routes",
    "dag_3_silver_stops",
    "dag_3_silver_stop_times",
    "dag_3_silver_trips",
]

silver_triggers = []
for dag_id in SILVER_DAGS:
    trigger = TriggerDagRunOperator(
        task_id=f"task_trigger_{dag_id}",
        trigger_dag_id=dag_id,
        wait_for_completion=False,
        dag=dag_gtfs_bronze_load,
    )
    silver_triggers.append(trigger)

from airflow.operators.empty import EmptyOperator

task_all_bronze_done = EmptyOperator(
    task_id="task_all_bronze_loaded",
    dag=dag_gtfs_bronze_load,
)

task_create_tables >> load_tasks >> task_all_bronze_done >> silver_triggers
