from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import os

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GTFS_URL      = "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"
ZIP_SAVE_PATH = "/opt/airflow/data/gtfs_sncf.zip"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 6),
    "catchup": False,
}

dag_gtfs_download = DAG(
    "dag_1_gtfs_download",
    schedule="@weekly",
    default_args=default_args,
    description="Download SNCF GTFS zip and save to /opt/airflow/data/",
)

# ---------------------------------------------------------------------------
# Task 1 — Download the zip and save to disk
# ---------------------------------------------------------------------------

def download_gtfs_zip(**kwargs):
    """Download the SNCF GTFS zip file and save it to the shared data folder."""

    # Ensure the data folder exists
    os.makedirs(os.path.dirname(ZIP_SAVE_PATH), exist_ok=True)

    print(f"Downloading GTFS zip from: {GTFS_URL}")
    response = requests.get(GTFS_URL, timeout=180)
    response.raise_for_status()

    with open(ZIP_SAVE_PATH, "wb") as f:
        f.write(response.content)

    size_mb = os.path.getsize(ZIP_SAVE_PATH) / 1_000_000
    print(f"✓ Saved to {ZIP_SAVE_PATH} ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

task_download = PythonOperator(
    task_id="task_download_gtfs_zip",
    python_callable=download_gtfs_zip,
    dag=dag_gtfs_download,
)

task_trigger_load = TriggerDagRunOperator(
    task_id="task_trigger_bronze_load",
    trigger_dag_id="dag_2_gtfs_bronze_load",   # must match the dag_id in DAG 2
    wait_for_completion=False,                  # fire and forget — DAG 2 runs independently
    dag=dag_gtfs_download,
)

# ---------------------------------------------------------------------------
# Pipeline
#
#   task_download_gtfs_zip
#           ↓
#   task_trigger_bronze_load  → triggers dag_2_gtfs_bronze_load
# ---------------------------------------------------------------------------

task_download >> task_trigger_load
