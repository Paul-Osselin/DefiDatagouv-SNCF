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

dag_3_silver_datatourisme_poi = DAG(
    "dag_3_silver_datatourisme_poi",
    schedule=None,
    default_args=default_args,
    description="Transform bronze_datatourisme_poi into silver. Adds GEOGRAPHY point from latitude/longitude for ST_DWithin proximity queries.",
)

# ---------------------------------------------------------------------------
# Task 1 — Create silver table
# ---------------------------------------------------------------------------

def create_silver_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver_datatourisme_poi (
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                id_poi          TEXT,
                nom_poi         TEXT,
                type_principal  TEXT,
                commune         TEXT,
                code_postal     TEXT,
                departement     TEXT,
                region          TEXT,
                latitude        DOUBLE PRECISION,
                longitude       DOUBLE PRECISION,
                geom            GEOGRAPHY(Point, 4326)  -- new: ST_DWithin perimeter search
            );
        """))
    engine.dispose()
    print("✓ Table silver_datatourisme_poi ready")


# ---------------------------------------------------------------------------
# Task 2 — Transform and load
# ---------------------------------------------------------------------------

def transform_and_load(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver_datatourisme_poi;"))
        conn.execute(text("""
            INSERT INTO silver_datatourisme_poi
                (id_poi, nom_poi, type_principal, commune, code_postal,
                 departement, region, latitude, longitude, geom)
            SELECT
                id_poi,
                nom_poi,
                type_principal,
                commune,
                code_postal,
                departement,
                region,
                latitude,
                longitude,
                -- longitude first, then latitude — PostGIS convention (x, y)
                ST_MakePoint(longitude, latitude)::GEOGRAPHY
            FROM bronze_datatourisme_poi
            WHERE latitude  IS NOT NULL
              AND longitude IS NOT NULL;
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM silver_datatourisme_poi;")).scalar()
    engine.dispose()
    print(f"✓ {count:,} rows loaded into silver_datatourisme_poi")


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

task_create_table = PythonOperator(
    task_id="task_create_silver_table",
    python_callable=create_silver_table,
    dag=dag_3_silver_datatourisme_poi,
)

task_transform = PythonOperator(
    task_id="task_transform_and_load",
    python_callable=transform_and_load,
    dag=dag_3_silver_datatourisme_poi,
)

task_create_table >> task_transform
