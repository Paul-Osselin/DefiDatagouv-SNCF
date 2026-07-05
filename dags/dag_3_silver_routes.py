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

dag_3_silver_routes = DAG(
    "dag_3_silver_routes",
    schedule=None,
    default_args=default_args,
    description="Transform bronze_gtfs_routes into silver. Casts route_type to INTEGER, drops unused columns.",
)

def create_silver_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver_gtfs_routes (
                _ingested_at      TIMESTAMP DEFAULT NOW(),
                route_id          TEXT    NOT NULL,
                agency_id         TEXT,
                route_short_name  TEXT,
                route_long_name   TEXT,
                route_desc        TEXT,
                route_type        INTEGER         -- 2=Rail, 3=Bus
                -- route_url, route_color, route_text_color dropped (unused in app)
            );
        """))
    engine.dispose()
    print("✓ Table silver_gtfs_routes ready")


def transform_and_load(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver_gtfs_routes;"))
        conn.execute(text("""
            INSERT INTO silver_gtfs_routes (route_id, agency_id, route_short_name, route_long_name, route_desc, route_type)
            SELECT
                route_id,
                agency_id,
                route_short_name,
                route_long_name,
                route_desc,
                route_type::INTEGER
            FROM bronze_gtfs_routes
            WHERE route_id IS NOT NULL;
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM silver_gtfs_routes;")).scalar()
    engine.dispose()
    print(f"✓ {count:,} rows loaded into silver_gtfs_routes")


task_create_table = PythonOperator(
    task_id="task_create_silver_table",
    python_callable=create_silver_table,
    dag=dag_3_silver_routes,
)

task_transform = PythonOperator(
    task_id="task_transform_and_load",
    python_callable=transform_and_load,
    dag=dag_3_silver_routes,
)

task_create_table >> task_transform
