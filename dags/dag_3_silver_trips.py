from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime
from sqlalchemy import create_engine, text

DB_CONN = "postgresql://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 6),
    "catchup": False,
}

dag_3_silver_trips = DAG(
    "dag_3_silver_trips",
    schedule=None,
    default_args=default_args,
    description="Transform bronze_gtfs_trips into silver. Waits for silver_gtfs_stops and silver_gtfs_stop_times to be populated before building LineString geometry.",
)

# ---------------------------------------------------------------------------
# Custom sensor — waits until a table has at least 1 row
# ---------------------------------------------------------------------------

class TableHasDataSensor(BaseSensorOperator):
    """
    Pokes a PostgreSQL table every `poke_interval` seconds.
    Succeeds as soon as the table contains at least one row.
    """
    def __init__(self, table_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name

    def poke(self, context):
        engine = create_engine(DB_CONN)
        try:
            with engine.connect() as conn:
                count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.table_name};")
                ).scalar()
            engine.dispose()
            print(f"Sensor: {self.table_name} has {count:,} rows")
            return count > 0
        except Exception as e:
            print(f"Sensor error: {e}")
            engine.dispose()
            return False


# ---------------------------------------------------------------------------
# Task 1a — Wait for silver_gtfs_stops
# ---------------------------------------------------------------------------

sensor_stops = TableHasDataSensor(
    task_id="sensor_wait_for_silver_stops",
    table_name="silver_gtfs_stops",
    poke_interval=30,   # check every 30 seconds
    timeout=60 * 30,    # give up after 30 minutes
    mode="poke",
    dag=dag_3_silver_trips,
)

# ---------------------------------------------------------------------------
# Task 1b — Wait for silver_gtfs_stop_times
# ---------------------------------------------------------------------------

sensor_stop_times = TableHasDataSensor(
    task_id="sensor_wait_for_silver_stop_times",
    table_name="silver_gtfs_stop_times",
    poke_interval=30,
    timeout=60 * 30,
    mode="poke",
    dag=dag_3_silver_trips,
)

# ---------------------------------------------------------------------------
# Task 2 — Create silver table
# ---------------------------------------------------------------------------

def create_silver_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS silver_gtfs_trips (
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                trip_id         TEXT    NOT NULL,
                route_id        TEXT,
                service_id      TEXT,
                trip_headsign   TEXT,
                direction_id    INTEGER,            -- 0=outbound, 1=return
                geom            GEOGRAPHY(LineString, 4326)  -- full trip path on map
                -- block_id, shape_id dropped (unused in app)
            );
        """))
    engine.dispose()
    print("✓ Table silver_gtfs_trips ready")


# ---------------------------------------------------------------------------
# Task 3 — Transform and load
# ---------------------------------------------------------------------------

def transform_and_load(**kwargs):
    """
    Builds one LineString per trip by connecting all its stops in stop_sequence order.
    ST_MakeLine aggregates ordered points into a single line geometry.
    """
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver_gtfs_trips;"))
        conn.execute(text("""
            INSERT INTO silver_gtfs_trips
                (trip_id, route_id, service_id, trip_headsign, direction_id, geom)
            SELECT
                t.trip_id,
                t.route_id,
                t.service_id,
                t.trip_headsign,
                t.direction_id::INTEGER,
                ST_MakeLine(
                    ST_MakePoint(s.stop_lon, s.stop_lat)
                    ORDER BY st.stop_sequence
                )::GEOGRAPHY AS geom
            FROM bronze_gtfs_trips t
            JOIN silver_gtfs_stop_times st ON t.trip_id = st.trip_id
            JOIN silver_gtfs_stops s       ON st.stop_id = s.stop_id
            WHERE t.trip_id IS NOT NULL
            GROUP BY t.trip_id, t.route_id, t.service_id, t.trip_headsign, t.direction_id;
        """))
        count = conn.execute(text("SELECT COUNT(*) FROM silver_gtfs_trips;")).scalar()
    engine.dispose()
    print(f"✓ {count:,} rows loaded into silver_gtfs_trips")


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

task_create_table = PythonOperator(
    task_id="task_create_silver_table",
    python_callable=create_silver_table,
    dag=dag_3_silver_trips,
)

task_transform = PythonOperator(
    task_id="task_transform_and_load",
    python_callable=transform_and_load,
    dag=dag_3_silver_trips,
)

# ---------------------------------------------------------------------------
# Pipeline
#
#   sensor_wait_for_silver_stops  \
#                                  → task_create_silver_table → task_transform_and_load
#   sensor_wait_for_silver_stop_times /
# ---------------------------------------------------------------------------

[sensor_stops, sensor_stop_times] >> task_create_table >> task_transform
