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

dag_6_gold_gtfs_trips = DAG(
    "dag_6_gold_gtfs_trips",
    schedule=None,
    default_args=default_args,
    description="Gold trips: each trip enriched with its emission mode "
                "(train_mode -> ref_emission_factors) and commercial label, "
                "classified purely from trip_id via dim_transport_mode. "
                "No stop_times scan. Read by the app at request time.",
)

# ---------------------------------------------------------------------------
# Task 1 - Create the gold table
# ---------------------------------------------------------------------------

def create_gold_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold_gtfs_trips (
                trip_id        TEXT PRIMARY KEY,
                route_id       TEXT,
                service_id     TEXT,
                trip_headsign  TEXT,
                direction_id   INTEGER,
                prefix         TEXT,                 -- F=rail, R=road
                code           TEXT,                 -- commercial sub-type code
                train_mode     TEXT NOT NULL,        -- -> ref_emission_factors.mode
                mode_label     TEXT,                 -- commercial label (TGV INOUI, ...)
                full_trip_km   DOUBLE PRECISION,     -- whole-trip length (NOT the carbon distance)
                geom           geography(LineString, 4326),
                _built_at      TIMESTAMP DEFAULT NOW()
            );
        """))
        # Indexes for the request-time joins (routes, calendar_dates, stop_times).
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_gold_trips_route   ON gold_gtfs_trips (route_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_gold_trips_service ON gold_gtfs_trips (service_id);"))
    engine.dispose()
    print("OK - Table gold_gtfs_trips ready")


# ---------------------------------------------------------------------------
# Task 2 - Build the gold table
#
# prefix/code parsed from trip_id (same regex used to build the dim).
# train_mode comes from dim_transport_mode; COALESCE keeps a sane default so a
# brand-new code is never left unclassified (unknown road -> autocar, rail -> ter).
# ---------------------------------------------------------------------------

def build_gold_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold_gtfs_trips;"))
        conn.execute(text("""
            INSERT INTO gold_gtfs_trips
                (trip_id, route_id, service_id, trip_headsign, direction_id,
                 prefix, code, train_mode, mode_label, full_trip_km, geom)
            WITH parsed AS (
                SELECT
                    t.trip_id, t.route_id, t.service_id,
                    t.trip_headsign, t.direction_id, t.geom,
                    (regexp_match(t.trip_id, '_([A-Za-z]):([A-Za-z]+):'))[1] AS prefix,
                    (regexp_match(t.trip_id, '_([A-Za-z]):([A-Za-z]+):'))[2] AS code
                FROM silver_gtfs_trips t
            )
            SELECT
                p.trip_id, p.route_id, p.service_id, p.trip_headsign, p.direction_id,
                p.prefix, p.code,
                COALESCE(d.train_mode,
                         CASE WHEN p.prefix = 'R' THEN 'autocar' ELSE 'ter' END) AS train_mode,
                COALESCE(d.full_name, p.prefix || ':' || p.code, 'inconnu')       AS mode_label,
                ST_Length(p.geom) / 1000.0                                        AS full_trip_km,
                p.geom
            FROM parsed p
            LEFT JOIN dim_transport_mode d
                   ON d.prefix = p.prefix AND d.code = p.code;
        """))

        total = conn.execute(text("SELECT COUNT(*) FROM gold_gtfs_trips;")).scalar()
        breakdown = conn.execute(text("""
            SELECT train_mode, COUNT(*) AS n
            FROM gold_gtfs_trips GROUP BY train_mode ORDER BY n DESC
        """)).fetchall()
    engine.dispose()

    for b in breakdown:
        print(f"  {b[0]:<11} {b[1]:>7} trips")
    print(f"OK - {total} trips materialized into gold_gtfs_trips")


task_create = PythonOperator(
    task_id="task_create_gold_table",
    python_callable=create_gold_table,
    dag=dag_6_gold_gtfs_trips,
)

task_build = PythonOperator(
    task_id="task_build_gold_table",
    python_callable=build_gold_table,
    dag=dag_6_gold_gtfs_trips,
)

task_create >> task_build
