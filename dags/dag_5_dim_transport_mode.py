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

dag_5_dim_transport_mode = DAG(
    "dag_5_dim_transport_mode",
    schedule=None,
    default_args=default_args,
    description="Derive the (prefix, code) -> emission-mode crosswalk directly "
                "from the silver layer. The pairs, labels and counts are "
                "discovered from the data; only the mapping rule to our ADEME "
                "modes is curated (the GTFS cannot tell us which factor applies).",
)

# ---------------------------------------------------------------------------
# Task 1 - Create the dimension table
# ---------------------------------------------------------------------------

def create_dim_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_transport_mode (
                prefix      TEXT NOT NULL,      -- F=rail (Ferre), R=road (Route)
                code        TEXT NOT NULL,      -- commercial sub-type from trip_id
                full_name   TEXT,               -- human-readable label from stop_id
                nb_trips    INTEGER,            -- observed volume (data-driven)
                train_mode  TEXT NOT NULL,      -- -> ref_emission_factors.mode
                mode_label  TEXT,
                is_approx   BOOLEAN DEFAULT FALSE,
                _loaded_at  TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (prefix, code)
            );
        """))
    engine.dispose()
    print("OK - Table dim_transport_mode ready")


# ---------------------------------------------------------------------------
# Task 2 - Build the crosswalk from the silver layer
#
# Discovered from data : prefix, code (silver_gtfs_trips), full_name + nb_trips
#                        (silver_gtfs_stop_times).
# Curated mapping rule  : the CASE below. prefix 'R' is always a road coach;
#                         prefix 'F' maps by code to the train sub-type.
#                           OUI/OGO/LYR -> tgv
#                           IC/ICN/ICE  -> intercites   (ICE ~8 g ~ Intercites)
#                           TER/TT/TRN/NAV + unknown rail -> ter
# A new code SNCF introduces appears automatically, defaulting to autocar
# (road) or ter (rail) until you give it an explicit rule.
# ---------------------------------------------------------------------------

def build_dim_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_transport_mode;"))
        conn.execute(text("""
            INSERT INTO dim_transport_mode
                (prefix, code, full_name, nb_trips, train_mode, mode_label, is_approx)
            WITH parsed AS (
                SELECT trip_id,
                       (regexp_match(trip_id, '_([A-Za-z]):([A-Za-z]+):'))[1] AS prefix,
                       (regexp_match(trip_id, '_([A-Za-z]):([A-Za-z]+):'))[2] AS code
                FROM silver_gtfs_trips
            ),
            named AS (
                SELECT DISTINCT ON (trip_id) trip_id,
                       substring(stop_id from '^StopPoint:OCE(.*)-[0-9]+$') AS full_name
                FROM silver_gtfs_stop_times
                WHERE stop_id LIKE 'StopPoint:%'
                ORDER BY trip_id, stop_sequence
            ),
            crosswalk AS (
                SELECT p.prefix, p.code,
                       MAX(n.full_name) AS full_name,
                       COUNT(*)         AS nb_trips
                FROM parsed p
                LEFT JOIN named n USING (trip_id)
                WHERE p.prefix IS NOT NULL AND p.code IS NOT NULL
                GROUP BY p.prefix, p.code
            )
            SELECT
                prefix, code, full_name, nb_trips,
                CASE
                    WHEN prefix = 'R'                THEN 'autocar'
                    WHEN code IN ('OUI','OGO','LYR') THEN 'tgv'
                    WHEN code IN ('IC','ICN','ICE')  THEN 'intercites'
                    ELSE 'ter'
                END AS train_mode,
                COALESCE(full_name, prefix || ':' || code)        AS mode_label,
                (prefix = 'F' AND code IN ('ICE','TT','TRN','NAV')) AS is_approx
            FROM crosswalk;
        """))

        # Safety net: every derived train_mode must exist in ref_emission_factors.
        orphans = conn.execute(text("""
            SELECT DISTINCT d.train_mode
            FROM dim_transport_mode d
            LEFT JOIN ref_emission_factors f ON f.mode = d.train_mode
            WHERE f.mode IS NULL
        """)).fetchall()
        if orphans:
            raise ValueError(f"train_mode(s) absent de ref_emission_factors: "
                             f"{[o[0] for o in orphans]}")

        rows = conn.execute(text("""
            SELECT prefix, code, full_name, nb_trips, train_mode
            FROM dim_transport_mode ORDER BY nb_trips DESC
        """)).fetchall()
    engine.dispose()

    for r in rows:
        print(f"  {r[0]}/{r[1]:<4} {str(r[2])[:22]:<22} -> {r[4]:<11} ({r[3]} trips)")
    print(f"OK - {len(rows)} (prefix, code) pairs loaded into dim_transport_mode")


task_create = PythonOperator(
    task_id="task_create_dim_table",
    python_callable=create_dim_table,
    dag=dag_5_dim_transport_mode,
)

task_build = PythonOperator(
    task_id="task_build_dim_table",
    python_callable=build_dim_table,
    dag=dag_5_dim_transport_mode,
)

task_create >> task_build
