# ============================================================
# DAG SILVER A — FAMILLE GTFS
# Projet : Tourisme en train — M1 Big Data & IA — SUP DE VINCI
# Declenche par : dag_orchestrateur (ou manuellement)
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

URI_DB = "postgresql+psycopg2://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"


def run_silver_a():
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    from sqlalchemy import create_engine, text

    engine = create_engine(URI_DB, connect_args={"client_encoding": "utf8"})

    def exec_sql(sql, label):
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"[SQL] {label}")

    # -- silver_gtfs_stops
    df_stops = pd.read_sql(text("""
        SELECT DISTINCT ON (stop_id)
            stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station
        FROM bronze_sncf_gtfs_full
        WHERE stop_id IS NOT NULL
        ORDER BY stop_id
    """), engine)
    df_stops["stop_lat"] = pd.to_numeric(
        df_stops["stop_lat"].astype(str).str.replace(",", ".", regex=False).str.strip(),
        errors="coerce")
    df_stops["stop_lon"] = pd.to_numeric(
        df_stops["stop_lon"].astype(str).str.replace(",", ".", regex=False).str.strip(),
        errors="coerce")
    df_stops["location_type"] = pd.to_numeric(df_stops["location_type"], errors="coerce").fillna(0).astype(int)
    df_stops["stop_id"]   = df_stops["stop_id"].astype(str).str.strip()
    df_stops["stop_name"] = df_stops["stop_name"].astype(str).str.strip()
    df_stops = df_stops.dropna(subset=["stop_lat", "stop_lon"])
    df_stops = df_stops[df_stops["stop_lat"].between(41.0, 52.0) & df_stops["stop_lon"].between(-5.5, 10.0)]
    df_stops["uic_from_parent"] = df_stops["parent_station"].fillna("").astype(str).str.extract(r"(\d{8})", expand=False)
    df_stops.to_sql("silver_gtfs_stops", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_gtfs_stops : {len(df_stops):,} lignes")

    # -- silver_gtfs_routes
    df_routes = pd.read_sql(text("""
        SELECT DISTINCT ON (route_id)
            route_id, agency_id, route_short_name, route_long_name,
            route_type, route_color, route_text_color
        FROM bronze_sncf_gtfs_full WHERE route_id IS NOT NULL ORDER BY route_id
    """), engine)
    df_routes["route_type"] = pd.to_numeric(df_routes["route_type"], errors="coerce").fillna(2).astype(int)
    type_map = {0:"Tramway", 1:"Metro", 2:"Ferroviaire", 3:"Bus", 4:"Ferry"}
    df_routes["route_type_label"] = df_routes["route_type"].map(type_map).fillna("Autre")
    df_routes.to_sql("silver_gtfs_routes", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_gtfs_routes : {len(df_routes):,} lignes")

    # -- silver_gtfs_trips
    df_trips = pd.read_sql(text("""
        SELECT DISTINCT ON (trip_id)
            trip_id, route_id, service_id, direction_id, block_id
        FROM bronze_sncf_gtfs_full WHERE trip_id IS NOT NULL ORDER BY trip_id
    """), engine)
    df_trips["direction_id"] = pd.to_numeric(df_trips["direction_id"], errors="coerce").fillna(0).astype(int)
    df_trips.to_sql("silver_gtfs_trips", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_gtfs_trips : {len(df_trips):,} lignes")

    # -- silver_gtfs_agency
    df_agency = pd.read_sql(text("""
        SELECT DISTINCT ON (agency_id)
            agency_id, agency_name, agency_url, agency_timezone, agency_lang
        FROM bronze_sncf_gtfs_full WHERE agency_id IS NOT NULL ORDER BY agency_id
    """), engine)
    df_agency.to_sql("silver_gtfs_agency", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_gtfs_agency : {len(df_agency):,} lignes")

    # -- silver_gtfs_calendar (388k lignes -> 100% SQL, cote serveur)
    exec_sql("DROP TABLE IF EXISTS silver_gtfs_calendar", "DROP calendar")
    exec_sql("""
        CREATE TABLE silver_gtfs_calendar AS
        SELECT DISTINCT
            trim(service_id) AS service_id,
            to_date(date::text, 'YYYYMMDD') AS date,
            COALESCE(exception_type, 1)::int AS exception_type
        FROM bronze_sncf_gtfs_full
        WHERE service_id IS NOT NULL AND date IS NOT NULL
    """, "CREATE silver_gtfs_calendar")
    n_cal = pd.read_sql(text("SELECT COUNT(*) AS n FROM silver_gtfs_calendar"), engine)["n"].iloc[0]
    print(f"silver_gtfs_calendar : {n_cal:,} lignes")

    # -- silver_gtfs_stop_times (100% SQL)
    exec_sql("DROP TABLE IF EXISTS silver_gtfs_stop_times", "DROP stop_times")
    exec_sql("""
        CREATE TABLE silver_gtfs_stop_times AS
        SELECT trim(trip_id) AS trip_id, trim(arrival_time) AS arrival_time,
               trim(departure_time) AS departure_time, trim(stop_id) AS stop_id,
               COALESCE(stop_sequence,0)::int AS stop_sequence,
               COALESCE(pickup_type,0)::int AS pickup_type,
               COALESCE(drop_off_type,0)::int AS drop_off_type
        FROM bronze_sncf_gtfs_full WHERE trip_id IS NOT NULL AND stop_id IS NOT NULL
    """, "CREATE silver_gtfs_stop_times")
    exec_sql("CREATE INDEX idx_st_trip ON silver_gtfs_stop_times(trip_id)", "index trip_id")
    exec_sql("CREATE INDEX idx_st_stop ON silver_gtfs_stop_times(stop_id)", "index stop_id")
    n = pd.read_sql(text("SELECT COUNT(*) AS n FROM silver_gtfs_stop_times"), engine)["n"].iloc[0]
    print(f"silver_gtfs_stop_times : {n:,} lignes")
    print("=== Silver A termine ===")


with DAG(
    default_args={"retries": 0, "retry_delay": pendulum.duration(minutes=1)},
    dag_id="silver_A_gtfs",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sncf", "silver", "gtfs"],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin   = EmptyOperator(task_id="fin")
    transform = PythonOperator(
        task_id="transformer_gtfs",
        python_callable=run_silver_a,
        execution_timeout=pendulum.duration(minutes=30),
    )
    debut >> transform >> fin
