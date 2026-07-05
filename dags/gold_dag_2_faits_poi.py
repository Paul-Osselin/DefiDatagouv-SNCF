# ============================================================
# DAG GOLD 2 — JOINTURE SPATIALE POI <-> GARE
# Projet : Tourisme en train — M1 Big Data & IA — SUP DE VINCI
# Declenche par : dag_orchestrateur apres gold_1
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

URI_DB = "postgresql+psycopg2://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"


def run_gold_2():
    import pandas as pd
    import numpy as np
    from scipy.spatial import cKDTree
    import warnings
    warnings.filterwarnings("ignore")
    from sqlalchemy import create_engine, text

    engine = create_engine(URI_DB, connect_args={"client_encoding": "utf8"})

    RAYONS_KM = [2, 5, 20, 50]

    def latlon_to_xy_km(lat, lon, lat0=46.6, lon0=2.5):
        R = 6371.0
        x = np.radians(lon - lon0) * np.cos(np.radians(lat0)) * R
        y = np.radians(lat - lat0) * R
        return x, y

    gares = pd.read_sql("SELECT gare_key, uic, latitude, longitude FROM dim_gare", engine)
    poi   = pd.read_sql("SELECT poi_id, latitude, longitude FROM silver_poi", engine)
    print(f"Gares : {len(gares)}, POI : {len(poi)}")

    gares["x"], gares["y"] = latlon_to_xy_km(gares["latitude"].values, gares["longitude"].values)
    poi["x"],   poi["y"]   = latlon_to_xy_km(poi["latitude"].values,   poi["longitude"].values)

    tree = cKDTree(poi[["x", "y"]].values)
    voisins = tree.query_ball_point(gares[["x", "y"]].values, r=max(RAYONS_KM))

    lignes = []
    poi_x, poi_y = poi["x"].values, poi["y"].values
    poi_ids = poi["poi_id"].values
    for i, idx_list in enumerate(voisins):
        if not idx_list:
            continue
        gx, gy = gares["x"].iloc[i], gares["y"].iloc[i]
        gk = gares["gare_key"].iloc[i]
        guic = gares["uic"].iloc[i]
        for j in idx_list:
            dist = np.sqrt((poi_x[j] - gx)**2 + (poi_y[j] - gy)**2)
            lignes.append((gk, guic, poi_ids[j], round(float(dist), 3)))

    fact = pd.DataFrame(lignes, columns=["gare_key", "uic", "poi_id", "distance_km"])
    for r in RAYONS_KM:
        fact[f"dans_{r}km"] = (fact["distance_km"] <= r).astype(int)

    # Ecriture rapide via COPY (2.4M lignes - to_sql serait trop lent en distant)
    import io as _io
    fact.head(0).to_sql("fact_poi_proximite", engine, if_exists="replace", index=False)
    buffer = _io.StringIO()
    fact.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.copy_expert(
            "COPY fact_poi_proximite FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')",
            buffer)
        raw.commit()
        cur.close()
    finally:
        raw.close()

    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_poiprox_gare ON fact_poi_proximite(gare_key)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_poiprox_poi ON fact_poi_proximite(poi_id)"))

    print(f"fact_poi_proximite : {len(fact):,} paires")
    print("=== Gold 2 termine ===")


with DAG(
    default_args={"retries": 0, "retry_delay": pendulum.duration(minutes=1)},
    dag_id="gold_2_faits_poi",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sncf", "gold", "poi"],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin   = EmptyOperator(task_id="fin")
    transform = PythonOperator(
        task_id="jointure_spatiale_poi",
        python_callable=run_gold_2,
        execution_timeout=pendulum.duration(minutes=60),
    )
    debut >> transform >> fin
