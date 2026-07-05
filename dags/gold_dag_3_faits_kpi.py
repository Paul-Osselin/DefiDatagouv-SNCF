# ============================================================
# DAG GOLD 3 — FAITS KPI (frequentation, regularite, accessibilite, POI)
# Projet : Tourisme en train — M1 Big Data & IA — SUP DE VINCI
# Declenche par : dag_orchestrateur apres gold_2
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

URI_DB = "postgresql+psycopg2://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"


def run_gold_3():
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    from sqlalchemy import create_engine

    engine = create_engine(URI_DB, connect_args={"client_encoding": "utf8"})

    dim_gare = pd.read_sql("SELECT gare_key, uic, nom_gare, departement, places_velo FROM dim_gare", engine)

    # -- fact_frequentation
    freq = pd.read_sql("SELECT uic, annee, total_voyageurs FROM silver_frequentation_gares", engine)
    freq = freq.merge(dim_gare[["gare_key", "uic", "departement"]], on="uic", how="inner")
    freq.to_sql("fact_frequentation", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"fact_frequentation : {len(freq):,}")

    # -- fact_regularite
    ter = pd.read_sql("SELECT date_mois, region, taux_regularite, trains_programmes, trains_circules, type_service FROM silver_regularite_ter", engine)
    ter["perimetre"] = ter.get("region", "")
    ic  = pd.read_sql("SELECT date_mois, gare_depart, gare_arrivee, taux_regularite, trains_programmes, trains_circules, type_service FROM silver_regularite_intercites", engine)
    ic["perimetre"] = ic.get("gare_depart", "") + " -> " + ic.get("gare_arrivee", "")
    ic["region"] = None
    cols_communes = ["date_mois", "type_service", "perimetre", "region", "taux_regularite", "trains_programmes", "trains_circules"]
    ter_h = ter[[c for c in cols_communes if c in ter.columns]]
    ic_h  = ic[[c for c in cols_communes if c in ic.columns]]
    fact_reg = pd.concat([ter_h, ic_h], ignore_index=True)
    fact_reg["annee"] = pd.to_datetime(fact_reg["date_mois"], errors="coerce").dt.year
    fact_reg["mois"]  = pd.to_datetime(fact_reg["date_mois"], errors="coerce").dt.month
    fact_reg.to_sql("fact_regularite", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"fact_regularite : {len(fact_reg):,}")

    # -- fact_accessibilite
    pmr = pd.read_sql("SELECT uic, SUM(total_pmr) AS total_pmr_cumul FROM silver_pmr_mensuel GROUP BY uic", engine)
    acc = dim_gare.merge(pmr, on="uic", how="left")
    acc["total_pmr_cumul"] = acc["total_pmr_cumul"].fillna(0).astype(int)
    acc["a_offre_pmr"] = (acc["total_pmr_cumul"] > 0).astype(int)
    acc["a_velo"]      = (acc["places_velo"] > 0).astype(int)
    acc.to_sql("fact_accessibilite", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"fact_accessibilite : {len(acc):,}")

    # -- agg_poi_par_gare
    prox = pd.read_sql("SELECT gare_key, poi_id, distance_km, dans_2km, dans_5km, dans_20km, dans_50km FROM fact_poi_proximite", engine)
    poi_cat = pd.read_sql("SELECT poi_id, categorie FROM silver_poi_categorie", engine)
    dim_cat = pd.read_sql("SELECT categorie, famille FROM dim_categorie_poi", engine)
    poi_fam = poi_cat.merge(dim_cat, on="categorie", how="left")
    poi_fam_unique = poi_fam[poi_fam["famille"] != "Autre"].drop_duplicates(subset=["poi_id", "famille"])
    prox_fam = prox.merge(poi_fam_unique[["poi_id", "famille"]], on="poi_id", how="left")
    prox_fam["famille"] = prox_fam["famille"].fillna("Autre")

    agg_rows = []
    for r in [2, 5, 20, 50]:
        col = f"dans_{r}km"
        sub = prox_fam[prox_fam[col] == 1]
        g = sub.groupby("gare_key").agg(nb_poi=("poi_id", "nunique"), nb_familles=("famille", "nunique")).reset_index()
        g["rayon_km"] = r
        agg_rows.append(g)
    pd.concat(agg_rows, ignore_index=True).to_sql(
        "agg_poi_par_gare", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"agg_poi_par_gare : {sum(len(x) for x in agg_rows):,}")

    # -- agg_poi_famille_gare
    detail_rows = []
    for r in [5, 20, 50]:
        col = f"dans_{r}km"
        sub = prox_fam[prox_fam[col] == 1]
        g = sub.groupby(["gare_key", "famille"]).agg(nb_poi=("poi_id", "nunique")).reset_index()
        g["rayon_km"] = r
        detail_rows.append(g)
    pd.concat(detail_rows, ignore_index=True).to_sql(
        "agg_poi_famille_gare", engine, if_exists="replace", index=False, chunksize=5000, method="multi")
    print(f"agg_poi_famille_gare : {sum(len(x) for x in detail_rows):,}")
    print("=== Gold 3 termine ===")


with DAG(
    default_args={"retries": 0, "retry_delay": pendulum.duration(minutes=1)},
    dag_id="gold_3_faits_kpi",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sncf", "gold", "kpi"],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin   = EmptyOperator(task_id="fin")
    transform = PythonOperator(
        task_id="construire_faits_kpi",
        python_callable=run_gold_3,
        execution_timeout=pendulum.duration(minutes=45),
    )
    debut >> transform >> fin
