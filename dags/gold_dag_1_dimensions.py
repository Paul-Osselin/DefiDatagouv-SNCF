# ============================================================
# DAG GOLD 1 — DIMENSIONS + REFERENTIEL GARE MAITRE
# Projet : Tourisme en train — M1 Big Data & IA — SUP DE VINCI
# Declenche par : dag_orchestrateur apres Silver A+B+C
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

URI_DB = "postgresql+psycopg2://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"


def run_gold_1():
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    from sqlalchemy import create_engine

    engine = create_engine(URI_DB, connect_args={"client_encoding": "utf8"})

    # -- dim_gare
    gv = pd.read_sql("SELECT * FROM silver_gares_voyageurs", engine)
    lg = pd.read_sql("SELECT uic, latitude AS lat_lg, longitude AS lon_lg, departement, commune FROM silver_liste_gare", engine).drop_duplicates(subset=["uic"])
    dim_gare = gv.merge(lg, on="uic", how="left")
    dim_gare["latitude"]  = dim_gare["latitude"].combine_first(dim_gare["lat_lg"])
    dim_gare["longitude"] = dim_gare["longitude"].combine_first(dim_gare["lon_lg"])
    dim_gare["departement"] = dim_gare.get("departement_x", dim_gare.get("departement", "")).fillna("")
    dim_gare["commune"]     = dim_gare.get("commune_x",     dim_gare.get("commune", "")).fillna("")

    gtfs = pd.read_sql("SELECT stop_id, uic_from_parent FROM silver_gtfs_stops WHERE uic_from_parent IS NOT NULL", engine)
    gtfs = gtfs.dropna().drop_duplicates(subset=["uic_from_parent"]).rename(columns={"uic_from_parent": "uic", "stop_id": "gtfs_stop_id"})
    dim_gare = dim_gare.merge(gtfs, on="uic", how="left")

    velo = pd.read_sql("SELECT trigramme, places_velo_dec2024 FROM silver_stationnement_velo", engine).drop_duplicates(subset=["trigramme"])
    dim_gare = dim_gare.merge(velo, on="trigramme", how="left")
    dim_gare["places_velo"] = dim_gare.get("places_velo_dec2024", pd.Series(0)).fillna(0).astype(int)

    cols = ["uic", "nom_gare", "trigramme", "segment_drg", "latitude", "longitude", "commune", "departement", "gtfs_stop_id", "places_velo"]
    cols = [c for c in cols if c in dim_gare.columns]
    dim_gare = dim_gare[cols].drop_duplicates(subset=["uic"]).reset_index(drop=True)
    dim_gare.insert(0, "gare_key", dim_gare.index + 1)
    dim_gare.to_sql("dim_gare", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"dim_gare : {len(dim_gare):,}")

    # -- dim_categorie_poi
    cats = pd.read_sql("SELECT DISTINCT categorie FROM silver_poi_categorie", engine)
    FAMILLES = {
        "Church":"Patrimoine religieux","Chapel":"Patrimoine religieux","ReligiousSite":"Patrimoine religieux",
        "Castle":"Chateaux & monuments","RemarkableBuilding":"Chateaux & monuments","Monument":"Chateaux & monuments",
        "Museum":"Musees & culture","CulturalSite":"Musees & culture","Library":"Musees & culture",
        "Landform":"Nature & paysage","NaturalHeritage":"Nature & paysage","ViewPoint":"Nature & paysage",
        "Beach":"Plages & littoral",
        "Park":"Parcs & jardins","ParkAndGarden":"Parcs & jardins",
        "SportsAndLeisurePlace":"Loisirs & sport","ThemePark":"Loisirs & sport",
        "WalkingTour":"Randonnee & velo","HikingRoute":"Randonnee & velo",
        "TechnicalHeritage":"Patrimoine technique","CityHeritage":"Memoire & histoire",
        "Winery":"Vignobles & terroir","Product":"Vignobles & terroir",
        "EntertainmentAndEvent":"Evenements & spectacles",
        "LocalTouristOffice":"Services touristiques","LocalBusiness":"Services touristiques",
        "Accommodation":"Hebergement & restauration","Restaurant":"Hebergement & restauration",
    }
    cats["famille"] = cats["categorie"].map(FAMILLES).fillna("Autre")
    cats = cats.reset_index(drop=True)
    cats.insert(0, "categorie_key", cats.index + 1)
    cats.to_sql("dim_categorie_poi", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"dim_categorie_poi : {len(cats):,}")

    # -- dim_region
    regions = pd.read_sql("SELECT DISTINCT region FROM silver_poi WHERE region IS NOT NULL", engine)["region"].dropna().unique()
    dim_region = pd.DataFrame({"region_nom": sorted(regions)})
    dim_region.insert(0, "region_key", dim_region.index + 1)
    dim_region.to_sql("dim_region", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"dim_region : {len(dim_region):,}")

    # -- dim_temps
    lignes = []
    for annee in range(2015, 2025):
        lignes.append({"annee": annee, "mois": None, "type_grain": "annee"})
    for annee in range(2013, 2027):
        for mois in range(1, 13):
            lignes.append({"annee": annee, "mois": mois, "type_grain": "mois"})
    dim_temps = pd.DataFrame(lignes)
    dim_temps.insert(0, "temps_key", dim_temps.index + 1)
    dim_temps.to_sql("dim_temps", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"dim_temps : {len(dim_temps):,}")

    # -- dim_ligne
    lr  = pd.read_sql("SELECT code_ligne, region FROM silver_ref_lignes_region", engine)
    ls  = pd.read_sql("SELECT code_ligne, statut FROM silver_ref_lignes_statut", engine)
    lt  = pd.read_sql("SELECT code_ligne, type_ligne FROM silver_ref_lignes_type", engine)
    lgv = pd.read_sql("SELECT code_ligne, categorie_ligne FROM silver_ref_lignes_ecartement", engine)
    nom = pd.read_sql("SELECT code_ligne, nom_ligne FROM silver_ref_lignes_nom", engine)
    lr_agg = lr.dropna(subset=["region"]).groupby("code_ligne")["region"].apply(lambda x: " | ".join(sorted(x.unique()))).reset_index()
    tous = pd.concat([lr[["code_ligne"]], ls[["code_ligne"]], lt[["code_ligne"]], lgv[["code_ligne"]], nom[["code_ligne"]]]).drop_duplicates()
    dim_ligne = (tous.merge(nom.drop_duplicates("code_ligne"), on="code_ligne", how="left")
                     .merge(lr_agg, on="code_ligne", how="left")
                     .merge(ls.drop_duplicates("code_ligne"), on="code_ligne", how="left")
                     .merge(lt.drop_duplicates("code_ligne"), on="code_ligne", how="left")
                     .merge(lgv.drop_duplicates("code_ligne"), on="code_ligne", how="left"))
    dim_ligne["nom_ligne"] = dim_ligne.get("nom_ligne", pd.Series("")).fillna("Ligne " + dim_ligne["code_ligne"].astype(str))
    dim_ligne = dim_ligne.reset_index(drop=True)
    dim_ligne.insert(0, "ligne_key", dim_ligne.index + 1)
    dim_ligne.to_sql("dim_ligne", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"dim_ligne : {len(dim_ligne):,}")
    print("=== Gold 1 termine ===")


with DAG(
    default_args={"retries": 0, "retry_delay": pendulum.duration(minutes=1)},
    dag_id="gold_1_dimensions",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sncf", "gold", "dimensions"],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin   = EmptyOperator(task_id="fin")
    transform = PythonOperator(
        task_id="construire_dimensions",
        python_callable=run_gold_1,
        execution_timeout=pendulum.duration(minutes=15),
    )
    debut >> transform >> fin
