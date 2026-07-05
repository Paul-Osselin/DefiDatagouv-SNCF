# ============================================================
# DAG SILVER C — FAMILLE DATATOURISME (POI)
# Projet : Tourisme en train — M1 Big Data & IA — SUP DE VINCI
# Declenche par : dag_orchestrateur (ou manuellement)
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

URI_DB = "postgresql+psycopg2://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"


def run_silver_c():
    import pandas as pd
    import numpy as np
    import warnings
    warnings.filterwarnings("ignore")
    from sqlalchemy import create_engine

    engine = create_engine(URI_DB, connect_args={"client_encoding": "utf8"})

    df = pd.read_sql('SELECT * FROM "datatourisme_sites"', engine)
    print(f"Source : {len(df):,} lignes, {len(df.columns)} colonnes")

    # Normaliser les noms de colonnes en minuscules
    df.columns = [c.lower() for c in df.columns]

    df = df.rename(columns={
        "id"                    : "poi_id",
        "type"                  : "categories_raw",
        "comment_fr"            : "description_fr",
        "label_fr"              : "nom_fr",
        "contact_email"         : "email",
        "contact_telephone"     : "telephone",
        "address_locality"      : "commune",
        "address_postalcode"    : "code_postal",
        "address_streetaddress" : "adresse",
        "department_fr"         : "departement",
        "region_fr"             : "region",
        "latitude"              : "lat_raw",
        "longitude"             : "lon_raw",
        "lastupdate"            : "date_maj",
        "lastupdatedatatourisme": "date_maj_datatourisme",
        "source_file"           : "source_fichier",
    })

    df["latitude"]  = pd.to_numeric(
        df["lat_raw"].astype(str).str.replace(",", ".", regex=False).str.strip(), errors="coerce")
    df["longitude"] = pd.to_numeric(
        df["lon_raw"].astype(str).str.replace(",", ".", regex=False).str.strip(), errors="coerce")

    n_avant = len(df)
    df = df.dropna(subset=["latitude", "longitude"])
    df = df[df["latitude"].between(41.0, 52.0) & df["longitude"].between(-5.5, 10.0)]
    print(f"POI valides : {n_avant} -> {len(df)}")

    df["date_maj"] = pd.to_datetime(df.get("date_maj"), errors="coerce")
    df["date_maj_datatourisme"] = pd.to_datetime(df.get("date_maj_datatourisme"), errors="coerce")

    for col in ["nom_fr", "commune", "departement", "region", "adresse"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", np.nan)

    df = df.drop_duplicates(subset=["poi_id"])
    df = df.drop(columns=["source_fichier", "lat_raw", "lon_raw"], errors="ignore")

    # Table silver_poi
    cols_poi = ["poi_id", "nom_fr", "description_fr", "email", "telephone",
                "commune", "code_postal", "adresse", "departement", "region",
                "latitude", "longitude", "date_maj", "date_maj_datatourisme"]
    cols_poi = [c for c in cols_poi if c in df.columns]
    df[cols_poi].to_sql("silver_poi", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_poi : {len(df):,}")

    # Eclatement categories
    CATEGORIES_EXCLUES = {"PlaceOfInterest", "PointOfInterest", "", "Thing"}
    FAMILLES = {
        "Church":"Patrimoine religieux","Chapel":"Patrimoine religieux",
        "ReligiousSite":"Patrimoine religieux","Cathedral":"Patrimoine religieux",
        "Castle":"Chateaux & monuments","Palace":"Chateaux & monuments",
        "RemarkableBuilding":"Chateaux & monuments","Monument":"Chateaux & monuments",
        "Museum":"Musees & culture","ArtGallery":"Musees & culture",
        "CulturalSite":"Musees & culture","Library":"Musees & culture",
        "Landform":"Nature & paysage","NaturalHeritage":"Nature & paysage",
        "ViewPoint":"Nature & paysage","Cave":"Nature & paysage",
        "Beach":"Plages & littoral",
        "Park":"Parcs & jardins","ParkAndGarden":"Parcs & jardins","Garden":"Parcs & jardins",
        "SportsAndLeisurePlace":"Loisirs & sport","ThemePark":"Loisirs & sport",
        "Zoo":"Loisirs & sport","Aquarium":"Loisirs & sport",
        "WalkingTour":"Randonnee & velo","HikingRoute":"Randonnee & velo","CyclingRoute":"Randonnee & velo",
        "TechnicalHeritage":"Patrimoine technique","Lighthouse":"Patrimoine technique",
        "RemembranceSite":"Memoire & histoire","ArcheologicalSite":"Memoire & histoire",
        "CityHeritage":"Memoire & histoire",
        "Winery":"Vignobles & terroir","Farm":"Vignobles & terroir","Product":"Vignobles & terroir",
        "EntertainmentAndEvent":"Evenements & spectacles","Festival":"Evenements & spectacles",
        "LocalTouristOffice":"Services touristiques","LocalBusiness":"Services touristiques",
        "Accommodation":"Hebergement & restauration","Hotel":"Hebergement & restauration",
        "Restaurant":"Hebergement & restauration","FoodEstablishment":"Hebergement & restauration",
    }

    cat_series = df[["poi_id", "categories_raw"]].dropna(subset=["categories_raw"]).copy()
    cat_long = cat_series.assign(categorie=cat_series["categories_raw"].str.split(";")).explode("categorie")
    cat_long["categorie"] = cat_long["categorie"].str.strip().str.replace("schema:", "", regex=False)
    cat_long = cat_long[~cat_long["categorie"].isin(CATEGORIES_EXCLUES)]
    cat_long = cat_long[cat_long["categorie"].str.len() > 2]
    cat_long["categorie_fr"] = cat_long["categorie"].map(FAMILLES).fillna(cat_long["categorie"])
    cat_long = cat_long.drop(columns=["categories_raw"]).drop_duplicates()

    cat_long[["poi_id", "categorie", "categorie_fr"]].to_sql(
        "silver_poi_categorie", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_poi_categorie : {len(cat_long):,}")
    print("=== Silver C termine ===")


with DAG(
    default_args={"retries": 0, "retry_delay": pendulum.duration(minutes=1)},
    dag_id="silver_C_poi",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sncf", "silver", "poi"],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin   = EmptyOperator(task_id="fin")
    transform = PythonOperator(
        task_id="transformer_poi",
        python_callable=run_silver_c,
        execution_timeout=pendulum.duration(minutes=20),
    )
    debut >> transform >> fin
