from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Drop the ADEME export in the same shared data folder as the GTFS zip.
CSV_PATH = "/opt/airflow/data/Base_Carbone_V23_10.csv"
DB_CONN  = "postgresql://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"

# The export is ISO-8859-1, semicolon-separated, decimal comma.
CSV_ENCODING  = "latin-1"
CSV_SEP       = ";"

# ---------------------------------------------------------------------------
# Curation map - official ADEME Base Carbone element IDs -> our modes.
# Using the stable "Identifiant de l'element" as the selector means the
# extraction survives row reordering or new versions of the export.
#
# distance_basis tells the app which distance to multiply the factor by:
#   'rail'         -> rail path length (ST_Length of the trip line)
#   'road'         -> great-circle distance x 1.3 (road detour approximation)
#   'great_circle' -> straight-line distance between the two stations
#
# Values in the CSV are kgCO2e/passager.km -> multiplied by 1000 -> gCO2e/pkm.
# Factors are per PASSENGER-km (occupancy already included).
# ---------------------------------------------------------------------------

FACTOR_MAP = {
    "43256": {
        "mode": "tgv", "mode_label": "TGV", "category": "train",
        "distance_basis": "rail", "source_year": 2022,
        "notes": "Train grande vitesse, cycle de vie complet (Amont + "
                 "Combustion + Fabrication).",
    },
    "43272": {
        "mode": "intercites", "mode_label": "Intercites", "category": "train",
        "distance_basis": "rail", "source_year": 2022,
        "notes": "Train classique longue distance.",
    },
    "43255": {
        "mode": "ter", "mode_label": "TER", "category": "train",
        "distance_basis": "rail", "source_year": 2022,
        "notes": "Plus eleve que le TGV (diesel sur lignes non electrifiees, "
                 "taux de remplissage plus faible).",
    },
    "43809": {
        "mode": "voiture", "mode_label": "Voiture moyenne", "category": "voiture",
        "distance_basis": "road", "source_year": 2023,
        "notes": "Voiture particuliere, motorisation moyenne, usage mixte, "
                 "taux d'occupation moyen. Distance route approximee par "
                 "vol d'oiseau x 1.3.",
    },
    "48811": {
        "mode": "avion", "mode_label": "Avion court-courrier", "category": "avion",
        "distance_basis": "great_circle", "source_year": 2023,
        "notes": "Vol domestique court-courrier, SANS trainees de condensation "
                 "(valeur officielle pour l'info GES transport). La valeur AVEC "
                 "trainees (ADEME id 48810 = 424 g) reflete l'impact climatique "
                 "reel mais n'est pas reconnue pour l'info GES.",
    },
    "43740": {
        "mode": "autocar", "mode_label": "Autocar (longue distance)", "category": "autocar",
        "distance_basis": "road", "source_year": 2024,
        "notes": "Autocar longue distance (type Flixbus / BlaBlaCar Bus), "
                 "motorisation gazole - quasi tout le parc autocar francais. "
                 "Coherent avec la methodo reglementaire SNCF Voyageurs "
                 "(~38 gCO2e/voy.km, perimetre complet). A comparer aux trains "
                 "grandes lignes (TGV, Intercites).",
    },
}

# Bronze keeps a readable subset of the 67 source columns, all TEXT, verbatim.
BRONZE_COLS = {
    "Identifiant de l'élément":    "ademe_id",
    "Type Ligne":                  "type_ligne",
    "Statut de l'élément":         "statut",
    "Nom base français":           "nom_fr",
    "Nom attribut français":       "attribut_fr",
    "Nom frontière français":      "frontiere_fr",
    "Code de la catégorie":        "code_categorie",
    "Unité français":              "unite_fr",
    "Source":                      "source",
    "Période de validité":         "periode_validite",
    "Total poste non décomposé":   "valeur_totale",
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 6),
    "catchup": False,
}

dag_4_ref_emission_factors = DAG(
    "dag_4_ref_emission_factors",
    schedule=None,
    default_args=default_args,
    description="Ingest the ADEME Base Carbone CSV (passenger transport subset) "
                "into bronze, then derive ref_emission_factors used to compare "
                "the carbon footprint of a train trip against car and plane.",
)

# ---------------------------------------------------------------------------
# Task 1 - Create the bronze table (raw passenger-transport factors, all TEXT)
# ---------------------------------------------------------------------------

def create_bronze_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        col_defs = ",\n            ".join(f"{c} TEXT" for c in BRONZE_COLS.values())
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS bronze_base_carbone_transport (
                _ingested_at TIMESTAMP DEFAULT NOW(),
                {col_defs}
            );
        """))
    engine.dispose()
    print("OK - Table bronze_base_carbone_transport ready")


# ---------------------------------------------------------------------------
# Task 2 - Load the passenger-transport rows from the CSV into bronze
# ---------------------------------------------------------------------------

def load_bronze(**kwargs):
    df = pd.read_csv(
        CSV_PATH, sep=CSV_SEP, encoding=CSV_ENCODING,
        dtype=str, keep_default_na=False, engine="python", quotechar='"',
    )
    print(f"OK - CSV read: {len(df):,} total rows")

    # Keep only the element totals for passenger transport (kgCO2e/passager.km).
    mask = (
        (df["Type Ligne"] == "Elément")
        & df["Unité français"].str.contains("passager.km", regex=False)
    )
    sub = df.loc[mask, list(BRONZE_COLS.keys())].rename(columns=BRONZE_COLS)
    print(f"OK - Passenger-transport element rows: {len(sub):,}")

    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE bronze_base_carbone_transport;"))
    sub.to_sql("bronze_base_carbone_transport", engine,
               if_exists="append", index=False, chunksize=5_000)
    engine.dispose()
    print(f"OK - {len(sub):,} rows loaded into bronze_base_carbone_transport")


# ---------------------------------------------------------------------------
# Task 3 - Create the reference table
# ---------------------------------------------------------------------------

def create_ref_table(**kwargs):
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ref_emission_factors (
                mode            TEXT PRIMARY KEY,
                mode_label      TEXT NOT NULL,
                category        TEXT NOT NULL,             -- train | voiture | avion
                gco2e_per_km    DOUBLE PRECISION NOT NULL, -- per passenger-km
                distance_basis  TEXT NOT NULL,             -- rail | road | great_circle
                ademe_id        TEXT,                      -- traceability to Base Carbone
                source          TEXT,
                source_year     INTEGER,
                notes           TEXT,
                _loaded_at      TIMESTAMP DEFAULT NOW()
            );
        """))
    engine.dispose()
    print("OK - Table ref_emission_factors ready")


# ---------------------------------------------------------------------------
# Task 4 - Derive ref_emission_factors from bronze (curate + convert units)
# ---------------------------------------------------------------------------

def build_ref_from_bronze(**kwargs):
    engine = create_engine(DB_CONN)
    ids = tuple(FACTOR_MAP.keys())

    with engine.connect() as conn:
        bronze = pd.read_sql(
            text("""
                SELECT ademe_id, nom_fr, unite_fr, source, valeur_totale
                FROM bronze_base_carbone_transport
                WHERE ademe_id IN :ids
            """),
            conn, params={"ids": ids},
        )

    missing = set(FACTOR_MAP) - set(bronze["ademe_id"])
    if missing:
        raise ValueError(f"ADEME IDs not found in bronze: {missing} - "
                         f"check the CSV version / load step.")

    rows = []
    for _, b in bronze.iterrows():
        meta = FACTOR_MAP[b["ademe_id"]]
        # kgCO2e/passager.km (decimal comma) -> gCO2e/passenger-km
        gco2e_per_km = round(float(b["valeur_totale"].replace(",", ".")) * 1000, 3)
        rows.append({
            "mode":           meta["mode"],
            "mode_label":     meta["mode_label"],
            "category":       meta["category"],
            "gco2e_per_km":   gco2e_per_km,
            "distance_basis": meta["distance_basis"],
            "ademe_id":       b["ademe_id"],
            "source":         f"ADEME Base Carbone - {b['source'] or 'element ' + b['ademe_id']}",
            "source_year":    meta["source_year"],
            "notes":          meta["notes"],
        })

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE ref_emission_factors;"))
        conn.execute(text("""
            INSERT INTO ref_emission_factors
                (mode, mode_label, category, gco2e_per_km,
                 distance_basis, ademe_id, source, source_year, notes)
            VALUES
                (:mode, :mode_label, :category, :gco2e_per_km,
                 :distance_basis, :ademe_id, :source, :source_year, :notes)
        """), rows)
        count = conn.execute(text("SELECT COUNT(*) FROM ref_emission_factors;")).scalar()
    engine.dispose()

    for r in sorted(rows, key=lambda x: x["gco2e_per_km"]):
        print(f"  {r['mode']:12} {r['gco2e_per_km']:>7} gCO2e/pkm  ({r['distance_basis']})")
    print(f"OK - {count} emission factors loaded into ref_emission_factors")


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

task_create_bronze = PythonOperator(
    task_id="task_create_bronze_table",
    python_callable=create_bronze_table,
    dag=dag_4_ref_emission_factors,
)

task_load_bronze = PythonOperator(
    task_id="task_load_bronze",
    python_callable=load_bronze,
    dag=dag_4_ref_emission_factors,
)

task_create_ref = PythonOperator(
    task_id="task_create_ref_table",
    python_callable=create_ref_table,
    dag=dag_4_ref_emission_factors,
)

task_build_ref = PythonOperator(
    task_id="task_build_ref_from_bronze",
    python_callable=build_ref_from_bronze,
    dag=dag_4_ref_emission_factors,
)

# ---------------------------------------------------------------------------
# Pipeline
#
#   task_create_bronze_table
#           v
#   task_load_bronze
#           v
#   task_create_ref_table
#           v
#   task_build_ref_from_bronze
# ---------------------------------------------------------------------------

task_create_bronze >> task_load_bronze >> task_create_ref >> task_build_ref
