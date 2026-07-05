# ============================================================
# DAG SILVER B — FAMILLE REFERENTIEL GARES (14 tables brut_*)
# Projet : Tourisme en train — M1 Big Data & IA — SUP DE VINCI
# Declenche par : dag_orchestrateur (ou manuellement)
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

URI_DB = "postgresql+psycopg2://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"


def run_silver_b():
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    from sqlalchemy import create_engine

    engine = create_engine(URI_DB, connect_args={"client_encoding": "utf8"})

    # -- silver_gares_voyageurs
    gv = pd.read_sql('SELECT * FROM "brut_gares_voyageurs"', engine)
    gv.columns = [c.lower() for c in gv.columns]
    gv = gv.rename(columns={"nom_gare": "nom_gare", "trigramme": "trigramme",
                             "segment(s) drg": "segment_drg",
                             "position géographique": "position_geo",
                             "code commune": "code_commune",
                             "code_uic": "code_uic_raw", "id_gare": "id_gare"})
    gv["uic"] = gv.get("code_uic_raw", gv.get("code_uic", "")).astype(str).str.strip().str.zfill(8)
    gv["trigramme"] = gv.get("trigramme", pd.Series("")).astype(str).str.strip().str.upper()
    if "position_geo" in gv.columns:
        coords = gv["position_geo"].str.split(",", expand=True)
        gv["latitude"]  = pd.to_numeric(coords[0].str.strip().str.replace(",", ".", regex=False), errors="coerce")
        gv["longitude"] = pd.to_numeric(coords[1].str.strip().str.replace(",", ".", regex=False), errors="coerce")
    gv = gv[gv["latitude"].between(41.0, 52.0) & gv["longitude"].between(-5.5, 10.0)]
    gv = gv.drop_duplicates(subset=["uic"])
    gv[["uic", "nom_gare", "trigramme", "segment_drg", "latitude", "longitude", "code_commune", "id_gare"]].to_sql(
        "silver_gares_voyageurs", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_gares_voyageurs : {len(gv):,}")

    # -- silver_liste_gare
    lg = pd.read_sql('SELECT * FROM "brut_liste_gare"', engine)
    lg.columns = [c.lower() for c in lg.columns]
    lg = lg.rename(columns={"code_uic": "uic_raw", "libelle": "libelle", "x_wgs84": "longitude", "y_wgs84": "latitude",
                             "commune": "commune", "departemen": "departement", "code_ligne": "code_ligne",
                             "fret": "fret", "voyageurs": "voyageurs"})
    lg["uic"] = lg["uic_raw"].astype(str).str.zfill(8)
    lg["latitude"]  = pd.to_numeric(lg["latitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    lg["longitude"] = pd.to_numeric(lg["longitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    lg = lg[lg["latitude"].between(41.0, 52.0) & lg["longitude"].between(-5.5, 10.0)]
    lg = lg.drop_duplicates(subset=["uic"])
    lg[["uic", "libelle", "fret", "voyageurs", "code_ligne", "commune", "departement", "latitude", "longitude"]].to_sql(
        "silver_liste_gare", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_liste_gare : {len(lg):,}")

    # -- silver_frequentation_gares (format long)
    freq = pd.read_sql('SELECT * FROM "brut_frequentation_gares"', engine)
    freq.columns = [c.lower() for c in freq.columns]
    uic_col = next((c for c in freq.columns if "uic" in c), None)
    nom_col = next((c for c in freq.columns if "gare" in c and "nom" in c), None)
    if uic_col:
        freq["uic"] = freq[uic_col].astype(str).str.zfill(8)
    lignes_long = []
    for annee in range(2015, 2025):
        col_candidates = [c for c in freq.columns if str(annee) in c and "voyageur" in c.lower()]
        if col_candidates:
            tmp = freq[["uic", col_candidates[0]]].copy()
            tmp = tmp.rename(columns={col_candidates[0]: "total_voyageurs"})
            tmp["annee"] = annee
            tmp["total_voyageurs"] = pd.to_numeric(tmp["total_voyageurs"], errors="coerce").fillna(0).astype(int)
            lignes_long.append(tmp[tmp["total_voyageurs"] > 0])
    if lignes_long:
        pd.concat(lignes_long, ignore_index=True).to_sql(
            "silver_frequentation_gares", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_frequentation_gares : {sum(len(x) for x in lignes_long):,}")

    # -- silver_pmr_mensuel
    pmr = pd.read_sql('SELECT * FROM "brut_accompagnement_pmr_gare"', engine)
    pmr.columns = [c.lower() for c in pmr.columns]
    uic_col = next((c for c in pmr.columns if "uic" in c), None)
    date_col = next((c for c in pmr.columns if "date" in c), None)
    total_col = next((c for c in pmr.columns if "total" in c), None)
    renames = {}
    if uic_col: renames[uic_col] = "uic_raw"
    if date_col: renames[date_col] = "date_mensuel"
    if total_col: renames[total_col] = "total_pmr"
    pmr = pmr.rename(columns=renames)
    if "uic_raw" in pmr.columns:
        pmr["uic"] = pmr["uic_raw"].astype(str).str.zfill(8)
    if "date_mensuel" in pmr.columns:
        pmr["date_mensuel"] = pd.to_datetime(pmr["date_mensuel"], errors="coerce")
    if "total_pmr" in pmr.columns:
        pmr["total_pmr"] = pd.to_numeric(pmr["total_pmr"], errors="coerce").fillna(0).astype(int)
    pmr.to_sql("silver_pmr_mensuel", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_pmr_mensuel : {len(pmr):,}")

    # -- silver_stationnement_velo
    velo = pd.read_sql('SELECT * FROM "brut_stationnement_velo"', engine)
    velo.columns = [c.lower() for c in velo.columns]
    trig_col = next((c for c in velo.columns if "gare" in c and "code" in c), "code gare")
    velo = velo.rename(columns={trig_col: "trigramme"})
    velo["trigramme"] = velo["trigramme"].astype(str).str.strip().str.upper()
    place_cols = [c for c in velo.columns if "places" in c or "nombre" in c]
    if len(place_cols) >= 2:
        velo = velo.rename(columns={place_cols[0]: "places_velo_juin2024", place_cols[1]: "places_velo_dec2024"})
        for c in ["places_velo_juin2024", "places_velo_dec2024"]:
            velo[c] = pd.to_numeric(velo[c], errors="coerce").fillna(0).astype(int)
    velo = velo.drop_duplicates(subset=["trigramme"])
    velo.to_sql("silver_stationnement_velo", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_stationnement_velo : {len(velo):,}")

    # -- silver_regularite_ter
    ter = pd.read_sql('SELECT * FROM "brut_regularite_ter"', engine)
    ter.columns = [c.lower() for c in ter.columns]
    date_col = next((c for c in ter.columns if "date" in c), None)
    reg_col  = next((c for c in ter.columns if "régularité" in c or "regularite" in c or "taux" in c), None)
    reg_col2 = next((c for c in ter.columns if "région" in c or "region" in c), None)
    renames = {}
    if date_col: renames[date_col] = "date_mois"
    if reg_col:  renames[reg_col]  = "taux_regularite"
    if reg_col2: renames[reg_col2] = "region"
    ter = ter.rename(columns=renames)
    if "date_mois" in ter.columns:
        ter["date_mois"] = pd.to_datetime(ter["date_mois"], errors="coerce")
    if "taux_regularite" in ter.columns:
        ter["taux_regularite"] = pd.to_numeric(ter["taux_regularite"], errors="coerce")
    ter["type_service"] = "TER"
    ter.to_sql("silver_regularite_ter", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_regularite_ter : {len(ter):,}")

    # -- silver_regularite_intercites
    ic = pd.read_sql('SELECT * FROM "brut_regularite_intercites"', engine)
    ic.columns = [c.lower() for c in ic.columns]
    date_col = next((c for c in ic.columns if "date" in c), None)
    reg_col  = next((c for c in ic.columns if "taux" in c), None)
    renames = {}
    if date_col: renames[date_col] = "date_mois"
    if reg_col:  renames[reg_col]  = "taux_regularite"
    ic = ic.rename(columns=renames)
    if "date_mois" in ic.columns:
        ic["date_mois"] = pd.to_datetime(ic["date_mois"], errors="coerce")
    ic["type_service"] = "Intercites"
    ic.to_sql("silver_regularite_intercites", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_regularite_intercites : {len(ic):,}")

    # -- silver_ref_lignes_* (5 tables)
    for nom, table, col_code, col_val, col_out in [
        ("region",    "brut_lignes_par_region",   "CODE_LIGNE", "REGION",     "region"),
        ("statut",    "brut_lignes_par_statut",    "CODE_LIGNE", "STATUT",     "statut"),
        ("type",      "brut_lignes_par_type",      "CODE_LIGNE", "TYPE_LIGNE", "type_ligne"),
        ("ecartement","brut_lgv_ecartement",       "CODE_LIGNE", "CATLIG",     "categorie_ligne"),
        ("nom",       "brut_mode_cantonnement",    "CODE_LIGNE", "LIB_LIGNE",  "nom_ligne"),
    ]:
        try:
            df = pd.read_sql(f'SELECT "{col_code}", "{col_val}" FROM "{table}"', engine)
            df.columns = ["code_ligne", col_out]
            df["code_ligne"] = pd.to_numeric(df["code_ligne"], errors="coerce").astype("Int64")
            df = df.dropna(subset=["code_ligne"])
            dedup_cols = ["code_ligne"] if nom != "region" else ["code_ligne", col_out]
            df = df.drop_duplicates(subset=dedup_cols)
            df.to_sql(f"silver_ref_lignes_{nom}", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
            print(f"silver_ref_lignes_{nom} : {len(df):,}")
        except Exception as e:
            print(f"[WARN] silver_ref_lignes_{nom} : {e}")

    # -- silver_horaires_gares
    hg = pd.read_sql('SELECT * FROM "brut_horaires_gares"', engine)
    hg.columns = [c.lower() for c in hg.columns]
    uic_col = next((c for c in hg.columns if "uic" in c), None)
    if uic_col:
        hg["uic"] = hg[uic_col].astype(str).str.zfill(8)
    hg.to_sql("silver_horaires_gares", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    print(f"silver_horaires_gares : {len(hg):,}")
    print("=== Silver B termine ===")


with DAG(
    default_args={"retries": 0, "retry_delay": pendulum.duration(minutes=1)},
    dag_id="silver_B_gares",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sncf", "silver", "gares"],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin   = EmptyOperator(task_id="fin")
    transform = PythonOperator(
        task_id="transformer_gares",
        python_callable=run_silver_b,
        execution_timeout=pendulum.duration(minutes=30),
    )
    debut >> transform >> fin
