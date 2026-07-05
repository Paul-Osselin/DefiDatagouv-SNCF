# ============================================================
# DAG GOLD 4 — SCORE ATTRACTIVITE + CARBONE
# Projet : Tourisme en train — M1 Big Data & IA — SUP DE VINCI
# Declenche par : dag_orchestrateur apres gold_3
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

URI_DB = "postgresql+psycopg2://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"

# ── CONFIG SCORE ─────────────────────────────────────────────
POIDS_TOURISME      = 0.40
POIDS_FREQUENTATION = 0.20
POIDS_ACCESSIBILITE = 0.25
POIDS_REGULARITE    = 0.15
POIDS_NB_POI        = 0.3
POIDS_DIVERSITE     = 0.7
RAYON_SCORE_KM      = 5
SEUIL_SCORE_KPI8    = 70
# ── CONFIG CARBONE ───────────────────────────────────────────
CO2_TRAIN, CO2_VOITURE, CO2_AVION = 6, 193, 230
CORRECTEUR_RAIL   = 1.2
CO2_PAR_ARBRE_KG  = 25
TOP_N_GARES       = 100
# ─────────────────────────────────────────────────────────────

MAP_REGION_TER = {
    "Alsace":"Grand Est","Lorraine":"Grand Est","Champagne Ardenne":"Grand Est","Grand Est":"Grand Est",
    "Bourgogne":"Bourgogne-Franche-Comte","Franche Comté":"Bourgogne-Franche-Comte","Bourgogne-Franche-Comté":"Bourgogne-Franche-Comte",
    "Auvergne":"Auvergne-Rhone-Alpes","Rhône Alpes":"Auvergne-Rhone-Alpes","Auvergne-Rhône-Alpes":"Auvergne-Rhone-Alpes",
    "Basse Normandie":"Normandie","Haute Normandie":"Normandie","Normandie":"Normandie",
    "Centre":"Centre-Val de Loire","Centre Val-de-Loire":"Centre-Val de Loire",
    "Nord Pas de Calais":"Hauts-de-France","Picardie":"Hauts-de-France","Hauts-de-France":"Hauts-de-France","Etoile Amiens":"Hauts-de-France",
    "Aquitaine":"Nouvelle-Aquitaine","Limousin":"Nouvelle-Aquitaine","Poitou Charentes":"Nouvelle-Aquitaine","Nouvelle Aquitaine":"Nouvelle-Aquitaine",
    "Languedoc Roussillon":"Occitanie","Midi Pyrénées":"Occitanie","Occitanie":"Occitanie",
    "Provence Alpes Côte d'Azur":"Provence-Alpes-Cote d'Azur","Sud Azur":"Provence-Alpes-Cote d'Azur",
    "Bretagne":"Bretagne","Pays-de-la-Loire":"Pays de la Loire","Loire Océan":"Pays de la Loire",
}

NOM_DEPT_TO_REGION = {
    "AIN":"Auvergne-Rhone-Alpes","ALLIER":"Auvergne-Rhone-Alpes","ARDECHE":"Auvergne-Rhone-Alpes",
    "CANTAL":"Auvergne-Rhone-Alpes","DROME":"Auvergne-Rhone-Alpes","ISERE":"Auvergne-Rhone-Alpes",
    "LOIRE":"Auvergne-Rhone-Alpes","HAUTE LOIRE":"Auvergne-Rhone-Alpes","PUY DE DOME":"Auvergne-Rhone-Alpes",
    "RHONE":"Auvergne-Rhone-Alpes","SAVOIE":"Auvergne-Rhone-Alpes","HAUTE SAVOIE":"Auvergne-Rhone-Alpes",
    "COTE D OR":"Bourgogne-Franche-Comte","DOUBS":"Bourgogne-Franche-Comte","JURA":"Bourgogne-Franche-Comte",
    "NIEVRE":"Bourgogne-Franche-Comte","HAUTE SAONE":"Bourgogne-Franche-Comte","SAONE ET LOIRE":"Bourgogne-Franche-Comte",
    "YONNE":"Bourgogne-Franche-Comte","TERRITOIRE DE BELFORT":"Bourgogne-Franche-Comte",
    "COTES D ARMOR":"Bretagne","FINISTERE":"Bretagne","ILLE ET VILAINE":"Bretagne","MORBIHAN":"Bretagne",
    "CHER":"Centre-Val de Loire","EURE ET LOIR":"Centre-Val de Loire","INDRE":"Centre-Val de Loire",
    "INDRE ET LOIRE":"Centre-Val de Loire","LOIR ET CHER":"Centre-Val de Loire","LOIRET":"Centre-Val de Loire",
    "CORSE DU SUD":"Corse","HAUTE CORSE":"Corse",
    "ARDENNES":"Grand Est","AUBE":"Grand Est","MARNE":"Grand Est","HAUTE MARNE":"Grand Est",
    "MEURTHE ET MOSELLE":"Grand Est","MEUSE":"Grand Est","MOSELLE":"Grand Est",
    "BAS RHIN":"Grand Est","HAUT RHIN":"Grand Est","VOSGES":"Grand Est",
    "AISNE":"Hauts-de-France","NORD":"Hauts-de-France","OISE":"Hauts-de-France",
    "PAS DE CALAIS":"Hauts-de-France","SOMME":"Hauts-de-France",
    "PARIS":"Ile-de-France","SEINE ET MARNE":"Ile-de-France","YVELINES":"Ile-de-France",
    "ESSONNE":"Ile-de-France","HAUTS DE SEINE":"Ile-de-France","SEINE SAINT DENIS":"Ile-de-France",
    "VAL DE MARNE":"Ile-de-France","VAL D OISE":"Ile-de-France",
    "CALVADOS":"Normandie","EURE":"Normandie","MANCHE":"Normandie","ORNE":"Normandie","SEINE MARITIME":"Normandie",
    "CHARENTE":"Nouvelle-Aquitaine","CHARENTE MARITIME":"Nouvelle-Aquitaine","CORREZE":"Nouvelle-Aquitaine",
    "CREUSE":"Nouvelle-Aquitaine","DORDOGNE":"Nouvelle-Aquitaine","GIRONDE":"Nouvelle-Aquitaine",
    "LANDES":"Nouvelle-Aquitaine","LOT ET GARONNE":"Nouvelle-Aquitaine","PYRENEES ATLANTIQUES":"Nouvelle-Aquitaine",
    "DEUX SEVRES":"Nouvelle-Aquitaine","VIENNE":"Nouvelle-Aquitaine","HAUTE VIENNE":"Nouvelle-Aquitaine",
    "ARIEGE":"Occitanie","AUDE":"Occitanie","AVEYRON":"Occitanie","GARD":"Occitanie","HAUTE GARONNE":"Occitanie",
    "GERS":"Occitanie","HERAULT":"Occitanie","LOT":"Occitanie","LOZERE":"Occitanie",
    "HAUTES PYRENEES":"Occitanie","PYRENEES ORIENTALES":"Occitanie","TARN":"Occitanie","TARN ET GARONNE":"Occitanie",
    "LOIRE ATLANTIQUE":"Pays de la Loire","MAINE ET LOIRE":"Pays de la Loire","MAYENNE":"Pays de la Loire",
    "SARTHE":"Pays de la Loire","VENDEE":"Pays de la Loire",
    "ALPES DE HAUTE PROVENCE":"Provence-Alpes-Cote d'Azur","HAUTES ALPES":"Provence-Alpes-Cote d'Azur",
    "ALPES MARITIMES":"Provence-Alpes-Cote d'Azur","BOUCHES DU RHONE":"Provence-Alpes-Cote d'Azur",
    "VAR":"Provence-Alpes-Cote d'Azur","VAUCLUSE":"Provence-Alpes-Cote d'Azur",
}


def run_gold_4():
    import pandas as pd
    import numpy as np
    import unicodedata
    import warnings
    warnings.filterwarnings("ignore")
    from sqlalchemy import create_engine

    engine = create_engine(URI_DB, connect_args={"client_encoding": "utf8"})

    def normaliser_percentile(s): return s.rank(pct=True) * 100
    def norm_txt(s):
        if pd.isna(s): return ""
        s = str(s).strip().upper()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        return " ".join(s.replace("-", " ").replace("'", " ").split())

    noms_ref = list(NOM_DEPT_TO_REGION.keys())
    def trouver_region(dep):
        n = norm_txt(dep)
        if not n: return None
        if n in NOM_DEPT_TO_REGION: return NOM_DEPT_TO_REGION[n]
        for ref in noms_ref:
            if ref.startswith(n) or n.startswith(ref): return NOM_DEPT_TO_REGION[ref]
        return None

    gares = pd.read_sql("SELECT gare_key, uic, nom_gare, departement, latitude, longitude FROM dim_gare", engine)

    # Score tourisme
    poi_agg = pd.read_sql(f"SELECT gare_key, nb_poi, nb_familles FROM agg_poi_par_gare WHERE rayon_km = {RAYON_SCORE_KM}", engine)
    gares = gares.merge(poi_agg, on="gare_key", how="left")
    gares["nb_poi"]      = gares["nb_poi"].fillna(0)
    gares["nb_familles"] = gares["nb_familles"].fillna(0)
    moy_dept = gares[gares["nb_poi"] > 0].groupby("departement")["nb_poi"].mean().rename("moy_poi_dept")
    gares = gares.merge(moy_dept, on="departement", how="left")
    gares["moy_poi_dept"] = gares["moy_poi_dept"].fillna(gares["nb_poi"].replace(0, 1).mean())
    gares["nb_poi_relatif"] = gares["nb_poi"] / gares["moy_poi_dept"].replace(0, 1)
    gares["score_tourisme"] = (POIDS_NB_POI * normaliser_percentile(gares["nb_poi_relatif"]) +
                               POIDS_DIVERSITE * normaliser_percentile(gares["nb_familles"]))

    # Score frequentation
    freq = pd.read_sql("SELECT gare_key, MAX(total_voyageurs) AS voyageurs FROM fact_frequentation GROUP BY gare_key", engine)
    gares = gares.merge(freq, on="gare_key", how="left")
    gares["voyageurs"] = gares["voyageurs"].fillna(0)
    gares["score_frequentation"] = normaliser_percentile(gares["voyageurs"])

    # Score accessibilite
    acc = pd.read_sql("SELECT gare_key, total_pmr_cumul, places_velo FROM fact_accessibilite", engine)
    gares = gares.merge(acc, on="gare_key", how="left")
    gares["total_pmr_cumul"] = gares["total_pmr_cumul"].fillna(0)
    gares["places_velo"]     = gares["places_velo"].fillna(0)
    gares["score_accessibilite"] = (0.5 * normaliser_percentile(gares["total_pmr_cumul"]) +
                                    0.5 * normaliser_percentile(gares["places_velo"]))

    # Score regularite
    gares["region_admin"] = gares["departement"].apply(trouver_region)
    reg_brut = pd.read_sql("SELECT region, taux_regularite, annee FROM fact_regularite WHERE type_service = 'TER' AND taux_regularite IS NOT NULL", engine)
    reg_brut["region_actuelle"] = reg_brut["region"].map(MAP_REGION_TER)
    reg_recent = reg_brut[reg_brut["annee"] >= 2018]
    regions_recent = set(reg_recent["region_actuelle"].dropna().unique())
    reg_pour_calcul = pd.concat([reg_recent, reg_brut[~reg_brut["region_actuelle"].isin(regions_recent)]])
    reg_region = (reg_pour_calcul.dropna(subset=["region_actuelle"])
                  .groupby("region_actuelle")["taux_regularite"].mean().reset_index()
                  .rename(columns={"region_actuelle": "region", "taux_regularite": "taux_moyen"}))
    reg_region["region_norm"] = reg_region["region"].apply(norm_txt)
    gares["region_norm"] = gares["region_admin"].apply(norm_txt)
    map_taux = dict(zip(reg_region["region_norm"], reg_region["taux_moyen"]))
    taux_nat = reg_region["taux_moyen"].mean()
    def taux_pour_region(rn):
        if not rn: return None
        if rn in map_taux: return map_taux[rn]
        for k, v in map_taux.items():
            if k.startswith(rn) or rn.startswith(k): return v
        return None
    gares["taux_regularite"] = gares["region_norm"].apply(taux_pour_region).fillna(taux_nat)
    gares["score_regularite"] = normaliser_percentile(gares["taux_regularite"])

    # Score global
    gares["score_global"] = (POIDS_TOURISME * gares["score_tourisme"] +
                             POIDS_FREQUENTATION * gares["score_frequentation"] +
                             POIDS_ACCESSIBILITE * gares["score_accessibilite"] +
                             POIDS_REGULARITE * gares["score_regularite"]).round(1)
    for c in ["score_tourisme", "score_frequentation", "score_accessibilite", "score_regularite"]:
        gares[c] = gares[c].round(1)

    cols = ["gare_key", "uic", "nom_gare", "departement", "region_admin", "latitude", "longitude",
            "nb_poi", "nb_poi_relatif", "moy_poi_dept", "nb_familles",
            "voyageurs", "total_pmr_cumul", "places_velo", "taux_regularite",
            "score_tourisme", "score_frequentation", "score_accessibilite", "score_regularite", "score_global"]
    gares[[c for c in cols if c in gares.columns]].to_sql(
        "fact_score_gare", engine, if_exists="replace", index=False, chunksize=1000, method="multi")
    n_above = (gares["score_global"] > SEUIL_SCORE_KPI8).sum()
    print(f"fact_score_gare : {len(gares):,} gares, KPI 8 : {n_above} avec score > {SEUIL_SCORE_KPI8}")

    # Carbone
    def haversine_km(lat1, lon1, lat2, lon2):
        R = 6371.0
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        a = np.sin((lat2-lat1)/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin((lon2-lon1)/2)**2
        return R * 2 * np.arcsin(np.sqrt(a))

    top = gares.nlargest(TOP_N_GARES, "voyageurs")[["gare_key", "uic", "nom_gare", "latitude", "longitude"]].to_dict("records")
    lignes = []
    for i in range(len(top)):
        for j in range(i+1, len(top)):
            o, d = top[i], top[j]
            dv = haversine_km(o["latitude"], o["longitude"], d["latitude"], d["longitude"])
            dr = dv * CORRECTEUR_RAIL
            if dr < 50: continue
            co2_t = dr * CO2_TRAIN / 1000
            co2_v = dr * CO2_VOITURE / 1000
            co2_a = dr * CO2_AVION / 1000
            lignes.append((o["gare_key"], o["uic"], o["nom_gare"],
                           d["gare_key"], d["uic"], d["nom_gare"],
                           round(dv, 1), round(dr, 1),
                           round(co2_t, 2), round(co2_v, 2), round(co2_a, 2),
                           round(co2_v - co2_t, 2), round(co2_a - co2_t, 2),
                           round((co2_v - co2_t) / CO2_PAR_ARBRE_KG, 2)))
    pd.DataFrame(lignes, columns=["gare_dep_key","uic_dep","gare_depart","gare_arr_key","uic_arr","gare_arrivee",
                                   "distance_vol_km","distance_rail_km","co2_train_kg","co2_voiture_kg","co2_avion_kg",
                                   "co2_evite_voiture_kg","co2_evite_avion_kg","equiv_arbres"]).to_sql(
        "fact_trajet_carbone", engine, if_exists="replace", index=False, chunksize=5000, method="multi")
    print(f"fact_trajet_carbone : {len(lignes):,} trajets")
    print("=== Gold 4 termine ===")


with DAG(
    default_args={"retries": 0, "retry_delay": pendulum.duration(minutes=1)},
    dag_id="gold_4_score_carbone",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sncf", "gold", "score", "carbone"],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin   = EmptyOperator(task_id="fin")
    transform = PythonOperator(
        task_id="calculer_score_carbone",
        python_callable=run_gold_4,
        execution_timeout=pendulum.duration(minutes=20),
    )
    debut >> transform >> fin
