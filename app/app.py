"""
Tourisme en Train — Flask backend (API only)

Run with:
    pip install flask sqlalchemy psycopg2-binary
    python app.py
Then open http://localhost:5000

This file ONLY contains the API and database logic.
The page itself lives in templates/index.html
"""

import traceback
from flask import Flask, request, jsonify, render_template
from sqlalchemy import create_engine, text

app = Flask(__name__)

DB_CONN = "postgresql://adminm1data:5T5^Aa25s^3#fN7*@100.127.4.50:49800/projetm1"

# Road distance is approximated from the great-circle distance with a detour
# factor (roads are not straight). ADEME / SNCF use a comparable approximation.
ROAD_DETOUR = 1.3

# Reference modes always shown in the carbon comparison, besides the train's own.
ALTERNATIVE_MODES = ["voiture", "autocar", "avion"]


# ---------------------------------------------------------------------------
# Carbon helpers
# ---------------------------------------------------------------------------

def grams_co2(factor, rail_km, gc_km):
    """gCO2e for one passenger, given a ref_emission_factors row and the two
    distances. The factor's distance_basis decides which distance applies."""
    basis = factor["distance_basis"]
    if basis == "rail":
        dist = rail_km
    elif basis == "road":
        dist = gc_km * ROAD_DETOUR
    else:  # great_circle
        dist = gc_km
    return dist * factor["gco2e_per_km"]


def build_comparison(train_mode, rail_km, gc_km, factors):
    """Returns {mode,label,grams,kg,is_trip} for the bar chart: the selected
    service plus car / coach / plane, de-duplicated."""
    comparison, seen = [], set()
    for mode in [train_mode] + ALTERNATIVE_MODES:
        if mode not in factors or mode in seen:
            continue
        seen.add(mode)
        g = grams_co2(factors[mode], rail_km, gc_km)
        comparison.append({
            "mode":    mode,
            "label":   factors[mode]["mode_label"],
            "grams":   round(g),
            "kg":      round(g / 1000.0, 2),
            "is_trip": (mode == train_mode),
        })
    return comparison


# ---------------------------------------------------------------------------
# Page — serves the HTML
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# API — autocomplete station names
# ---------------------------------------------------------------------------

@app.route("/api/stations")
def stations():
    q = request.args.get("q", "")
    if len(q) < 2:
        return jsonify([])

    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT stop_name
            FROM silver_gtfs_stops
            WHERE stop_name ILIKE :q
              AND location_type = 1
            ORDER BY stop_name
            LIMIT 10
        """), {"q": f"%{q}%"}).fetchall()
    engine.dispose()
    return jsonify([r[0] for r in rows])


# ---------------------------------------------------------------------------
# API — search trips + POIs around arrival station
# ---------------------------------------------------------------------------

# Optimised query.
#   cal   : services running on the requested date  (filters early)
#   dep   : stop_times at the departure station, after the requested time
#   arr   : stop_times at the arrival station
#   cand  : trips serving dep -> arr (arr after dep) AND running on the date,
#           enriched with train_mode / label from gold  (small result set)
#   geo   : line geometry + rail distance, aggregated ONLY for candidate trips
# The heavy ST_MakeLine work therefore runs on a handful of trips, not the
# whole timetable.
SEARCH_SQL = text("""
    WITH cal AS (
        SELECT service_id, date
        FROM silver_gtfs_calendar_dates
        WHERE date = :date AND exception_type = 1
    ),
    dep AS (
        SELECT st.trip_id, st.stop_sequence AS seq,
               st.departure_time AS dtime, s.geom
        FROM silver_gtfs_stop_times st
        JOIN silver_gtfs_stops s ON s.stop_id = st.stop_id
        WHERE s.stop_name ILIKE :dep
          AND st.departure_time >= :time
    ),
    arr AS (
        SELECT st.trip_id, st.stop_sequence AS seq,
               st.arrival_time AS atime, s.geom
        FROM silver_gtfs_stop_times st
        JOIN silver_gtfs_stops s ON s.stop_id = st.stop_id
        WHERE s.stop_name ILIKE :arr
    ),
    cand AS (
        SELECT d.trip_id,
               d.seq            AS dep_seq,
               a.seq            AS arr_seq,
               d.dtime,
               a.atime,
               ST_Distance(d.geom, a.geom) AS gc_distance_m,
               t.trip_headsign  AS train_number,
               t.train_mode,
               t.mode_label,
               t.route_id,
               cal.date         AS service_date
        FROM dep d
        JOIN arr a              ON a.trip_id = d.trip_id AND a.seq > d.seq
        JOIN gold_gtfs_trips t  ON t.trip_id = d.trip_id
        JOIN cal                ON cal.service_id = t.service_id
    ),
    geo AS (
        SELECT c.trip_id, c.dep_seq, c.arr_seq,
               ST_AsGeoJSON(
                   ST_MakeLine(
                       ST_MakePoint(s.stop_lon, s.stop_lat)
                       ORDER BY st.stop_sequence
                   )
               )                                                       AS trip_geojson,
               ST_Length(
                   ST_MakeLine(s.geom::geometry ORDER BY st.stop_sequence)
                   ::geography
               )                                                       AS od_distance_m
        FROM cand c
        JOIN silver_gtfs_stop_times st
             ON  st.trip_id = c.trip_id
             AND st.stop_sequence BETWEEN c.dep_seq AND c.arr_seq
        JOIN silver_gtfs_stops s ON s.stop_id = st.stop_id
        GROUP BY c.trip_id, c.dep_seq, c.arr_seq
    )
    SELECT
        c.trip_id,
        c.train_number,
        c.train_mode,
        c.mode_label,
        r.route_short_name AS line,
        r.route_long_name  AS line_name,
        c.service_date     AS departure_date,
        c.dtime            AS departure_time,
        c.atime            AS arrival_time,
        g.trip_geojson,
        g.od_distance_m,
        c.gc_distance_m
    FROM cand c
    JOIN geo g
         ON g.trip_id = c.trip_id
        AND g.dep_seq = c.dep_seq
        AND g.arr_seq = c.arr_seq
    JOIN silver_gtfs_routes r ON r.route_id = c.route_id
    ORDER BY c.dtime ASC
    LIMIT 50
""")


@app.route("/api/search")
def search():
    departure = request.args.get("departure", "")
    arrival   = request.args.get("arrival", "")
    date      = request.args.get("date", "")        # YYYY-MM-DD
    time      = request.args.get("time", "00:00")   # HH:MM
    radius    = request.args.get("radius", 1000)

    if not departure or not arrival or not date:
        return jsonify({"error": "Missing parameters"}), 400

    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:

            # --- Emission factors (small reference table) ---
            factors = {
                r.mode: {
                    "gco2e_per_km":   float(r.gco2e_per_km),
                    "distance_basis": r.distance_basis,
                    "mode_label":     r.mode_label,
                }
                for r in conn.execute(text("""
                    SELECT mode, gco2e_per_km, distance_basis, mode_label
                    FROM ref_emission_factors
                """)).fetchall()
            }

            # --- Trips ---
            trips_rows = conn.execute(SEARCH_SQL, {
                "dep":  f"%{departure}%",
                "arr":  f"%{arrival}%",
                "date": date,
                "time": f"{time}:00",
            }).fetchall()

            # --- Arrival station location ---
            station_row = conn.execute(text("""
                SELECT stop_name,
                       ST_X(geom::geometry) AS lon,
                       ST_Y(geom::geometry) AS lat
                FROM silver_gtfs_stops
                WHERE stop_name ILIKE :arr
                  AND location_type = 1
                LIMIT 1
            """), {"arr": f"%{arrival}%"}).fetchone()

            # --- POIs within radius of arrival station (optional feature) ---
            # Isolated in its own transaction so a missing/empty POI table never
            # breaks the trips + carbon response.
            poi_rows = []
            poi_warning = None
            if station_row:
                try:
                    with engine.connect() as poi_conn:
                        poi_rows = poi_conn.execute(text("""
                            SELECT
                                poi.nom_poi,
                                poi.type_principal,
                                poi.commune,
                                ROUND(ST_Distance(poi.geom, s.geom)::numeric, 0) AS distance_metres,
                                ST_AsGeoJSON(poi.geom) AS geojson
                            FROM silver_datatourisme_poi poi
                            CROSS JOIN (
                                SELECT geom FROM silver_gtfs_stops
                                WHERE stop_name ILIKE :arr AND location_type = 1
                                LIMIT 1
                            ) s
                            WHERE ST_DWithin(poi.geom, s.geom, :radius)
                            ORDER BY distance_metres ASC
                            LIMIT 100
                        """), {"arr": f"%{arrival}%", "radius": int(radius)}).fetchall()
                except Exception as pe:
                    poi_warning = f"POIs indisponibles ({pe.__class__.__name__})"
                    print("POI query skipped:", pe)

        engine.dispose()

        # --- Build JSON response ---
        trips = []
        for r in trips_rows:
            rail_km = (r.od_distance_m or 0) / 1000.0
            gc_km   = (r.gc_distance_m or 0) / 1000.0
            comparison = build_comparison(r.train_mode, rail_km, gc_km, factors)
            own = next((c for c in comparison if c["is_trip"]), None)

            trips.append({
                "trip_id":        r.trip_id,
                "train_number":   r.train_number,
                "train_mode":     r.train_mode,
                "mode_label":     r.mode_label,
                "line":           r.line,
                "line_name":      r.line_name,
                "departure_date": str(r.departure_date),
                "departure_time": r.departure_time,
                "arrival_time":   r.arrival_time,
                "distance_km":    round(rail_km, 1),
                "co2_kg":         own["kg"]    if own else None,
                "co2_grams":      own["grams"] if own else None,
                "emissions":      comparison,
                "geojson":        r.trip_geojson,
            })

        pois = [{
            "nom":      r.nom_poi,
            "type":     r.type_principal,
            "commune":  r.commune,
            "distance": int(r.distance_metres),
            "geojson":  r.geojson,
        } for r in poi_rows]

        station = None
        if station_row:
            station = {
                "name": station_row.stop_name,
                "lon":  station_row.lon,
                "lat":  station_row.lat,
            }

        return jsonify({
            "trips": trips,
            "pois": pois,
            "arrival_station": station,
            "warning": poi_warning,
        })

    except Exception as e:
        # Surface the real error as JSON instead of an HTML 500 page.
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500




# ---------------------------------------------------------------------------
# API — autocomplete POI names
# ---------------------------------------------------------------------------

@app.route("/api/pois")
def pois_autocomplete():
    q = request.args.get("q", "")
    if len(q) < 2:
        return jsonify([])
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT nom_poi
                FROM silver_datatourisme_poi
                WHERE nom_poi ILIKE :q
                ORDER BY nom_poi
                LIMIT 10
            """), {"q": f"%{q}%"}).fetchall()
        engine.dispose()
        return jsonify([r[0] for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# API — search trips by POI as destination
#   1. Resolve the POI name to its nearest StopArea
#   2. Run the same SEARCH_SQL against that station
# ---------------------------------------------------------------------------

@app.route("/api/search_poi")
def search_poi():
    departure = request.args.get("departure", "")
    poi_name  = request.args.get("poi", "")
    date      = request.args.get("date", "")
    time      = request.args.get("time", "00:00")
    radius    = request.args.get("radius", 1000)

    if not departure or not poi_name or not date:
        return jsonify({"error": "Missing parameters"}), 400

    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:

            # Step 1 — find nearest StopArea to the POI
            resolved = conn.execute(text("""
                SELECT
                    s.stop_name,
                    ST_X(s.geom::geometry)      AS lon,
                    ST_Y(s.geom::geometry)      AS lat,
                    ST_Distance(s.geom, p.geom) AS distance_m
                FROM silver_gtfs_stops s
                CROSS JOIN (
                    SELECT geom FROM silver_datatourisme_poi
                    WHERE nom_poi ILIKE :poi
                    LIMIT 1
                ) p
                WHERE s.location_type = 1
                ORDER BY distance_m ASC
                LIMIT 1
            """), {"poi": f"%{poi_name}%"}).fetchone()

            if not resolved:
                return jsonify({"error": "POI introuvable ou aucune gare a proximite."}), 404

            arrival = resolved.stop_name

            # Step 2 — emission factors
            factors = {
                r.mode: {
                    "gco2e_per_km":   float(r.gco2e_per_km),
                    "distance_basis": r.distance_basis,
                    "mode_label":     r.mode_label,
                }
                for r in conn.execute(text("""
                    SELECT mode, gco2e_per_km, distance_basis, mode_label
                    FROM ref_emission_factors
                """)).fetchall()
            }

            # Step 3 — trip search (reuse same SQL)
            trips_rows = conn.execute(SEARCH_SQL, {
                "dep":  f"%{departure}%",
                "arr":  f"%{arrival}%",
                "date": date,
                "time": f"{time}:00",
            }).fetchall()

            # Step 4 — arrival station coords for the map
            station_row = conn.execute(text("""
                SELECT stop_name,
                       ST_X(geom::geometry) AS lon,
                       ST_Y(geom::geometry) AS lat
                FROM silver_gtfs_stops
                WHERE stop_name ILIKE :arr AND location_type = 1
                LIMIT 1
            """), {"arr": f"%{arrival}%"}).fetchone()

            # Step 5 — POIs around resolved station (non-fatal)
            poi_rows = []
            try:
                with engine.connect() as poi_conn:
                    poi_rows = poi_conn.execute(text("""
                        SELECT
                            poi.nom_poi, poi.type_principal, poi.commune,
                            ROUND(ST_Distance(poi.geom, s.geom)::numeric, 0) AS distance_metres,
                            ST_AsGeoJSON(poi.geom) AS geojson
                        FROM silver_datatourisme_poi poi
                        CROSS JOIN (
                            SELECT geom FROM silver_gtfs_stops
                            WHERE stop_name ILIKE :arr AND location_type = 1
                            LIMIT 1
                        ) s
                        WHERE ST_DWithin(poi.geom, s.geom, :radius)
                        ORDER BY distance_metres ASC
                        LIMIT 100
                    """), {"arr": f"%{arrival}%", "radius": int(radius)}).fetchall()
            except Exception as pe:
                print("POI query skipped:", pe)

        engine.dispose()

        trips = []
        for r in trips_rows:
            rail_km    = (r.od_distance_m or 0) / 1000.0
            gc_km      = (r.gc_distance_m or 0) / 1000.0
            comparison = build_comparison(r.train_mode, rail_km, gc_km, factors)
            own        = next((c for c in comparison if c["is_trip"]), None)
            trips.append({
                "trip_id":        r.trip_id,
                "train_number":   r.train_number,
                "train_mode":     r.train_mode,
                "mode_label":     r.mode_label,
                "line":           r.line,
                "line_name":      r.line_name,
                "departure_date": str(r.departure_date),
                "departure_time": r.departure_time,
                "arrival_time":   r.arrival_time,
                "distance_km":    round(rail_km, 1),
                "co2_kg":         own["kg"]    if own else None,
                "co2_grams":      own["grams"] if own else None,
                "emissions":      comparison,
                "geojson":        r.trip_geojson,
            })

        pois = [{
            "nom": r.nom_poi, "type": r.type_principal,
            "commune": r.commune, "distance": int(r.distance_metres),
            "geojson": r.geojson,
        } for r in poi_rows]

        station = None
        if station_row:
            station = {"name": station_row.stop_name,
                       "lon": station_row.lon, "lat": station_row.lat}

        return jsonify({
            "trips": trips, "pois": pois, "arrival_station": station,
            "resolved_station": {
                "name": resolved.stop_name, "lon": resolved.lon,
                "lat": resolved.lat, "distance_m": round(resolved.distance_m),
            },
            "warning": None,
        })

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ---------------------------------------------------------------------------
# API — statistics dashboard (ports the Streamlit KPI pages)
# Reads the Gold layer (fact_score_gare, fact_trajet_carbone, fact_regularite,
# silver_poi_categorie, dim_categorie_poi). Returns everything the dashboard
# needs in a single payload so the front-end just draws the charts.
# ---------------------------------------------------------------------------

def _rows(conn, sql, params=None):
    """Run a query, return list of dicts with float-coerced numbers."""
    res = conn.execute(text(sql), params or {})
    cols = res.keys()
    out = []
    for r in res.fetchall():
        d = {}
        for k, v in zip(cols, r):
            try:
                d[k] = float(v) if isinstance(v, (int, float)) or hasattr(v, "__float__") else v
            except (TypeError, ValueError):
                d[k] = v
        out.append(d)
    return out


@app.route("/api/stats/dashboard")
def stats_dashboard():
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:

            # ── Touristique ──────────────────────────────────────────
            top_diversite = _rows(conn, """
                SELECT nom_gare, nb_familles
                FROM fact_score_gare
                WHERE latitude IS NOT NULL
                ORDER BY nb_familles DESC NULLS LAST LIMIT 15
            """)
            top_poi = _rows(conn, """
                SELECT nom_gare, nb_poi
                FROM fact_score_gare
                WHERE latitude IS NOT NULL
                ORDER BY nb_poi DESC NULLS LAST LIMIT 15
            """)
            poi_familles = _rows(conn, """
                SELECT dc.famille, COUNT(DISTINCT pc.poi_id) AS nb_poi
                FROM silver_poi_categorie pc
                JOIN dim_categorie_poi dc ON dc.categorie = pc.categorie
                WHERE dc.famille <> 'Autre'
                GROUP BY dc.famille ORDER BY nb_poi DESC
            """)

            # ── Fréquentation & régularité ──────────────────────────
            top_freq = _rows(conn, """
                SELECT nom_gare, voyageurs
                FROM fact_score_gare
                WHERE latitude IS NOT NULL
                ORDER BY voyageurs DESC NULLS LAST LIMIT 15
            """)
            freq_region = _rows(conn, """
                SELECT region_admin AS region, SUM(voyageurs) AS voyageurs
                FROM fact_score_gare
                WHERE region_admin IS NOT NULL
                GROUP BY region_admin ORDER BY voyageurs DESC
            """)
            regularite_region = _rows(conn, """
                SELECT region, AVG(taux_regularite) AS taux
                FROM fact_regularite
                WHERE type_service = 'TER' AND annee >= 2020
                  AND taux_regularite IS NOT NULL
                GROUP BY region ORDER BY taux DESC
            """)

            # ── Accessibilité ───────────────────────────────────────
            top_velo = _rows(conn, """
                SELECT nom_gare, places_velo
                FROM fact_score_gare
                WHERE latitude IS NOT NULL
                ORDER BY places_velo DESC NULLS LAST LIMIT 15
            """)
            top_access = _rows(conn, """
                SELECT nom_gare, score_accessibilite
                FROM fact_score_gare
                WHERE latitude IS NOT NULL
                ORDER BY score_accessibilite DESC NULLS LAST LIMIT 15
            """)
            acc_metrics = _rows(conn, """
                SELECT
                    COUNT(*) FILTER (WHERE total_pmr_cumul > 0) AS nb_pmr,
                    COUNT(*) FILTER (WHERE places_velo > 0)     AS nb_velo,
                    COALESCE(SUM(places_velo), 0)               AS total_velo
                FROM fact_score_gare
            """)[0]

            # ── Carbone ─────────────────────────────────────────────
            carbone_metrics = _rows(conn, """
                SELECT COUNT(*)                      AS nb_trajets,
                       AVG(co2_evite_voiture_kg)     AS co2_moyen,
                       AVG(equiv_arbres)             AS arbres_moyen
                FROM fact_trajet_carbone
            """)[0]
            top_co2 = _rows(conn, """
                SELECT gare_depart || ' \u2192 ' || gare_arrivee AS trajet,
                       co2_evite_voiture_kg
                FROM fact_trajet_carbone
                ORDER BY co2_evite_voiture_kg DESC NULLS LAST LIMIT 15
            """)
            score_metrics = _rows(conn, """
                SELECT COUNT(*) FILTER (WHERE score_global > 70) AS nb_above,
                       COUNT(*)                                  AS total
                FROM fact_score_gare
            """)[0]
            score_values = [r["score_global"] for r in _rows(conn, """
                SELECT score_global FROM fact_score_gare
                WHERE score_global IS NOT NULL
            """)]

        engine.dispose()

        return jsonify({
            "touristique": {
                "top_diversite": top_diversite,
                "top_poi": top_poi,
                "poi_familles": poi_familles,
            },
            "frequentation": {
                "top_freq": top_freq,
                "freq_region": freq_region,
                "regularite_region": regularite_region,
            },
            "accessibilite": {
                "top_velo": top_velo,
                "top_access": top_access,
                "metrics": acc_metrics,
            },
            "carbone": {
                "metrics": carbone_metrics,
                "top_co2": top_co2,
                "score_metrics": score_metrics,
                "score_values": score_values,
            },
        })

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
