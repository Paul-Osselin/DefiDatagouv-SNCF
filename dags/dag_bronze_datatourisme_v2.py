from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import json
import io
import csv
import psycopg2

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR   = "/opt/airflow/data/datatourisme_2026-06-19"
TABLE      = "bronze_datatourisme_v2"
BATCH_SIZE = 5000

DB_DSN = "host=100.127.4.50 port=49800 dbname=projetm1 user=adminm1data password=5T5^Aa25s^3#fN7*"

# All lowercase — no quoting issues with PostgreSQL
_COLUMNS = [
    "id", "identifier", "type", "comment_fr", "label_fr",
    "contact_email", "contact_telephone",
    "address_locality", "address_postalcode", "address_streetaddress",
    "department_fr", "region_fr",
    "latitude", "longitude",
    "lastupdate", "lastupdatedatatourisme",
    "source_file",
]

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 6),
    "catchup": False,
}

dag_bronze_datatourisme_v2 = DAG(
    "dag_bronze_datatourisme_v2",
    schedule=None,
    default_args=default_args,
    description="Flatten Datatourisme JSON and bulk-load into bronze_datatourisme_v2 via COPY.",
)

# ---------------------------------------------------------------------------
# Task 1 — Create bronze table
# ---------------------------------------------------------------------------

def create_bronze_table(**kwargs):
    conn = psycopg2.connect(DB_DSN)
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
            cur.execute(f"""
                CREATE TABLE {TABLE} (
                    _ingested_at          TIMESTAMP DEFAULT NOW(),
                    id                    TEXT,
                    identifier            TEXT,
                    type                  TEXT,
                    comment_fr            TEXT,
                    label_fr              TEXT,
                    contact_email         TEXT,
                    contact_telephone     TEXT,
                    address_locality      TEXT,
                    address_postalcode    TEXT,
                    address_streetaddress TEXT,
                    department_fr         TEXT,
                    region_fr             TEXT,
                    latitude              TEXT,
                    longitude             TEXT,
                    lastupdate            TEXT,
                    lastupdatedatatourisme TEXT,
                    source_file           TEXT
                );
            """)
    conn.close()
    print(f"✓ Table {TABLE} created")


# ---------------------------------------------------------------------------
# Flatten helper
# ---------------------------------------------------------------------------

def _flatten(json_obj: dict, source_file: str) -> dict:
    flat = {}

    flat["id"]         = json_obj.get("@id")
    flat["identifier"] = json_obj.get("dc:identifier")
    flat["type"]       = "; ".join(json_obj.get("@type", []))

    comments  = json_obj.get("rdfs:comment", {})
    text_list = comments.get("fr") if isinstance(comments, dict) else None
    flat["comment_fr"] = text_list[0] if text_list else None

    labels    = json_obj.get("rdfs:label", {})
    text_list = labels.get("fr") if isinstance(labels, dict) else None
    flat["label_fr"] = text_list[0] if text_list else None

    contacts = json_obj.get("hasContact", [])
    if contacts:
        contact = contacts[0]
        flat["contact_email"]     = "; ".join(contact.get("schema:email", []))
        flat["contact_telephone"] = "; ".join(contact.get("schema:telephone", []))
    else:
        flat["contact_email"]     = None
        flat["contact_telephone"] = None

    locations = json_obj.get("isLocatedAt", [])
    if locations:
        location = locations[0]

        if "schema:address" in location:
            address = location["schema:address"][0]
            flat["address_locality"]      = address.get("schema:addressLocality")
            flat["address_postalcode"]    = address.get("schema:postalCode")
            street = address.get("schema:streetAddress", [])
            flat["address_streetaddress"] = "; ".join(street) if street else None

            city   = address.get("hasAddressCity", {})
            dept   = city.get("isPartOfDepartment", {})
            flat["department_fr"] = dept.get("rdfs:label", {}).get("fr", [None])[0]
            region = dept.get("isPartOfRegion", {})
            flat["region_fr"]     = region.get("rdfs:label", {}).get("fr", [None])[0]
        else:
            flat["address_locality"]      = None
            flat["address_postalcode"]    = None
            flat["address_streetaddress"] = None
            flat["department_fr"]         = None
            flat["region_fr"]             = None

        if "schema:geo" in location:
            geo = location["schema:geo"]
            flat["latitude"]  = str(geo.get("schema:latitude",  "") or "") or None
            flat["longitude"] = str(geo.get("schema:longitude", "") or "") or None
        else:
            flat["latitude"]  = None
            flat["longitude"] = None
    else:
        flat["address_locality"]      = None
        flat["address_postalcode"]    = None
        flat["address_streetaddress"] = None
        flat["department_fr"]         = None
        flat["region_fr"]             = None
        flat["latitude"]              = None
        flat["longitude"]             = None

    flat["lastupdate"]             = json_obj.get("lastUpdate")
    flat["lastupdatedatatourisme"] = json_obj.get("lastUpdateDatatourisme")
    flat["source_file"]            = str(source_file)

    return flat


# ---------------------------------------------------------------------------
# COPY helper
# ---------------------------------------------------------------------------

def _copy_batch(cur, batch: list):
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for row in batch:
        writer.writerow([
            row.get(col) if row.get(col) is not None else r"\N"
            for col in _COLUMNS
        ])
    buf.seek(0)
    col_list = ", ".join(_COLUMNS)
    cur.copy_expert(
        f"COPY {TABLE} ({col_list}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        buf,
    )


# ---------------------------------------------------------------------------
# Task 2 — Flatten + bulk COPY
# ---------------------------------------------------------------------------

def flatten_and_load(**kwargs):
    print("Scanning directory structure...")
    json_files = list(Path(DATA_DIR).rglob("*.json"))
    total      = len(json_files)
    print(f"Found {total} JSON files")

    if total == 0:
        raise ValueError(f"No .json files found under {DATA_DIR}")

    conn = psycopg2.connect(DB_DSN)
    cur  = conn.cursor()

    batch           = []
    processed_count = 0
    error_count     = 0
    total_inserted  = 0

    for idx, file_path in enumerate(json_files, 1):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                json_obj = json.load(f)

            if not isinstance(json_obj, dict):
                print(f"⚠ Skipping {file_path.name}: root is {type(json_obj).__name__}, expected dict")
                error_count += 1
                continue

            flat = _flatten(json_obj, file_path)
            batch.append(flat)
            processed_count += 1

        except json.JSONDecodeError as e:
            print(f"⚠ JSON error in {file_path.name}: {e}")
            error_count += 1
        except Exception as e:
            print(f"⚠ Error in {file_path.name}: {e}")
            error_count += 1

        if len(batch) >= BATCH_SIZE:
            _copy_batch(cur, batch)
            conn.commit()
            total_inserted += len(batch)
            batch = []
            print(f"  {idx}/{total} files | {total_inserted:,} rows inserted")

    # Flush remainder
    if batch:
        _copy_batch(cur, batch)
        conn.commit()
        total_inserted += len(batch)

    cur.close()
    conn.close()

    print(f"\n✓ Load complete!")
    print(f"  Total rows inserted : {total_inserted:,}")
    print(f"  Successfully parsed : {processed_count}")
    print(f"  Skipped / errors    : {error_count}")

    if total_inserted == 0:
        raise ValueError("0 rows inserted — check JSON structure and DATA_DIR.")


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

task_create_table = PythonOperator(
    task_id="task_create_bronze_table",
    python_callable=create_bronze_table,
    dag=dag_bronze_datatourisme_v2,
)

task_flatten_load = PythonOperator(
    task_id="task_flatten_and_load",
    python_callable=flatten_and_load,
    dag=dag_bronze_datatourisme_v2,
)

task_create_table >> task_flatten_load
