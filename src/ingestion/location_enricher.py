"""
location_enricher.py

Reads raw HDB data from GCS, geocodes unique street names via the OneMap API,
writes the enriched CSV back to GCS, and loads the geocoded lookup table into
BigQuery raw_hdb.hdb_locations.

Usage:
    uv run python src/ingestion/location_enricher.py \
        --input gs://de-zoomcamp-hdb-resale-hdb-raw/hdb_resale/hdb_resale_20260320.csv \
        --output gs://de-zoomcamp-hdb-resale-hdb-raw/hdb_enriched/hdb_enriched_20260320.csv

    uv run python src/ingestion/location_enricher.py \
        --input gs://de-zoomcamp-hdb-resale-hdb-raw/hdb_resale/hdb_resale_20260320.csv \
        --dry-run
"""

import argparse
import io
import sys
import time
import traceback
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig, SchemaField

import config


BQ_TABLE = "hdb_locations"

BQ_SCHEMA = [
    SchemaField("street_name", "STRING"),
    SchemaField("latitude", "FLOAT64"),
    SchemaField("longitude", "FLOAT64"),
    SchemaField("geocode_status", "STRING"),
    SchemaField("geocoded_at", "TIMESTAMP"),
]


def read_hdb_from_gcs(gcs_uri: str) -> pd.DataFrame:
    """Download a CSV from GCS and return as a DataFrame."""
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")

    # Parse gs://bucket/path
    without_scheme = gcs_uri[len("gs://"):]
    bucket_name, blob_path = without_scheme.split("/", 1)

    credentials = config.get_gcp_credentials()
    client = storage.Client(project=config.GCP_PROJECT_ID, credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    content = blob.download_as_bytes()
    print(f"Downloaded {len(content) / 1024 / 1024:.1f} MB from {gcs_uri}")
    return pd.read_csv(io.BytesIO(content))


def get_onemap_token() -> str:
    """Fetch a fresh OneMap bearer token."""
    resp = requests.post(
        config.ONEMAP_AUTH_URL,
        json={"email": config.ONEMAP_EMAIL, "password": config.ONEMAP_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("OneMap auth response missing access_token")
    print("OneMap token obtained.")
    return token


def geocode_street(street_name: str, session: requests.Session) -> dict:
    """Call OneMap API for a single street name. Returns a result dict."""
    params = {
        "searchVal": street_name,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": 1,
    }

    for attempt in range(3):
        try:
            resp = session.get(config.ONEMAP_API_URL, params=params, timeout=10)
            if resp.status_code == 429:
                time.sleep(5)
                continue
            resp.raise_for_status()
            break
        except requests.exceptions.ConnectionError:
            if attempt == 2:
                return {
                    "street_name": street_name,
                    "latitude": None,
                    "longitude": None,
                    "geocode_status": "ERROR",
                }
            time.sleep(5)

    data = resp.json()
    results = data.get("results", [])

    if results:
        first = results[0]
        return {
            "street_name": street_name,
            "latitude": float(first["LATITUDE"]),
            "longitude": float(first["LONGITUDE"]),
            "geocode_status": "OK",
        }

    return {
        "street_name": street_name,
        "latitude": None,
        "longitude": None,
        "geocode_status": "NOT_FOUND",
    }


def geocode_all_streets(street_names: list, dry_run: bool = False) -> pd.DataFrame:
    """Geocode a list of unique street names with rate limiting."""
    unique = list(dict.fromkeys(street_names))  # deduplicate, preserve order

    if dry_run:
        unique = unique[:10]
        print(f"Dry run — geocoding first {len(unique)} streets only.")

    print(f"Geocoding {len(unique)} unique street names...")
    results = []

    with requests.Session() as session:
        for i, street in enumerate(unique, 1):
            result = geocode_street(street, session)
            results.append(result)
            time.sleep(config.ONEMAP_RATE_LIMIT_DELAY)

            if i % 50 == 0:
                print(f"  Geocoded {i} / {len(unique)} streets...")

    df = pd.DataFrame(results)
    df["geocoded_at"] = datetime.now(timezone.utc)

    ok = (df["geocode_status"] == "OK").sum()
    not_found = (df["geocode_status"] == "NOT_FOUND").sum()
    error = (df["geocode_status"] == "ERROR").sum()
    print(f"Geocoding complete: {ok} OK, {not_found} NOT_FOUND, {error} ERROR.")

    return df


def merge_coordinates(hdb_df: pd.DataFrame, geo_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join geocoded coordinates onto the full HDB dataset."""
    enriched = pd.merge(hdb_df, geo_df[["street_name", "latitude", "longitude"]], on="street_name", how="left")
    null_count = enriched["latitude"].isna().sum()
    if null_count:
        print(f"Warning: {null_count:,} rows have no coordinates (street not geocoded).")
    return enriched


def upload_enriched_to_gcs(df: pd.DataFrame, gcs_path: str) -> str:
    """Upload enriched DataFrame as CSV to GCS. Returns the gs:// URI."""
    credentials = config.get_gcp_credentials()
    client = storage.Client(project=config.GCP_PROJECT_ID, credentials=credentials)
    bucket = client.bucket(config.GCS_BUCKET_RAW)
    blob = bucket.blob(gcs_path)

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    blob.upload_from_string(csv_bytes, content_type="text/csv")

    uri = f"gs://{config.GCS_BUCKET_RAW}/{gcs_path}"
    print(f"Uploaded {len(csv_bytes) / 1024 / 1024:.1f} MB to {uri}")
    return uri


def load_locations_to_bigquery(geo_df: pd.DataFrame) -> None:
    """Write geocoded lookup table to BigQuery raw_hdb.hdb_locations."""
    credentials = config.get_gcp_credentials()
    client = bigquery.Client(project=config.GCP_PROJECT_ID, credentials=credentials)

    table_ref = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_RAW}.{BQ_TABLE}"

    job_config = LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        job = client.load_table_from_dataframe(geo_df, table_ref, job_config=job_config)
        job.result()
    except Exception as e:
        raise RuntimeError(f"BigQuery load failed for {table_ref}: {e}") from e

    print(f"Loaded {job.output_rows:,} rows into {table_ref}.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Geocode HDB street names and enrich dataset"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="GCS URI of the raw HDB CSV (output of hdb_loader.py)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="GCS URI for the enriched CSV (default: hdb_enriched/hdb_enriched_YYYYMMDD.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Geocode first 10 streets only — skip GCS and BigQuery writes",
    )
    args = parser.parse_args()

    config.validate_config()

    print(f"Reading HDB data from {args.input}...")
    hdb_df = read_hdb_from_gcs(args.input)
    print(f"Loaded {len(hdb_df):,} rows.")

    street_names = hdb_df["street_name"].dropna().tolist()
    geo_df = geocode_all_streets(street_names, dry_run=args.dry_run)

    if args.dry_run:
        print("Dry run — skipping GCS and BigQuery writes.")
        print(geo_df)
        return

    enriched_df = merge_coordinates(hdb_df, geo_df)

    output_path = args.output
    if output_path and output_path.startswith("gs://"):
        without_scheme = output_path[len("gs://"):]
        _, blob_path = without_scheme.split("/", 1)
        output_path = blob_path
    else:
        output_path = (
            f"hdb_enriched/hdb_enriched_{datetime.now().strftime('%Y%m%d')}.csv"
        )

    gcs_uri = upload_enriched_to_gcs(enriched_df, output_path)
    load_locations_to_bigquery(geo_df)

    print("\nSummary:")
    print(f"  Streets geocoded : {len(geo_df):,}")
    print(f"  Enriched rows    : {len(enriched_df):,}")
    print(f"  GCS output       : {gcs_uri}")
    print(f"  BQ table         : {config.GCP_PROJECT_ID}.{config.BQ_DATASET_RAW}.{BQ_TABLE}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
