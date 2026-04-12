"""
hdb_loader.py

Downloads the full HDB resale flat prices dataset from data.gov.sg,
uploads the raw CSV to GCS, and loads it into BigQuery raw_hdb.hdb_resale.

Usage:
    uv run python src/ingestion/hdb_loader.py --backfill
    uv run python src/ingestion/hdb_loader.py --dry-run
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
from google.cloud.bigquery import LoadJobConfig, SchemaField, TimePartitioning

import config


BQ_TABLE = "hdb_resale"

BQ_SCHEMA = [
    SchemaField("month", "STRING"),
    SchemaField("town", "STRING"),
    SchemaField("flat_type", "STRING"),
    SchemaField("block", "STRING"),
    SchemaField("street_name", "STRING"),
    SchemaField("storey_range", "STRING"),
    SchemaField("floor_area_sqm", "STRING"),
    SchemaField("flat_model", "STRING"),
    SchemaField("lease_commence_date", "STRING"),
    SchemaField("remaining_lease", "STRING"),
    SchemaField("resale_price", "STRING"),
    SchemaField("ingested_at", "TIMESTAMP"),
]

EXPECTED_COLUMNS = {
    "month", "town", "flat_type", "block", "street_name",
    "storey_range", "floor_area_sqm", "flat_model",
    "lease_commence_date", "remaining_lease", "resale_price",
}


def fetch_hdb_data() -> pd.DataFrame:
    """Fetch full HDB resale dataset from data.gov.sg API (bulk download)."""
    headers = {"Content-Type": "application/json"}
    if config.DATA_GOV_SG_API_KEY:
        headers["x-api-key"] = config.DATA_GOV_SG_API_KEY

    s = requests.Session()

    # Step 1: Initiate the download job
    print("Initiating download from data.gov.sg API...")
    resp = s.get(config.HDB_API_INITIATE_URL, headers=headers, json={}, timeout=30)
    resp.raise_for_status()
    print(resp.json().get("data", {}).get("message", ""))

    # Step 2: Poll until download URL is ready
    print("Polling for download readiness...")
    for attempt in range(30):
        time.sleep(3)
        poll_resp = s.get(config.HDB_API_POLL_URL, headers=headers, json={}, timeout=30)
        poll_resp.raise_for_status()
        poll_data = poll_resp.json().get("data", {})
        download_url = poll_data.get("url")
        if download_url:
            print(f"  Ready after {attempt + 1} poll(s).")
            break
        print(f"  Still processing... ({attempt + 1}/30)")
    else:
        raise RuntimeError("Download did not become ready after 90 seconds.")

    # Step 3: Download the CSV
    print(f"Downloading CSV from {download_url[:60]}...")
    csv_resp = requests.get(download_url, timeout=120)
    csv_resp.raise_for_status()

    df = pd.read_csv(io.StringIO(csv_resp.text))
    print(f"Fetched {len(df):,} records.")

    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Downloaded CSV missing expected columns: {missing}")

    return df


def add_ingested_at(df: pd.DataFrame) -> pd.DataFrame:
    """Add ingestion timestamp — no other transformations."""
    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc)
    # Cast all source columns to string to preserve raw values
    source_cols = [f.name for f in BQ_SCHEMA if f.name != "ingested_at"]
    df[source_cols] = df[source_cols].astype(str)
    return df[source_cols + ["ingested_at"]]


def upload_to_gcs(df: pd.DataFrame, gcs_path: str) -> str:
    """Upload DataFrame as CSV to GCS. Returns the gs:// URI."""
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


def load_to_bigquery(df: pd.DataFrame, write_disposition: str) -> None:
    """Load DataFrame into BigQuery raw_hdb.hdb_resale."""
    credentials = config.get_gcp_credentials()
    client = bigquery.Client(project=config.GCP_PROJECT_ID, credentials=credentials)

    table_ref = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_RAW}.{BQ_TABLE}"

    job_config = LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=write_disposition,
        time_partitioning=TimePartitioning(field="ingested_at"),
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
    except Exception as e:
        raise RuntimeError(f"BigQuery load failed for {table_ref}: {e}") from e

    print(f"Loaded {job.output_rows:,} rows into {table_ref} ({write_disposition}).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load HDB resale data to GCS + BigQuery")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Truncate and reload the full table (default: append)",
    )
    parser.add_argument(
        "--gcs-path",
        default=None,
        help="Override GCS object key (default: hdb_resale/hdb_resale_YYYYMMDD.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and clean data only — skip GCS and BigQuery writes",
    )
    args = parser.parse_args()

    config.validate_config()

    print("Fetching HDB resale data from data.gov.sg...")
    df = fetch_hdb_data()

    df = add_ingested_at(df)
    print(f"Raw dataset: {len(df):,} rows, {len(df.columns)} columns.")

    if args.dry_run:
        print("Dry run — skipping GCS and BigQuery writes.")
        print(df.dtypes)
        print(df.head())
        return

    gcs_path = args.gcs_path or (
        f"hdb_resale/hdb_resale_{datetime.now().strftime('%Y%m%d')}.csv"
    )
    gcs_uri = upload_to_gcs(df, gcs_path)

    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if args.backfill
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    load_to_bigquery(df, write_disposition)

    print("\nSummary:")
    print(f"  Rows loaded : {len(df):,}")
    print(f"  GCS URI     : {gcs_uri}")
    print(f"  BQ table    : {config.GCP_PROJECT_ID}.{config.BQ_DATASET_RAW}.{BQ_TABLE}")
    print(f"  Disposition : {write_disposition}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
