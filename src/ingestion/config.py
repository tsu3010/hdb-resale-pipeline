import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from google.oauth2 import service_account

load_dotenv(find_dotenv())

# GCP
GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
GCP_REGION: str = os.getenv("GCP_REGION", "asia-southeast1")
SA_KEY_PATH: str = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH", "")

# BigQuery
BQ_DATASET_RAW: str = os.getenv("BQ_DATASET_RAW", "raw_hdb")

# GCS
GCS_BUCKET_RAW: str = os.getenv("GCS_BUCKET_RAW", "")

# data.gov.sg API
HDB_DATASET_ID: str = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
HDB_API_INITIATE_URL: str = f"https://api-open.data.gov.sg/v1/public/api/datasets/{HDB_DATASET_ID}/initiate-download"
HDB_API_POLL_URL: str = f"https://api-open.data.gov.sg/v1/public/api/datasets/{HDB_DATASET_ID}/poll-download"
DATA_GOV_SG_API_KEY: str = os.getenv("DATA_GOV_SG_API_KEY", "")

# OneMap API
ONEMAP_API_URL: str = os.getenv(
    "ONEMAP_API_URL",
    "https://www.onemap.gov.sg/api/common/elastic/search",
)
ONEMAP_AUTH_URL: str = os.getenv(
    "ONEMAP_AUTH_URL",
    "https://www.onemap.gov.sg/api/auth/post/getToken",
)
ONEMAP_EMAIL: str = os.getenv("ONEMAP_EMAIL", "")
ONEMAP_PASSWORD: str = os.getenv("ONEMAP_PASSWORD", "")
ONEMAP_RATE_LIMIT_DELAY: float = 0.3

_GCP_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/devstorage.read_write",
]


def validate_config() -> None:
    errors = []

    if not GCP_PROJECT_ID:
        errors.append("GCP_PROJECT_ID is not set")
    if not SA_KEY_PATH:
        errors.append("GCP_SERVICE_ACCOUNT_KEY_PATH is not set")
    elif not Path(SA_KEY_PATH).exists():
        errors.append(f"Service account key file not found: {SA_KEY_PATH}")
    if not GCS_BUCKET_RAW:
        errors.append("GCS_BUCKET_RAW is not set")
    if not BQ_DATASET_RAW:
        errors.append("BQ_DATASET_RAW is not set")
    if not DATA_GOV_SG_API_KEY:
        errors.append("DATA_GOV_SG_API_KEY is not set")

    if errors:
        raise EnvironmentError(
            "Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors)
        )


def get_gcp_credentials() -> service_account.Credentials:
    try:
        return service_account.Credentials.from_service_account_file(
            SA_KEY_PATH, scopes=_GCP_SCOPES
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to load service account credentials from {SA_KEY_PATH}: {e}"
        ) from e
