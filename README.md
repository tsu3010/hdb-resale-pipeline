# HDB Resale Flat Prices Analytics Pipeline

![GCP](https://img.shields.io/badge/Cloud-Google_Cloud_Platform-blue)
![Terraform](https://img.shields.io/badge/IaC-Terraform-green)
![GCS](https://img.shields.io/badge/Data_Lake-Google_Cloud_Storage-blue)
![BigQuery](https://img.shields.io/badge/Data_Warehouse-BigQuery-blue)
![dbt](https://img.shields.io/badge/Transform-dbt-orange)
![Kestra](https://img.shields.io/badge/Orchestration-Kestra-purple)
![Python](https://img.shields.io/badge/Language-Python_3.13-yellow)

**Data Engineering Zoomcamp 2026 Capstone Project**

---

## Problem Statement

Singapore's public housing (HDB) resale market is one of the most active in the world, yet understanding affordability trends and market dynamics across towns and flat types requires piecing together fragmented data. Buyers, analysts, and policymakers lack a single, up-to-date analytical view of how resale prices have evolved — by town, room type, and time period.

This project builds an end-to-end data pipeline to answer:

- How have HDB resale prices evolved over time by town and flat type?
- What market segments exist across price ranges, room types, and affordability levels?
- Which towns show the highest growth and remain most affordable?

---

## Overview

This project ingests historical HDB resale flat price data from [data.gov.sg](https://data.gov.sg/datasets/189) and enriches it with geographic coordinates via the OneMap API. The pipeline loads raw data into Google Cloud Storage, transforms it through dbt models in BigQuery, and surfaces insights via a Looker Studio dashboard.

Orchestration is handled by Kestra, with infrastructure provisioned via Terraform on GCP.

---

## Table of Contents

- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Source Overview](#data-source-overview)
- [Data Pipeline](#data-pipeline)
- [Data Quality & Testing](#data-quality--testing)
- [Dashboard](#dashboard)
- [Steps to Reproduce](#steps-to-reproduce)
- [Known Limitations & Future Work](#known-limitations--future-work)
- [Resources](#resources)
- [Author](#author)

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Cloud Platform** | Google Cloud Platform (GCP) |
| **Infrastructure as Code** | Terraform |
| **Data Lake** | Google Cloud Storage (GCS) |
| **Data Warehouse** | BigQuery |
| **Ingestion** | Python 3.13+ |
| **Transformation** | dbt + BigQuery SQL |
| **Orchestration** | Kestra |
| **Visualization** | Looker Studio |
| **Package Manager** | uv |

---

## Architecture

```text
data.gov.sg (HDB CSV)
    ↓
Python Ingestion (hdb_loader.py)
    ↓
GCS Raw Layer (gs://de-zoomcamp-hdb-resale-hdb-raw/)
    ↓
OneMap API Geocoding (location_enricher.py)
    ↓
BigQuery raw_hdb (raw table)
    ↓
dbt Transformation
    ↓
BigQuery dbt_sthyagaraj
    ↓
Looker Studio Dashboard
```

Orchestration via Kestra manages both the backfill and weekly incremental flows end-to-end.

---

## Project Structure

```text
hdb-resale-pipeline/
├── README.md
├── pyproject.toml            # uv project config
├── uv.lock                   # pinned dependencies
├── requirements.txt          # legacy reference
├── .env.example              # environment variable template
├── .gitignore
│
├── terraform/                # GCP infrastructure
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars
│
├── src/ingestion/            # Python ingestion scripts
│   ├── hdb_loader.py         # Load HDB CSV to GCS + BigQuery
│   ├── location_enricher.py  # Geocode via OneMap API
│   └── config.py             # Shared configuration
│
├── dbt/                      # Data transformation
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_hdb_prices.sql
│   │   │   └── stg_locations.sql
│   │   └── marts/
│   │       ├── mart_hdb_by_month.sql
│   │       ├── mart_affordability_index.sql
│   │       └── mart_price_trends.sql
│   └── tests/
│
├── kestra/flows/             # Orchestration
│   ├── backfill_flow.yml
│   └── incremental_flow.yml
│
└── docs/
    ├── architecture.md
    └── video_script.md
```

---

## Data Source Overview

[data.gov.sg](https://data.gov.sg/datasets/189) provides historical HDB resale flat transaction records updated monthly. The dataset includes town, flat type, storey range, floor area, resale price, and lease commencement date going back to 1990.

Geographic enrichment is done via the [OneMap API](https://www.onemap.sg/docs/) — Singapore's official geocoding service — which maps street names to latitude/longitude coordinates.

---

## Data Pipeline

### 1. Data Extraction

- Download HDB resale CSV from data.gov.sg
- `hdb_loader.py` uploads raw CSV to GCS and loads it into BigQuery `raw_hdb`

### 2. Location Enrichment

- `location_enricher.py` calls the OneMap API for each unique street name
- Enriched dataset (with lat/long) written back to GCS and joined into staging

### 3. Data Transformation (dbt)

| Layer | Model | Description |
| ----- | ----- | ----------- |
| Staging | `stg_hdb_prices` | Cleaned prices with standardized columns |
| Staging | `stg_locations` | Lat/long from OneMap joined to transactions |
| Marts | `mart_hdb_by_month` | Monthly median price, count, price/sqft by town |
| Marts | `mart_affordability_index` | Affordability metrics by town |
| Marts | `mart_price_trends` | Quarter-over-quarter price evolution |

### 4. Orchestration (Kestra)

**Backfill flow** (one-time):

```
extract_csv → upload_to_gcs → geocode → load_to_bigquery → dbt_run → dbt_test
```

**Incremental flow** (weekly):

```
check_for_updates → enrich → append_to_gcs → load_delta → dbt_run_incremental → alert_on_failure
```

### 5. Visualization

Looker Studio dashboard connects to `dbt_sthyagaraj` in BigQuery and presents:

- **Affordability Trends** — median price by town over time
- **Market Segments** — room-type distribution and price ranges
- **Price Heatmap** — geographic scatter plot coloured by resale price

Filters: town, flat type, date range

---

## Data Quality & Testing

- **dbt schema tests:** `not_null`, `unique`, `accepted_values` on key columns
- **dbt custom tests:** price range sanity checks, floor area bounds
- **Terraform validation:** `terraform validate` and `terraform plan` before every apply
- **dbt docs:** full lineage and data dictionary at `dbt/target/index.html`

```bash
cd dbt
dbt test
dbt docs generate
```

---

## Dashboard

Looker Studio dashboard — connect to BigQuery dataset `dbt_sthyagaraj`.

> Dashboard link to be added after deployment.

---

## Steps to Reproduce

### Prerequisites

- GCP account with billing enabled
- `gcloud` CLI installed and authenticated
- Python 3.13+
- `uv` — [install guide](https://docs.astral.sh/uv/getting-started/installation/)
- Terraform >= 1.0
- Git

### 1. Clone & Configure Environment

```bash
git clone <repo-url>
cd hdb-resale-pipeline
cp .env.example .env
# Edit .env with your GCP project ID and paths
```

### 2. Install Dependencies

`uv init` has already been run — a `pyproject.toml` is included in the repo:

```bash
uv add google-cloud-bigquery google-cloud-storage pandas requests python-dotenv pyarrow
uv sync
```

> On subsequent clones, `uv sync` alone is sufficient — `uv.lock` pins all versions.

### 3. Create GCP Project & Enable APIs

```bash
export GCP_PROJECT_ID="your-project-id"
gcloud projects create $GCP_PROJECT_ID
gcloud config set project $GCP_PROJECT_ID

gcloud services enable bigquery.googleapis.com
gcloud services enable storage-api.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable logging.googleapis.com
```

### 4. Deploy Infrastructure (Terraform)

Terraform uses **Application Default Credentials (ADC)** — no credentials file needed in the config. Authenticate once with:

```bash
gcloud auth application-default login
```

Then deploy:

```bash
cd terraform
# Update terraform.tfvars with your GCP project ID
terraform init
terraform plan
terraform apply

# Retrieve service account details
terraform output service_account_email

# Save the private key locally
terraform output -raw service_account_key_private | base64 --decode > ../credentials/hdb-pipeline-sa.json
cd ..
```

> The service account (`hdb-pipeline-sa`) is created in GCP. Store `credentials/hdb-pipeline-sa.json` securely and never commit it.

### 5. Download HDB Dataset

Download from [data.gov.sg - HDB Resale Flat Prices](https://data.gov.sg/datasets/189) and place at `data/raw/hdb_resale.csv`.

### 6. Run Data Ingestion

```bash
uv run python src/ingestion/hdb_loader.py --file data/raw/hdb_resale.csv --backfill
uv run python src/ingestion/location_enricher.py --input gs://de-zoomcamp-hdb-resale-hdb-raw/hdb_resale.csv --output gs://de-zoomcamp-hdb-resale-hdb-raw/hdb_enriched.csv
```

### 7. Run dbt Transformations

```bash
cd dbt
dbt debug       # verify BigQuery connection
dbt deps
dbt run
dbt test
dbt docs generate
cd ..
```

### 8. Setup Kestra Orchestration

```bash
docker-compose up -d  # start Kestra locally
# Import flows via UI at http://localhost:8080
# or: kestra flow create < kestra/flows/backfill_flow.yml
```

### 9. Connect Looker Studio Dashboard

Open [Looker Studio](https://lookerstudio.google.com/) and connect to BigQuery dataset `dbt_sthyagaraj`.

---

## Known Limitations & Future Work

- **Proximity enrichment**: MRT/school proximity data deferred to Phase 2
- **Real-time updates**: Weekly batch only — no streaming
- **Forecasting**: Price prediction/ML models not in MVP scope
- **Monitoring**: Enhanced alerting and SLAs planned

---

## Resources

- [data.gov.sg HDB Resale Data](https://data.gov.sg/datasets/189)
- [OneMap API Documentation](https://www.onemap.sg/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Kestra Documentation](https://kestra.io/)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [uv Documentation](https://docs.astral.sh/uv/)

---

## Author

Sudharsan

---

**Status**: MVP Phase (April 2026)

## Completed

- [x] GCP infrastructure provisioned via Terraform (GCS bucket, BigQuery datasets, service account)
- [x] HDB resale data ingested from data.gov.sg → GCS + BigQuery (`raw_hdb.hdb_resale`, 228,542 rows)
- [x] Street geocoding via OneMap API → BigQuery (`raw_hdb.hdb_locations`, 577 streets)
- [x] dbt staging models: `stg_hdb_prices`, `stg_locations`
- [x] dbt mart models: `mart_hdb_by_month`, `mart_affordability_index`, `mart_price_trends`
- [x] 24 dbt tests passing across staging and mart layers
- [ ] Kestra orchestration flows
- [ ] Looker Studio dashboard
