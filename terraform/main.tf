terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# Service Account for Data Pipeline
resource "google_service_account" "hdb_pipeline" {
  account_id   = "hdb-pipeline-sa"
  display_name = "HDB Pipeline Service Account"
}

# IAM Binding: BigQuery Admin
resource "google_project_iam_member" "hdb_bq_admin" {
  project = var.gcp_project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.hdb_pipeline.email}"
}

# IAM Binding: Storage Admin
resource "google_project_iam_member" "hdb_storage_admin" {
  project = var.gcp_project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.hdb_pipeline.email}"
}

# IAM Binding: Cloud Logging
resource "google_project_iam_member" "hdb_logging" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.hdb_pipeline.email}"
}

# Service Account Key
resource "google_service_account_key" "hdb_pipeline_key" {
  service_account_id = google_service_account.hdb_pipeline.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

# GCS Bucket: Raw Data Layer
resource "google_storage_bucket" "hdb_raw" {
  name          = "${var.gcp_project_id}-hdb-raw"
  location      = var.gcp_region
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    environment = "dev"
    project     = "hdb-pipeline"
    layer       = "raw"
  }
}

# BigQuery Dataset: Raw
resource "google_bigquery_dataset" "raw_hdb" {
  dataset_id    = var.bq_dataset_raw
  friendly_name = "Raw HDB Data"
  description   = "Raw HDB resale flat prices from data.gov.sg"
  location      = var.gcp_region

  labels = {
    environment = "dev"
    project     = "hdb-pipeline"
    layer       = "raw"
  }

  access {
    role          = "OWNER"
    user_by_email = google_service_account.hdb_pipeline.email
  }
}

# BigQuery Dataset: dbt
resource "google_bigquery_dataset" "dbt_sthyagaraj" {
  dataset_id    = "dbt_sthyagaraj"
  friendly_name = "dbt HDB Models"
  description   = "dbt-transformed HDB resale data"
  location      = var.gcp_region

  labels = {
    environment = "dev"
    project     = "hdb-pipeline"
    layer       = "dbt"
  }

  access {
    role          = "OWNER"
    user_by_email = google_service_account.hdb_pipeline.email
  }
}
