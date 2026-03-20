output "service_account_email" {
  description = "Service account email for HDB pipeline"
  value       = google_service_account.hdb_pipeline.email
}

output "service_account_key_private" {
  description = "Service account private key (save this securely)"
  value       = google_service_account_key.hdb_pipeline_key.private_key
  sensitive   = true
}

output "gcs_bucket_raw" {
  description = "GCS bucket name for raw data"
  value       = google_storage_bucket.hdb_raw.name
}

output "bq_dataset_raw" {
  description = "BigQuery raw dataset"
  value       = google_bigquery_dataset.raw_hdb.dataset_id
}

output "bq_dataset_dbt" {
  description = "BigQuery dbt dataset"
  value       = google_bigquery_dataset.dbt_sthyagaraj.dataset_id
}
