variable "gcp_project_id" {
  description = "GCP Project ID"
  type        = string
  nullable    = false
}

variable "gcp_region" {
  description = "GCP Region for resources"
  type        = string
  default     = "asia-southeast1"
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset name for raw data"
  type        = string
  default     = "raw_hdb"
}


variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}
