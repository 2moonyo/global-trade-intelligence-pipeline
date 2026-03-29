output "project_id" {
  value       = var.project_id
  description = "Google Cloud project id."
}

output "gcp_location" {
  value       = var.gcp_location
  description = "Shared location for bucket and BigQuery datasets."
}

output "gcs_bucket_name" {
  value       = google_storage_bucket.lake.name
  description = "Lake bucket name."
}

output "gcs_prefix" {
  value       = var.gcs_prefix
  description = "Optional object prefix inside the bucket."
}

output "raw_dataset_id" {
  value       = google_bigquery_dataset.raw.dataset_id
  description = "BigQuery raw landing dataset."
}

output "analytics_dataset_id" {
  value       = google_bigquery_dataset.analytics.dataset_id
  description = "BigQuery analytics dataset."
}

output "pipeline_service_account_email" {
  value       = var.create_pipeline_service_account ? google_service_account.pipeline[0].email : null
  description = "Runtime service account for scheduled pipeline runs."
}

output "runtime_env" {
  description = "Environment values expected by the Python cloud scripts and dbt BigQuery target."
  value = {
    GCP_PROJECT_ID                = var.project_id
    GCP_LOCATION                  = var.gcp_location
    GCS_BUCKET                    = google_storage_bucket.lake.name
    GCS_PREFIX                    = var.gcs_prefix
    GCP_BIGQUERY_RAW_DATASET      = google_bigquery_dataset.raw.dataset_id
    GCP_BIGQUERY_ANALYTICS_DATASET = google_bigquery_dataset.analytics.dataset_id
    DBT_BIGQUERY_DATASET          = google_bigquery_dataset.analytics.dataset_id
  }
}
