# This module intentionally creates only a user-managed service account and IAM bindings.
# It does not create a service-account key. The VM authenticates with Application Default
# Credentials (ADC) over the Compute Engine metadata server, which returns short-lived
# credentials for the attached service account at runtime.

locals {
  pipeline_sa_member = var.create_pipeline_service_account ? [
    "serviceAccount:${google_service_account.pipeline[0].email}"
  ] : []

  vm_runtime_sa_member = var.vm_service_account_email != null ? "serviceAccount:${var.vm_service_account_email}" : (
    var.create_pipeline_service_account ? "serviceAccount:${google_service_account.pipeline[0].email}" : null
  )

  storage_object_admin_members     = distinct(concat(var.storage_object_admin_members, local.pipeline_sa_member))
  raw_dataset_editor_members       = distinct(concat(var.raw_dataset_editor_members, local.pipeline_sa_member))
  analytics_dataset_editor_members = distinct(concat(var.analytics_dataset_editor_members, local.pipeline_sa_member))
  project_job_user_members         = distinct(concat(var.project_job_user_members, local.pipeline_sa_member))
  project_bigquery_user_members    = distinct(concat(var.project_bigquery_user_members, local.pipeline_sa_member))
}

resource "google_service_account" "pipeline" {
  count        = var.create_pipeline_service_account ? 1 : 0
  account_id   = var.pipeline_service_account_id
  display_name = var.pipeline_service_account_display_name
}

resource "google_storage_bucket_iam_member" "object_admin" {
  for_each = toset(local.storage_object_admin_members)
  bucket   = google_storage_bucket.lake.name
  role     = "roles/storage.objectAdmin"
  member   = each.value
}

resource "google_bigquery_dataset_iam_member" "raw_editor" {
  for_each   = toset(local.raw_dataset_editor_members)
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = each.value
}

resource "google_bigquery_dataset_iam_member" "analytics_editor" {
  for_each   = toset(local.analytics_dataset_editor_members)
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = each.value
}

resource "google_bigquery_dataset_iam_member" "analytics_viewer" {
  for_each   = toset(var.analytics_dataset_viewer_members)
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

resource "google_project_iam_member" "job_user" {
  for_each = toset(local.project_job_user_members)
  project  = var.project_id
  role     = "roles/bigquery.jobUser"
  member   = each.value
}

resource "google_project_iam_member" "bigquery_user" {
  for_each = toset(local.project_bigquery_user_members)
  project  = var.project_id
  role     = "roles/bigquery.user"
  member   = each.value
}

resource "google_secret_manager_secret_iam_member" "vm_runtime_secret_accessor" {
  for_each = local.vm_runtime_sa_member == null ? {} : google_secret_manager_secret.vm_runtime_env

  project   = var.project_id
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.vm_runtime_sa_member
}
