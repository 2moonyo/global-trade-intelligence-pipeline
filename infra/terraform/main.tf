locals {
  base_labels = merge(
    {
      managed_by = "terraform"
      workload   = "capstone-monthly"
    },
    var.labels
  )

  pipeline_sa_member = var.create_pipeline_service_account ? [
    "serviceAccount:${google_service_account.pipeline[0].email}"
  ] : []

  storage_object_admin_members     = distinct(concat(var.storage_object_admin_members, local.pipeline_sa_member))
  raw_dataset_editor_members       = distinct(concat(var.raw_dataset_editor_members, local.pipeline_sa_member))
  analytics_dataset_editor_members = distinct(concat(var.analytics_dataset_editor_members, local.pipeline_sa_member))
  project_job_user_members         = distinct(concat(var.project_job_user_members, local.pipeline_sa_member))
}

resource "google_storage_bucket" "lake" {
  name                        = var.gcs_bucket_name
  location                    = var.gcp_location
  storage_class               = var.bucket_storage_class
  uniform_bucket_level_access = true
  force_destroy               = var.allow_force_destroy

  public_access_prevention = "enforced"

  versioning {
    enabled = true
  }

  labels = local.base_labels
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.raw_dataset_id
  location                   = var.gcp_location
  delete_contents_on_destroy = var.allow_force_destroy
  labels                     = local.base_labels
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = var.analytics_dataset_id
  location                   = var.gcp_location
  delete_contents_on_destroy = var.allow_force_destroy
  labels                     = local.base_labels
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
