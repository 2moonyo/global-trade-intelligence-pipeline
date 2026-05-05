locals {
  base_labels = merge(
    {
      managed_by = "terraform"
      workload   = "capstone-monthly"
    },
    var.labels
  )
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
