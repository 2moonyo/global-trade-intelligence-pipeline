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
  description = "Runtime service account attached to the VM for metadata-based ADC."
}

output "runtime_env" {
  description = "Environment values expected by the Python cloud scripts and dbt BigQuery target."
  value = {
    GCP_PROJECT_ID                 = var.project_id
    GCP_LOCATION                   = var.gcp_location
    GCS_BUCKET                     = google_storage_bucket.lake.name
    GCS_PREFIX                     = var.gcs_prefix
    GCP_BIGQUERY_RAW_DATASET       = google_bigquery_dataset.raw.dataset_id
    GCP_BIGQUERY_ANALYTICS_DATASET = google_bigquery_dataset.analytics.dataset_id
    DBT_BIGQUERY_DATASET           = google_bigquery_dataset.analytics.dataset_id
  }
}

output "gcp_region" {
  value       = var.gcp_region
  description = "Primary region for VM-adjacent resources."
}

output "gcp_zone" {
  value       = var.gcp_zone
  description = "Primary zone for the free-tier VM and attached data disk."
}

output "vm_name" {
  value       = var.create_compute_vm ? google_compute_instance.free_vm[0].name : null
  description = "Compute Engine VM name when VM provisioning is enabled."
}

output "vm_zone" {
  value       = var.create_compute_vm ? google_compute_instance.free_vm[0].zone : null
  description = "Compute Engine VM zone when VM provisioning is enabled."
}

output "vm_external_ip" {
  value       = var.create_compute_vm && var.vm_assign_public_ip ? google_compute_instance.free_vm[0].network_interface[0].access_config[0].nat_ip : null
  description = "Ephemeral external IPv4 address for the VM when one is assigned."
}

output "vm_data_disk_name" {
  value       = var.create_compute_vm ? google_compute_disk.data_disk[0].name : null
  description = "Attached persistent data disk name when VM provisioning is enabled."
}

output "vm_data_mount_point" {
  value       = var.create_compute_vm ? var.vm_data_mount_point : null
  description = "Mount point configured by the VM startup script for the additional persistent data disk."
}

output "vm_repo_root" {
  value       = var.create_compute_vm ? var.vm_repo_root : null
  description = "Path on the mounted persistent disk where operators should copy the repository on the VM."
}

output "vm_env_file_path" {
  value       = var.create_compute_vm ? var.vm_env_file_path : null
  description = "Root-owned env file path consumed by systemd services and docker compose on the VM."
}

output "vm_schedule_lane_timer_units" {
  value       = var.create_compute_vm ? [for lane in sort(keys(var.vm_schedule_lane_timers)) : "capstone-schedule-lane-${lane}.timer"] : []
  description = "Systemd timer unit names written by the VM startup script for schedule lane execution."
}

output "vm_schedule_name" {
  value       = var.create_compute_vm && var.enable_vm_instance_schedule ? google_compute_resource_policy.vm_schedule[0].name : null
  description = "Attached instance schedule resource policy name when schedule provisioning is enabled."
}

output "vm_runtime_service_account_email" {
  value       = var.create_compute_vm ? local.vm_runtime_service_account_email : null
  description = "Service account attached to the VM for metadata-based ADC, when configured."
}
