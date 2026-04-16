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
  description = "Runtime service account attached to VM instances for metadata-based ADC."
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

output "primary_region" {
  value       = var.primary_region
  description = "Primary region for the Europe runtime and its snapshot policies."
}

output "primary_zone" {
  value       = var.primary_zone
  description = "Primary zone for the Europe runtime."
}

output "fallback_zone" {
  value       = var.fallback_zone
  description = "Fallback recovery zone."
}

output "legacy_region" {
  value       = var.legacy_compute_vm_enabled ? var.legacy_region : null
  description = "Legacy US runtime region when enabled."
}

output "legacy_zone" {
  value       = var.legacy_compute_vm_enabled ? var.legacy_zone : null
  description = "Legacy US runtime zone when enabled."
}

output "vm_machine_type" {
  value       = var.vm_machine_type
  description = "Machine type configured for VM runtimes."
}

output "vm_data_mount_point" {
  value       = var.vm_data_mount_point
  description = "Mount point configured by the VM startup script for attached persistent data disks."
}

output "vm_repo_root" {
  value       = var.vm_repo_root
  description = "Path on the mounted persistent disk where operators should copy the repository on the VM."
}

output "vm_env_file_path" {
  value       = var.vm_env_file_path
  description = "Root-owned env file path consumed by systemd services and docker compose on the VM."
}

output "vm_secret_sync_enabled" {
  value       = var.vm_secret_sync_enabled
  description = "Whether VM startup syncs selected runtime env keys from Secret Manager."
}

output "vm_secret_env_to_secret_id" {
  value       = var.vm_secret_env_to_secret_id
  description = "Configured mapping of runtime env variable names to Secret Manager secret IDs."
}

output "vm_secret_ids" {
  value       = [for secret in values(google_secret_manager_secret.vm_runtime_env) : secret.secret_id]
  description = "Secret Manager secret IDs created for VM runtime env sync."
}

output "vm_swap_enabled" {
  value       = var.vm_swap_enabled
  description = "Whether the VM startup script provisions swap space on the attached persistent disk."
}

output "vm_swap_size_gb" {
  value       = var.vm_swap_size_gb
  description = "Swap size in GB provisioned on the attached persistent disk when enabled."
}

output "vm_swap_file_path" {
  value       = var.vm_swap_file_path
  description = "Swap file path created by the VM startup script on the attached persistent disk."
}

output "vm_schedule_lane_timer_units" {
  value       = [for lane in sort(keys(var.vm_schedule_lane_timers)) : "capstone-schedule-lane-${lane}.timer"]
  description = "Systemd timer unit names written by the VM startup script for schedule lane execution."
}

output "vm_runtime_service_account_email" {
  value       = local.vm_runtime_service_account_email
  description = "Service account attached to VM instances for metadata-based ADC, when configured."
}

output "primary_vm_name" {
  value       = var.primary_compute_vm_enabled ? google_compute_instance.primary_vm[0].name : null
  description = "Primary Europe VM name when enabled."
}

output "primary_vm_zone" {
  value       = var.primary_compute_vm_enabled ? google_compute_instance.primary_vm[0].zone : null
  description = "Primary Europe VM zone when enabled."
}

output "primary_vm_external_ip" {
  value       = var.primary_compute_vm_enabled && var.vm_assign_public_ip ? google_compute_instance.primary_vm[0].network_interface[0].access_config[0].nat_ip : null
  description = "Ephemeral external IPv4 address for the primary VM when one is assigned."
}

output "primary_boot_disk_name" {
  value       = var.primary_compute_vm_enabled ? google_compute_disk.primary_boot_disk[0].name : null
  description = "Primary Europe boot disk name when enabled."
}

output "primary_data_disk_name" {
  value       = var.primary_compute_vm_enabled ? google_compute_disk.primary_data_disk[0].name : null
  description = "Primary Europe persistent data disk name when enabled."
}

output "primary_schedule_name" {
  value       = var.primary_compute_vm_enabled && var.primary_instance_schedule_enabled ? google_compute_resource_policy.primary_vm_schedule[0].name : null
  description = "Primary Europe instance schedule policy name when enabled."
}

output "primary_boot_snapshot_policy_name" {
  value       = var.primary_compute_vm_enabled && var.primary_snapshot_schedule_enabled ? google_compute_resource_policy.primary_boot_snapshot_schedule[0].name : null
  description = "Scheduled snapshot policy attached to the primary boot disk when enabled."
}

output "primary_data_snapshot_policy_name" {
  value       = var.primary_compute_vm_enabled && var.primary_snapshot_schedule_enabled ? google_compute_resource_policy.primary_data_snapshot_schedule[0].name : null
  description = "Scheduled snapshot policy attached to the primary data disk when enabled."
}

output "legacy_vm_name" {
  value       = var.legacy_compute_vm_enabled ? google_compute_instance.free_vm[0].name : null
  description = "Legacy US VM name when enabled."
}

output "legacy_vm_zone" {
  value       = var.legacy_compute_vm_enabled ? google_compute_instance.free_vm[0].zone : null
  description = "Legacy US VM zone when enabled."
}

output "legacy_vm_external_ip" {
  value       = var.legacy_compute_vm_enabled && var.vm_assign_public_ip ? google_compute_instance.free_vm[0].network_interface[0].access_config[0].nat_ip : null
  description = "Ephemeral external IPv4 address for the legacy US VM when enabled."
}

output "legacy_data_disk_name" {
  value       = var.legacy_compute_vm_enabled ? google_compute_disk.data_disk[0].name : null
  description = "Legacy US persistent data disk name when enabled."
}

output "legacy_schedule_name" {
  value       = var.legacy_compute_vm_enabled && var.legacy_instance_schedule_enabled ? google_compute_resource_policy.vm_schedule[0].name : null
  description = "Legacy US instance schedule policy name when enabled."
}

output "recovery_vm_name" {
  value       = var.recovery_vm_enabled ? google_compute_instance.recovery_vm[0].name : null
  description = "Fallback recovery VM name when enabled."
}

output "recovery_vm_zone" {
  value       = var.recovery_vm_enabled ? google_compute_instance.recovery_vm[0].zone : null
  description = "Fallback recovery VM zone when enabled."
}

output "recovery_vm_external_ip" {
  value       = var.recovery_vm_enabled && var.vm_assign_public_ip ? google_compute_instance.recovery_vm[0].network_interface[0].access_config[0].nat_ip : null
  description = "Ephemeral external IPv4 address for the fallback recovery VM when enabled."
}

output "recovery_boot_disk_name" {
  value       = var.recovery_boot_disk_enabled ? google_compute_disk.recovery_boot_disk[0].name : null
  description = "Fallback recovery boot disk name when enabled."
}

output "recovery_data_disk_name" {
  value       = var.recovery_data_disk_enabled ? google_compute_disk.recovery_data_disk[0].name : null
  description = "Fallback recovery data disk name when enabled."
}
