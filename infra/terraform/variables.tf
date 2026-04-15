variable "project_id" {
  description = "Google Cloud project id."
  type        = string
}

variable "gcp_location" {
  description = "Multi-region or region shared by the bucket and BigQuery datasets."
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "Bucket used for bronze and silver storage."
  type        = string
}

variable "gcs_prefix" {
  description = "Optional path prefix inside the bucket."
  type        = string
  default     = ""
}

variable "bucket_storage_class" {
  description = "Storage class for the lake bucket."
  type        = string
  default     = "STANDARD"
}

variable "raw_dataset_id" {
  description = "BigQuery raw landing dataset id."
  type        = string
  default     = "raw"
}

variable "analytics_dataset_id" {
  description = "BigQuery analytics dataset id."
  type        = string
  default     = "analytics"
}

variable "create_pipeline_service_account" {
  description = "Whether Terraform should create the user-managed service account attached to the VM runtime."
  type        = bool
  default     = true
}

variable "pipeline_service_account_id" {
  description = "Account id for the pipeline runtime service account."
  type        = string
  default     = "capstone-pipeline"
}

variable "pipeline_service_account_display_name" {
  description = "Display name for the VM runtime service account."
  type        = string
  default     = "Capstone Pipeline Runtime"
}

variable "storage_object_admin_members" {
  description = "Members granted object admin on the lake bucket."
  type        = list(string)
  default     = []
}

variable "raw_dataset_editor_members" {
  description = "Members granted dataEditor on the raw BigQuery dataset."
  type        = list(string)
  default     = []
}

variable "analytics_dataset_editor_members" {
  description = "Members granted dataEditor on the analytics BigQuery dataset."
  type        = list(string)
  default     = []
}

variable "analytics_dataset_viewer_members" {
  description = "Members granted dataViewer on the analytics BigQuery dataset."
  type        = list(string)
  default     = []
}

variable "project_job_user_members" {
  description = "Members granted bigquery.jobUser at project level."
  type        = list(string)
  default     = []
}

variable "labels" {
  description = "Optional labels applied to created resources."
  type        = map(string)
  default     = {}
}

variable "allow_force_destroy" {
  description = "When true, allow terraform destroy to remove non-empty buckets and datasets with contents."
  type        = bool
  default     = false
}

variable "primary_region" {
  description = "Region for the primary VM runtime and snapshot policies."
  type        = string
  default     = "europe-west1"
}

variable "primary_zone" {
  description = "Zone for the primary VM runtime."
  type        = string
  default     = "europe-west1-b"
}

variable "fallback_zone" {
  description = "Zone reserved for manual fallback recovery."
  type        = string
  default     = "europe-west1-d"
}

variable "legacy_region" {
  description = "Region for the legacy US runtime kept temporarily during migration."
  type        = string
  default     = "us-central1"
}

variable "legacy_zone" {
  description = "Zone for the legacy US runtime kept temporarily during migration."
  type        = string
  default     = "us-central1-a"
}

variable "vm_machine_type" {
  description = "Machine type for the Compute Engine VM."
  type        = string
  default     = "e2-standard-2"
}

variable "vm_boot_image" {
  description = "Boot image family or image reference used for fresh primary boot disks."
  type        = string
  default     = "debian-cloud/debian-11"
}

variable "vm_boot_disk_size_gb" {
  description = "Boot disk size in GB."
  type        = number
  default     = 25
}

variable "vm_boot_disk_type" {
  description = "Boot disk type for the VM."
  type        = string
  default     = "pd-standard"
}

variable "vm_data_disk_device_name" {
  description = "Device name used when attaching the additional data disk to the VM."
  type        = string
  default     = "data-disk"
}

variable "vm_data_disk_size_gb" {
  description = "Size in GB for a fresh additional persistent data disk."
  type        = number
  default     = 30
}

variable "vm_data_disk_type" {
  description = "Disk type for the additional persistent data disk."
  type        = string
  default     = "pd-standard"
}

variable "vm_network" {
  description = "VPC network name to use for VM runtimes."
  type        = string
  default     = "default"
}

variable "vm_subnetwork" {
  description = "Optional subnetwork self-link or name for VM runtimes. Leave null to use the network default in each region."
  type        = string
  default     = null
}

variable "vm_assign_public_ip" {
  description = "Whether to assign an ephemeral public IPv4 address to VM runtimes."
  type        = bool
  default     = true
}

variable "vm_data_mount_point" {
  description = "Mount point for the attached persistent data disk that stores repo state, logs, dbt artifacts, and Postgres data."
  type        = string
  default     = "/var/lib/pipeline"
}

variable "vm_repo_root" {
  description = "Absolute path on the mounted persistent disk where operators copy the repository for VM runs."
  type        = string
  default     = "/var/lib/pipeline/capstone"
}

variable "vm_env_file_path" {
  description = "Root-owned env file path consumed by systemd services and docker compose on the VM."
  type        = string
  default     = "/etc/capstone/pipeline.env"
}

variable "vm_swap_enabled" {
  description = "Whether the VM startup script should provision swap space on the attached persistent disk."
  type        = bool
  default     = true
}

variable "vm_swap_size_gb" {
  description = "Size in GB for the swap file created on the attached persistent disk."
  type        = number
  default     = 4
}

variable "vm_swap_file_path" {
  description = "Absolute path to the swap file stored on the attached persistent disk."
  type        = string
  default     = "/var/lib/pipeline/swapfile"
}

variable "vm_schedule_lane_timers" {
  description = "Map of schedule lane name to systemd OnCalendar expression. Terraform writes unit files for each entry, and operators enable only the timers they want."
  type        = map(string)
  default = {
    incremental_daily = "*-*-* 06:00:00 UTC"
    weekly_refresh    = "Mon *-*-* 06:15:00 UTC"
    monthly_refresh   = "*-*-01 06:30:00 UTC"
    yearly_refresh    = "*-01-01 06:45:00 UTC"
  }
}

variable "vm_service_account_email" {
  description = "Optional service account email to attach to VM runtimes for metadata-based ADC. Leave null to use the Terraform-created pipeline service account when available."
  type        = string
  default     = null
}

variable "legacy_compute_vm_enabled" {
  description = "Whether Terraform should keep the legacy US VM runtime and attached data disk."
  type        = bool
  default     = false
}

variable "legacy_vm_name" {
  description = "Name of the legacy US Compute Engine VM instance."
  type        = string
  default     = "capstone-vm"
}

variable "legacy_data_disk_name" {
  description = "Name of the legacy US persistent data disk."
  type        = string
  default     = "secondary-data-disk"
}

variable "legacy_instance_schedule_enabled" {
  description = "Whether to keep the legacy US Compute Engine instance schedule resource policy."
  type        = bool
  default     = true
}

variable "legacy_schedule_name" {
  description = "Name of the legacy US Compute Engine instance schedule resource policy."
  type        = string
  default     = "capstone-vm-schedule"
}

variable "legacy_schedule_timezone" {
  description = "IANA time zone used by the legacy US VM start/stop schedule."
  type        = string
  default     = "UTC"
}

variable "legacy_start_schedule" {
  description = "Cron schedule for starting the legacy US VM."
  type        = string
  default     = "45 5 * * *"
}

variable "legacy_stop_schedule" {
  description = "Cron schedule for stopping the legacy US VM."
  type        = string
  default     = "45 16 * * *"
}

variable "primary_compute_vm_enabled" {
  description = "Whether Terraform should create the primary Europe VM runtime."
  type        = bool
  default     = true
}

variable "primary_vm_name" {
  description = "Name of the primary Europe Compute Engine VM instance."
  type        = string
  default     = "capstone-vm-eu"
}

variable "primary_boot_disk_name" {
  description = "Name of the primary Europe boot disk."
  type        = string
  default     = "capstone-vm-eu-boot"
}

variable "primary_data_disk_name" {
  description = "Name of the primary Europe persistent data disk."
  type        = string
  default     = "capstone-vm-eu-data"
}

variable "primary_instance_schedule_enabled" {
  description = "Whether to create and attach a Compute Engine instance schedule resource policy to the primary VM."
  type        = bool
  default     = true
}

variable "primary_schedule_name" {
  description = "Name of the primary Europe Compute Engine instance schedule resource policy."
  type        = string
  default     = "capstone-vm-eu-schedule"
}

variable "primary_schedule_timezone" {
  description = "IANA time zone used by the primary VM start/stop schedule."
  type        = string
  default     = "UTC"
}

variable "primary_start_schedule" {
  description = "Cron schedule for starting the primary VM."
  type        = string
  default     = "45 5 * * *"
}

variable "primary_stop_schedule" {
  description = "Cron schedule for stopping the primary VM."
  type        = string
  default     = "45 16 * * *"
}

variable "primary_boot_restore_from_snapshot" {
  description = "Whether the primary boot disk should be restored from a snapshot instead of built from the boot image."
  type        = bool
  default     = false

  validation {
    condition     = !var.primary_boot_restore_from_snapshot || trimspace(var.primary_boot_source_snapshot) != ""
    error_message = "primary_boot_source_snapshot must be set when primary_boot_restore_from_snapshot is true."
  }
}

variable "primary_boot_source_snapshot" {
  description = "Snapshot self-link or name used to restore the primary boot disk."
  type        = string
  default     = ""
}

variable "primary_data_restore_from_snapshot" {
  description = "Whether the primary data disk should be restored from a snapshot instead of created empty."
  type        = bool
  default     = false

  validation {
    condition     = !var.primary_data_restore_from_snapshot || trimspace(var.primary_data_source_snapshot) != ""
    error_message = "primary_data_source_snapshot must be set when primary_data_restore_from_snapshot is true."
  }
}

variable "primary_data_source_snapshot" {
  description = "Snapshot self-link or name used to restore the primary data disk."
  type        = string
  default     = ""
}

variable "primary_snapshot_schedule_enabled" {
  description = "Whether to create scheduled snapshot policies for the primary boot and data disks."
  type        = bool
  default     = true
}

variable "primary_snapshot_hours_in_cycle" {
  description = "Compatibility input for primary snapshot cadence in hours. Must be 24 because the underlying daily schedule requires days_in_cycle = 1."
  type        = number
  default     = 24

  validation {
    condition     = var.primary_snapshot_hours_in_cycle == 24
    error_message = "primary_snapshot_hours_in_cycle must be 24 because Compute Engine daily snapshot schedules require days_in_cycle = 1."
  }
}

variable "primary_snapshot_start_time" {
  description = "UTC time when the primary disk snapshot cycle should start, in HH:MM format."
  type        = string
  default     = "04:00"
}

variable "primary_snapshot_retention_days" {
  description = "Retention period in days for automatically scheduled primary disk snapshots."
  type        = number
  default     = 7
}

variable "primary_snapshot_storage_locations" {
  description = "Storage locations used for scheduled primary disk snapshots."
  type        = list(string)
  default     = ["europe-west1"]
}

variable "primary_boot_snapshot_policy_name" {
  description = "Name of the scheduled snapshot policy attached to the primary boot disk."
  type        = string
  default     = "capstone-vm-eu-boot-snapshots"
}

variable "primary_data_snapshot_policy_name" {
  description = "Name of the scheduled snapshot policy attached to the primary data disk."
  type        = string
  default     = "capstone-vm-eu-data-snapshots"
}

variable "recovery_boot_disk_enabled" {
  description = "Whether Terraform should create the fallback recovery boot disk in the fallback zone."
  type        = bool
  default     = false

  validation {
    condition     = !var.recovery_boot_disk_enabled || trimspace(var.recovery_boot_source_snapshot) != ""
    error_message = "recovery_boot_source_snapshot must be set when recovery_boot_disk_enabled is true."
  }
}

variable "recovery_boot_source_snapshot" {
  description = "Snapshot self-link or name used to restore the fallback recovery boot disk."
  type        = string
  default     = ""
}

variable "recovery_data_disk_enabled" {
  description = "Whether Terraform should create the fallback recovery data disk in the fallback zone."
  type        = bool
  default     = false

  validation {
    condition     = !var.recovery_data_disk_enabled || trimspace(var.recovery_data_source_snapshot) != ""
    error_message = "recovery_data_source_snapshot must be set when recovery_data_disk_enabled is true."
  }
}

variable "recovery_data_source_snapshot" {
  description = "Snapshot self-link or name used to restore the fallback recovery data disk."
  type        = string
  default     = ""
}

variable "recovery_vm_enabled" {
  description = "Whether Terraform should create the fallback recovery VM in the fallback zone."
  type        = bool
  default     = false

  validation {
    condition = !var.recovery_vm_enabled || (
      var.recovery_boot_disk_enabled &&
      var.recovery_data_disk_enabled &&
      trimspace(var.recovery_boot_source_snapshot) != "" &&
      trimspace(var.recovery_data_source_snapshot) != ""
    )
    error_message = "recovery_vm_enabled requires both recovery disks to be enabled and both recovery snapshot names to be set."
  }
}

variable "recovery_vm_name" {
  description = "Name of the fallback recovery VM."
  type        = string
  default     = "capstone-vm-recovery"
}

variable "recovery_boot_disk_name" {
  description = "Name of the fallback recovery boot disk."
  type        = string
  default     = "capstone-vm-recovery-boot"
}

variable "recovery_data_disk_name" {
  description = "Name of the fallback recovery data disk."
  type        = string
  default     = "capstone-vm-recovery-data"
}
