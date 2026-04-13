variable "project_id" {
  description = "Google Cloud project id."
  type        = string
}

variable "gcp_location" {
  description = "Multi-region or region shared by the bucket and BigQuery datasets."
  type        = string
  default     = "us-central1"
}

variable "gcp_region" {
  description = "Primary region for regional resources such as the VM schedule policy."
  type        = string
  default     = "us-central1"
}

variable "gcp_zone" {
  description = "Primary zone for zonal resources such as the free-tier VM and attached disk."
  type        = string
  default     = "us-central1-a"
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

variable "create_compute_vm" {
  description = "Whether Terraform should create the free-tier-friendly Compute Engine VM and attached data disk."
  type        = bool
  default     = false
}

variable "vm_name" {
  description = "Name of the Compute Engine VM instance."
  type        = string
  default     = "free-tier-vm"
}

variable "vm_machine_type" {
  description = "Machine type for the Compute Engine VM."
  type        = string
  default     = "e2-micro"
}

variable "vm_boot_image" {
  description = "Boot image family or image reference for the VM boot disk."
  type        = string
  default     = "debian-cloud/debian-11"
}

variable "vm_boot_disk_size_gb" {
  description = "Boot disk size in GB."
  type        = number
  default     = 18
}

variable "vm_boot_disk_type" {
  description = "Boot disk type for the VM."
  type        = string
  default     = "pd-standard"
}

variable "vm_data_disk_name" {
  description = "Name of the additional persistent data disk."
  type        = string
  default     = "secondary-data-disk"
}

variable "vm_data_disk_device_name" {
  description = "Device name used when attaching the additional data disk to the VM."
  type        = string
  default     = "data-disk"
}

variable "vm_data_disk_size_gb" {
  description = "Size in GB for the additional persistent data disk."
  type        = number
  default     = 12
}

variable "vm_data_disk_type" {
  description = "Disk type for the additional persistent data disk."
  type        = string
  default     = "pd-standard"
}

variable "vm_network" {
  description = "VPC network name to use for the VM."
  type        = string
  default     = "default"
}

variable "vm_subnetwork" {
  description = "Optional subnetwork self-link or name for the VM. Leave null to use the network default."
  type        = string
  default     = null
}

variable "vm_assign_public_ip" {
  description = "Whether to assign an ephemeral public IPv4 address to the VM."
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
  description = "Root-owned env file path consumed by systemd and docker compose on the VM."
  type        = string
  default     = "/etc/capstone/pipeline.env"
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
  description = "Optional service account email to attach to the VM for metadata-based ADC. Leave null to use the Terraform-created pipeline service account when available."
  type        = string
  default     = null
}

variable "enable_vm_instance_schedule" {
  description = "Whether to create and attach a Compute Engine instance schedule resource policy to the VM."
  type        = bool
  default     = false
}

variable "vm_schedule_name" {
  description = "Name of the Compute Engine instance schedule resource policy."
  type        = string
  default     = "capstone-vm-schedule"
}

variable "vm_schedule_timezone" {
  description = "IANA time zone used by the VM start/stop schedule."
  type        = string
  default     = "UTC"
}

variable "vm_start_schedule" {
  description = "Cron schedule for starting the VM. Per Google guidance, set this at least 15 minutes before the first in-VM job."
  type        = string
  default     = "45 5 * * *"
}

variable "vm_stop_schedule" {
  description = "Cron schedule for stopping the VM. Keep it at least 15 minutes after the latest expected job or retry window."
  type        = string
  default     = "45 10 * * *"
}
