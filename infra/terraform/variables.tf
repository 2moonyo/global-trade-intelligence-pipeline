variable "project_id" {
  description = "Google Cloud project id."
  type        = string
}

variable "gcp_location" {
  description = "Multi-region or region shared by the bucket and BigQuery datasets."
  type        = string
  default     = "EU"
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
  description = "Whether Terraform should create a runtime service account for scheduled pipeline runs."
  type        = bool
  default     = true
}

variable "pipeline_service_account_id" {
  description = "Account id for the pipeline runtime service account."
  type        = string
  default     = "capstone-pipeline"
}

variable "pipeline_service_account_display_name" {
  description = "Display name for the runtime service account."
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
