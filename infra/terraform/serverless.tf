locals {
  serverless_active           = var.serverless_enabled && var.execution_profile == "hybrid_vm_serverless"
  serverless_region           = coalesce(var.serverless_region, var.primary_region)
  serverless_scheduler_region = coalesce(var.serverless_scheduler_region, local.serverless_region)
  serverless_jobs             = local.serverless_active ? var.serverless_scheduled_batches : {}

  serverless_runtime_env = {
    GCP_PROJECT_ID                 = var.project_id
    GCP_LOCATION                   = var.gcp_location
    GCS_BUCKET                     = google_storage_bucket.lake.name
    GCS_PREFIX                     = var.gcs_prefix
    GCP_BIGQUERY_RAW_DATASET       = google_bigquery_dataset.raw.dataset_id
    GCP_BIGQUERY_ANALYTICS_DATASET = google_bigquery_dataset.analytics.dataset_id
    DBT_BIGQUERY_DATASET           = google_bigquery_dataset.analytics.dataset_id
    BATCH_PLAN_PATH                = "ops/batch_plan.json"
    EXECUTION_PROFILE              = var.execution_profile
    EXECUTION_RUNTIME              = "cloud_run"
    EXECUTION_PROFILE_PATH         = "ops/execution_profiles.json"
    OPS_POSTGRES_ENABLED           = "false"
    ENABLE_BIGQUERY_OPS_MIRROR     = "true"
    OPS_STRICT_BIGQUERY_MIRROR     = "false"
    GOOGLE_AUTH_MODE               = "auto"
    GOOGLE_APPLICATION_CREDENTIALS = ""
    POSTGRES_SCHEMA                = "ops"
    AWS_EC2_METADATA_DISABLED      = "true"
    TELEMETRY_OPTOUT               = "true"
  }

  serverless_runtime_sa_member = local.serverless_active ? "serviceAccount:${google_service_account.serverless_runtime[0].email}" : null
  scheduler_sa_member          = local.serverless_active ? "serviceAccount:${google_service_account.serverless_scheduler[0].email}" : null
}

resource "google_project_service" "cloudrun" {
  count = local.serverless_active ? 1 : 0

  project            = var.project_id
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudscheduler" {
  count = local.serverless_active ? 1 : 0

  project            = var.project_id
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry" {
  count = local.serverless_active ? 1 : 0

  project            = var.project_id
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_service_account" "serverless_runtime" {
  count = local.serverless_active ? 1 : 0

  account_id   = var.serverless_runtime_service_account_id
  display_name = var.serverless_runtime_service_account_display_name
}

resource "google_service_account" "serverless_scheduler" {
  count = local.serverless_active ? 1 : 0

  account_id   = var.serverless_scheduler_service_account_id
  display_name = var.serverless_scheduler_service_account_display_name
}

resource "google_storage_bucket_iam_member" "serverless_object_admin" {
  count = local.serverless_active ? 1 : 0

  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = local.serverless_runtime_sa_member
}

resource "google_bigquery_dataset_iam_member" "serverless_raw_editor" {
  count = local.serverless_active ? 1 : 0

  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = local.serverless_runtime_sa_member
}

resource "google_bigquery_dataset_iam_member" "serverless_analytics_editor" {
  count = local.serverless_active ? 1 : 0

  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = local.serverless_runtime_sa_member
}

resource "google_project_iam_member" "serverless_job_user" {
  count = local.serverless_active ? 1 : 0

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = local.serverless_runtime_sa_member
}

resource "google_project_iam_member" "serverless_bigquery_user" {
  count = local.serverless_active ? 1 : 0

  project = var.project_id
  role    = "roles/bigquery.user"
  member  = local.serverless_runtime_sa_member
}

resource "google_project_iam_member" "serverless_artifactregistry_reader" {
  count = local.serverless_active ? 1 : 0

  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = local.serverless_runtime_sa_member
}

resource "google_secret_manager_secret_iam_member" "serverless_runtime_secret_accessor" {
  for_each = local.serverless_active ? var.serverless_secret_env_to_secret_id : {}

  project   = var.project_id
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = local.serverless_runtime_sa_member

  depends_on = [google_project_service.secretmanager]
}

resource "google_service_account_iam_member" "scheduler_act_as_serverless_runtime" {
  count = local.serverless_active ? 1 : 0

  service_account_id = google_service_account.serverless_runtime[0].name
  role               = "roles/iam.serviceAccountUser"
  member             = local.scheduler_sa_member
}

resource "google_cloud_run_v2_job" "serverless_dataset_batch" {
  for_each = local.serverless_jobs

  name                = each.value.job_name
  location            = local.serverless_region
  deletion_protection = var.serverless_deletion_protection
  labels = merge(
    local.base_labels,
    {
      component         = "capstone-cloud-run-job"
      execution_profile = var.execution_profile
      dataset           = each.value.dataset_name
      batch_id          = each.value.batch_id
    },
  )

  template {
    task_count  = try(each.value.task_count, 1)
    parallelism = try(each.value.parallelism, 1)

    template {
      service_account = google_service_account.serverless_runtime[0].email
      timeout         = try(each.value.timeout, var.serverless_default_task_timeout)
      max_retries     = try(each.value.max_retries, var.serverless_default_max_retries)

      containers {
        image   = var.serverless_container_image
        command = ["/workspace/scripts/run_serverless_batch.sh"]
        args    = [each.value.dataset_name, each.value.batch_id]

        resources {
          limits = {
            cpu    = try(each.value.cpu, var.serverless_default_cpu)
            memory = try(each.value.memory, var.serverless_default_memory)
          }
        }

        dynamic "env" {
          for_each = local.serverless_runtime_env
          content {
            name  = env.key
            value = env.value
          }
        }

        dynamic "env" {
          for_each = var.serverless_secret_env_to_secret_id
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = env.value
                version = "latest"
              }
            }
          }
        }
      }
    }
  }

  lifecycle {
    precondition {
      condition = (
        trimspace(var.serverless_container_image) != ""
        && trimspace(var.serverless_container_image) != "REPLACE_AFTER_BUILD"
        && can(regex("^(?:[a-z0-9-]+-docker\\.pkg\\.dev|(?:[a-z0-9-]+\\.)?gcr\\.io|docker\\.io)/.+", trimspace(var.serverless_container_image)))
      )
      error_message = "serverless_container_image must be a real container image URI such as REGION-docker.pkg.dev/PROJECT/REPOSITORY/capstone-pipeline:TAG when hybrid serverless resources are active."
    }
  }

  depends_on = [
    google_project_service.cloudrun,
    google_project_service.artifactregistry,
    google_storage_bucket_iam_member.serverless_object_admin,
    google_bigquery_dataset_iam_member.serverless_raw_editor,
    google_bigquery_dataset_iam_member.serverless_analytics_editor,
    google_project_iam_member.serverless_job_user,
    google_project_iam_member.serverless_bigquery_user,
    google_project_iam_member.serverless_artifactregistry_reader,
    google_secret_manager_secret_iam_member.serverless_runtime_secret_accessor,
  ]
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  for_each = local.serverless_jobs

  project  = var.project_id
  location = local.serverless_region
  name     = google_cloud_run_v2_job.serverless_dataset_batch[each.key].name
  role     = "roles/run.invoker"
  member   = local.scheduler_sa_member
}

resource "google_cloud_scheduler_job" "serverless_dataset_batch" {
  for_each = local.serverless_jobs

  name             = each.value.scheduler_name
  description      = each.value.description
  project          = var.project_id
  region           = local.serverless_scheduler_region
  schedule         = each.value.schedule
  time_zone        = try(each.value.time_zone, var.serverless_scheduler_time_zone)
  attempt_deadline = try(each.value.attempt_deadline, var.serverless_scheduler_attempt_deadline)
  paused           = var.serverless_scheduler_paused

  retry_config {
    retry_count          = try(each.value.scheduler_retry_count, var.serverless_scheduler_retry_count)
    min_backoff_duration = var.serverless_scheduler_min_backoff_duration
    max_backoff_duration = var.serverless_scheduler_max_backoff_duration
    max_doublings        = var.serverless_scheduler_max_doublings
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${local.serverless_region}/jobs/${google_cloud_run_v2_job.serverless_dataset_batch[each.key].name}:run"

    oauth_token {
      service_account_email = google_service_account.serverless_scheduler[0].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_project_service.cloudscheduler,
    google_cloud_run_v2_job.serverless_dataset_batch,
    google_cloud_run_v2_job_iam_member.scheduler_invoker,
    google_service_account_iam_member.scheduler_act_as_serverless_runtime,
  ]
}
