resource "google_project_service" "secretmanager" {
  count = var.vm_secret_sync_enabled ? 1 : 0

  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_secret_manager_secret" "vm_runtime_env" {
  for_each = var.vm_secret_sync_enabled ? var.vm_secret_env_to_secret_id : {}

  project   = var.project_id
  secret_id = each.value

  replication {
    auto {}
  }

  labels = merge(
    local.base_labels,
    {
      component = "capstone-vm-secret"
    },
  )

  depends_on = [google_project_service.secretmanager]
}
