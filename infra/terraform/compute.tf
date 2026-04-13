# The VM uses a user-managed service account attached directly to the instance.
# On Compute Engine, Google client libraries and dbt's BigQuery adapter can resolve
# ADC without a JSON key because the metadata server mints short-lived credentials.

locals {
  vm_runtime_service_account_email = var.vm_service_account_email != null ? var.vm_service_account_email : try(google_service_account.pipeline[0].email, null)
}

resource "google_compute_disk" "data_disk" {
  provider = google-beta

  count = var.create_compute_vm ? 1 : 0

  name = var.vm_data_disk_name
  type = var.vm_data_disk_type
  size = var.vm_data_disk_size_gb
  zone = var.gcp_zone
  labels = merge(
    local.base_labels,
    {
      component = "capstone-vm-data-disk"
    },
  )
}

resource "google_compute_resource_policy" "vm_schedule" {
  provider = google-beta

  count = var.create_compute_vm && var.enable_vm_instance_schedule ? 1 : 0

  name        = var.vm_schedule_name
  region      = var.gcp_region
  description = "Start and stop policy for ${var.vm_name}."

  instance_schedule_policy {
    vm_start_schedule {
      schedule = var.vm_start_schedule
    }

    vm_stop_schedule {
      schedule = var.vm_stop_schedule
    }

    time_zone = var.vm_schedule_timezone
  }
}

resource "google_compute_instance" "free_vm" {
  provider = google-beta

  count = var.create_compute_vm ? 1 : 0

  name                      = var.vm_name
  machine_type              = var.vm_machine_type
  zone                      = var.gcp_zone
  allow_stopping_for_update = true
  deletion_protection       = false
  resource_policies = var.create_compute_vm && var.enable_vm_instance_schedule ? [
    google_compute_resource_policy.vm_schedule[0].self_link,
  ] : []

  boot_disk {
    initialize_params {
      image = var.vm_boot_image
      size  = var.vm_boot_disk_size_gb
      type  = var.vm_boot_disk_type
    }
  }

  network_interface {
    network    = var.vm_network
    subnetwork = var.vm_subnetwork

    dynamic "access_config" {
      for_each = var.vm_assign_public_ip ? [1] : []
      content {}
    }
  }

  attached_disk {
    source      = google_compute_disk.data_disk[0].id
    device_name = var.vm_data_disk_device_name
    mode        = "READ_WRITE"
  }

  metadata_startup_script = templatefile(
    "${path.module}/templates/vm_startup.sh.tftpl",
    {
      data_disk_device_name = var.vm_data_disk_device_name
      data_mount_point      = var.vm_data_mount_point
      repo_root             = var.vm_repo_root
      env_file_path         = var.vm_env_file_path
      schedule_lane_timers  = var.vm_schedule_lane_timers
    },
  )

  dynamic "service_account" {
    for_each = local.vm_runtime_service_account_email == null ? [] : [local.vm_runtime_service_account_email]
    content {
      email  = service_account.value
      scopes = ["cloud-platform"]
    }
  }

  lifecycle {
    precondition {
      condition     = local.vm_runtime_service_account_email != null
      error_message = "Compute VM creation requires a service account so the runtime can use keyless ADC through the metadata server."
    }
  }

  labels = merge(
    local.base_labels,
    {
      component = "capstone-vm"
    },
  )
}
