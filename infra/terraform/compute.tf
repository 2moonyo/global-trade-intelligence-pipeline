# The VM runtime uses a user-managed service account attached directly to each instance.
# On Compute Engine, Google client libraries and dbt's BigQuery adapter can resolve
# ADC without a JSON key because the metadata server mints short-lived credentials.

locals {
  vm_runtime_service_account_email = var.vm_service_account_email != null ? var.vm_service_account_email : try(google_service_account.pipeline[0].email, null)

  vm_metadata_startup_script = templatefile(
    "${path.module}/templates/vm_startup.sh.tftpl",
    {
      data_disk_device_name = var.vm_data_disk_device_name
      data_mount_point      = var.vm_data_mount_point
      repo_root             = var.vm_repo_root
      env_file_path         = var.vm_env_file_path
      swap_enabled          = var.vm_swap_enabled
      swap_size_gb          = var.vm_swap_size_gb
      swap_file_path        = var.vm_swap_file_path
      schedule_lane_timers  = var.vm_schedule_lane_timers
    },
  )
}

# Keep the legacy US runtime on the existing Terraform addresses so it can remain
# state-managed during the Europe migration and be removed later in a second step.
resource "google_compute_disk" "data_disk" {
  provider = google-beta

  count = var.legacy_compute_vm_enabled ? 1 : 0

  name = var.legacy_data_disk_name
  type = var.vm_data_disk_type
  size = var.vm_data_disk_size_gb
  zone = var.legacy_zone
  labels = merge(
    local.base_labels,
    {
      component = "capstone-vm-data-disk"
      runtime   = "legacy"
    },
  )

  # Leave the legacy disk untouched during migration. We only want to remove it
  # later by setting legacy_compute_vm_enabled=false after Europe validation.
  lifecycle {
    ignore_changes = all
  }
}

resource "google_compute_resource_policy" "vm_schedule" {
  provider = google-beta

  count = var.legacy_compute_vm_enabled && var.legacy_instance_schedule_enabled ? 1 : 0

  name        = var.legacy_schedule_name
  region      = var.legacy_region
  description = "Start and stop policy for legacy runtime ${var.legacy_vm_name}."

  instance_schedule_policy {
    vm_start_schedule {
      schedule = var.legacy_start_schedule
    }

    vm_stop_schedule {
      schedule = var.legacy_stop_schedule
    }

    time_zone = var.legacy_schedule_timezone
  }

  # Preserve the existing legacy schedule as-is during migration.
  lifecycle {
    ignore_changes = all
  }
}

resource "google_compute_instance" "free_vm" {
  provider = google-beta

  count = var.legacy_compute_vm_enabled ? 1 : 0

  name                      = var.legacy_vm_name
  machine_type              = var.vm_machine_type
  zone                      = var.legacy_zone
  allow_stopping_for_update = true
  deletion_protection       = false
  resource_policies = var.legacy_compute_vm_enabled && var.legacy_instance_schedule_enabled ? [
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

  metadata_startup_script = local.vm_metadata_startup_script

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

    # The legacy US VM must remain completely untouched while we stand up and
    # validate the Europe runtime. Otherwise metadata_startup_script drift can
    # force a destructive replacement of the old instance.
    ignore_changes = all
  }

  labels = merge(
    local.base_labels,
    {
      component = "capstone-vm"
      runtime   = "legacy"
    },
  )
}

# The primary Europe runtime keeps the boot and data disks separate so migration can
# restore each disk independently and scheduled snapshots can be attached per disk.
resource "google_compute_disk" "primary_boot_disk" {
  provider = google-beta

  count = var.primary_compute_vm_enabled ? 1 : 0

  name     = var.primary_boot_disk_name
  type     = var.vm_boot_disk_type
  size     = var.primary_boot_restore_from_snapshot ? null : var.vm_boot_disk_size_gb
  zone     = var.primary_zone
  image    = var.primary_boot_restore_from_snapshot ? null : var.vm_boot_image
  snapshot = var.primary_boot_restore_from_snapshot ? var.primary_boot_source_snapshot : null
  labels = merge(
    local.base_labels,
    {
      component = "capstone-primary-vm-boot-disk"
      runtime   = "primary"
    },
  )
}

resource "google_compute_disk" "primary_data_disk" {
  provider = google-beta

  count = var.primary_compute_vm_enabled ? 1 : 0

  name     = var.primary_data_disk_name
  type     = var.vm_data_disk_type
  size     = var.primary_data_restore_from_snapshot ? null : var.vm_data_disk_size_gb
  zone     = var.primary_zone
  snapshot = var.primary_data_restore_from_snapshot ? var.primary_data_source_snapshot : null
  labels = merge(
    local.base_labels,
    {
      component = "capstone-primary-vm-data-disk"
      runtime   = "primary"
    },
  )
}

resource "google_compute_resource_policy" "primary_vm_schedule" {
  provider = google-beta

  count = var.primary_compute_vm_enabled && var.primary_instance_schedule_enabled ? 1 : 0

  name        = var.primary_schedule_name
  region      = var.primary_region
  description = "Start and stop policy for primary runtime ${var.primary_vm_name}."

  instance_schedule_policy {
    vm_start_schedule {
      schedule = var.primary_start_schedule
    }

    vm_stop_schedule {
      schedule = var.primary_stop_schedule
    }

    time_zone = var.primary_schedule_timezone
  }
}

resource "google_compute_resource_policy" "primary_boot_snapshot_schedule" {
  provider = google-beta

  count = var.primary_compute_vm_enabled && var.primary_snapshot_schedule_enabled ? 1 : 0

  name        = var.primary_boot_snapshot_policy_name
  region      = var.primary_region
  description = "Scheduled snapshots for primary boot disk ${var.primary_boot_disk_name}."

  snapshot_schedule_policy {
    schedule {
      # GCE daily snapshot schedules currently require days_in_cycle=1.
      daily_schedule {
        days_in_cycle = 1
        start_time    = var.primary_snapshot_start_time
      }
    }

    retention_policy {
      max_retention_days    = var.primary_snapshot_retention_days
      on_source_disk_delete = "KEEP_AUTO_SNAPSHOTS"
    }

    snapshot_properties {
      guest_flush       = false
      storage_locations = var.primary_snapshot_storage_locations
      labels = merge(
        local.base_labels,
        {
          component = "capstone-primary-boot-snapshot"
          runtime   = "primary"
        },
      )
    }
  }
}

resource "google_compute_resource_policy" "primary_data_snapshot_schedule" {
  provider = google-beta

  count = var.primary_compute_vm_enabled && var.primary_snapshot_schedule_enabled ? 1 : 0

  name        = var.primary_data_snapshot_policy_name
  region      = var.primary_region
  description = "Scheduled snapshots for primary data disk ${var.primary_data_disk_name}."

  snapshot_schedule_policy {
    schedule {
      # GCE daily snapshot schedules currently require days_in_cycle=1.
      daily_schedule {
        days_in_cycle = 1
        start_time    = var.primary_snapshot_start_time
      }
    }

    retention_policy {
      max_retention_days    = var.primary_snapshot_retention_days
      on_source_disk_delete = "KEEP_AUTO_SNAPSHOTS"
    }

    snapshot_properties {
      guest_flush       = false
      storage_locations = var.primary_snapshot_storage_locations
      labels = merge(
        local.base_labels,
        {
          component = "capstone-primary-data-snapshot"
          runtime   = "primary"
        },
      )
    }
  }
}

resource "google_compute_disk_resource_policy_attachment" "primary_boot_snapshot_schedule" {
  provider = google-beta

  count = var.primary_compute_vm_enabled && var.primary_snapshot_schedule_enabled ? 1 : 0

  name = google_compute_resource_policy.primary_boot_snapshot_schedule[0].name
  disk = google_compute_disk.primary_boot_disk[0].name
  zone = var.primary_zone
}

resource "google_compute_disk_resource_policy_attachment" "primary_data_snapshot_schedule" {
  provider = google-beta

  count = var.primary_compute_vm_enabled && var.primary_snapshot_schedule_enabled ? 1 : 0

  name = google_compute_resource_policy.primary_data_snapshot_schedule[0].name
  disk = google_compute_disk.primary_data_disk[0].name
  zone = var.primary_zone
}

resource "google_compute_instance" "primary_vm" {
  provider = google-beta

  count = var.primary_compute_vm_enabled ? 1 : 0

  name                      = var.primary_vm_name
  machine_type              = var.vm_machine_type
  zone                      = var.primary_zone
  allow_stopping_for_update = true
  deletion_protection       = false
  resource_policies = var.primary_compute_vm_enabled && var.primary_instance_schedule_enabled ? [
    google_compute_resource_policy.primary_vm_schedule[0].self_link,
  ] : []

  boot_disk {
    source      = google_compute_disk.primary_boot_disk[0].id
    auto_delete = false
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
    source      = google_compute_disk.primary_data_disk[0].id
    device_name = var.vm_data_disk_device_name
    mode        = "READ_WRITE"
  }

  metadata_startup_script = local.vm_metadata_startup_script

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
      component = "capstone-primary-vm"
      runtime   = "primary"
    },
  )
}

# A single zonal VM cannot fail over automatically across zones in Terraform. The
# fallback runtime is therefore a manual recovery scaffold, disabled by default.
resource "google_compute_disk" "recovery_boot_disk" {
  provider = google-beta

  count = var.recovery_boot_disk_enabled ? 1 : 0

  name     = var.recovery_boot_disk_name
  type     = var.vm_boot_disk_type
  zone     = var.fallback_zone
  snapshot = var.recovery_boot_source_snapshot
  labels = merge(
    local.base_labels,
    {
      component = "capstone-recovery-boot-disk"
      runtime   = "recovery"
    },
  )
}

resource "google_compute_disk" "recovery_data_disk" {
  provider = google-beta

  count = var.recovery_data_disk_enabled ? 1 : 0

  name     = var.recovery_data_disk_name
  type     = var.vm_data_disk_type
  zone     = var.fallback_zone
  snapshot = var.recovery_data_source_snapshot
  labels = merge(
    local.base_labels,
    {
      component = "capstone-recovery-data-disk"
      runtime   = "recovery"
    },
  )
}

resource "google_compute_instance" "recovery_vm" {
  provider = google-beta

  count = var.recovery_vm_enabled ? 1 : 0

  name                      = var.recovery_vm_name
  machine_type              = var.vm_machine_type
  zone                      = var.fallback_zone
  allow_stopping_for_update = true
  deletion_protection       = false

  boot_disk {
    source      = google_compute_disk.recovery_boot_disk[0].id
    auto_delete = false
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
    source      = google_compute_disk.recovery_data_disk[0].id
    device_name = var.vm_data_disk_device_name
    mode        = "READ_WRITE"
  }

  metadata_startup_script = local.vm_metadata_startup_script

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
      component = "capstone-recovery-vm"
      runtime   = "recovery"
    },
  )
}
