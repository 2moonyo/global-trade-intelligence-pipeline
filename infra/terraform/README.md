# Terraform Scaffold

This directory provisions the shared cloud resources plus a configurable Compute Engine runtime layout:

- one GCS bucket for bronze and silver assets
- one BigQuery `raw` dataset
- one BigQuery `analytics` dataset
- one user-managed VM runtime service account
- one optional primary VM runtime in Europe with a separate boot disk and persistent data disk
- one optional legacy US VM runtime kept temporarily during migration
- one optional fallback recovery runtime in `europe-west1-d`
- one optional primary instance schedule policy
- two optional scheduled snapshot policies for the primary boot and data disks
- IAM grants for bucket object admin, dataset editors/viewers, and project-level `bigquery.jobUser`

## Execution modes

- Local Docker: the lightweight development path that uses local ADC from `gcloud auth application-default login`.
- GCE VM runtime: the supported cloud-hosted runtime that uses metadata-based ADC from the attached service account, a persistent disk, systemd timers, and an instance schedule.
- Manual fallback recovery: a disabled-by-default Terraform path that can recreate the VM in `europe-west1-d` from explicit boot and data snapshots.

## Local Terraform auth

Run Terraform from your laptop with Application Default Credentials:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

The Terraform provider does not use a service-account key file.

## Why the VM runtime is keyless

- Terraform creates a user-managed service account and attaches it directly to VM instances.
- Each VM uses `cloud-platform` scope so Google client libraries can ask the metadata server for short-lived access tokens at runtime.
- The startup script deliberately does not set `GOOGLE_APPLICATION_CREDENTIALS`.
- `GOOGLE_AUTH_MODE=vm_metadata` tells the containers to ignore mounted key files and rely on metadata-based ADC instead.

This keeps GCP auth aligned with org policies that disallow JSON keys while staying easy to explain in an interview: the VM identity is the service account, and the metadata server brokers credentials on demand.

## Fresh-account deploy

1. Copy the example vars file:

```bash
cp infra/terraform/terraform.tfvars.json.example infra/terraform/terraform.tfvars.json
```

2. Fill in your project id, bucket name, location, and IAM members.

3. The default example creates a Europe-first primary runtime from scratch:

```json
{
  "primary_region": "europe-west1",
  "primary_zone": "europe-west1-b",
  "primary_compute_vm_enabled": true,
  "primary_boot_restore_from_snapshot": false,
  "primary_data_restore_from_snapshot": false,
  "legacy_compute_vm_enabled": false,
  "recovery_boot_disk_enabled": false,
  "recovery_data_disk_enabled": false,
  "recovery_vm_enabled": false
}
```

4. Apply Terraform:

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

This creates the bucket, datasets, IAM, the primary Europe VM, the primary boot and data disks, and scheduled snapshots for those two disks.

## Migration deploy

For the current project migration:

- keep `legacy_compute_vm_enabled = true`
- set `primary_boot_restore_from_snapshot = true`
- set `primary_data_restore_from_snapshot = true`
- set `primary_boot_source_snapshot` and `primary_data_source_snapshot` to explicit snapshot names

Use a stopped-point boot snapshot and a stopped-point data snapshot taken as close together as possible. The current repo tfvars are prepared for that pattern but should be updated to the freshest snapshots before the cutover apply.

## Primary, legacy, and fallback behavior

- The primary runtime is the default operating environment and lives in `europe-west1-b`.
- The legacy runtime remains on the existing Terraform resource addresses so it can be preserved during migration and removed in a later apply.
- The fallback runtime in `europe-west1-d` is manual recovery only. Terraform does not attempt automatic failover for a single zonal VM.
- Fallback creation is split into separate controls for boot disk, data disk, and VM so you can stage recovery safely.

## Snapshot schedules

- Snapshot schedules apply to disks, not the instance.
- The primary boot disk and primary data disk each get their own scheduled snapshot policy.
- Defaults:
  - every 72 hours
  - 7-day retention
  - storage location `europe-west1`

This keeps recent backups available without letting scheduled snapshots accumulate indefinitely.

## VM operator flow

The startup script prepares the VM, installs Docker, mounts the persistent disk at `/var/lib/pipeline`, provisions swap on that disk, writes systemd units, and enables `capstone-stack.service`.

After the first SSH:

1. Copy the repository bundle to `/var/lib/pipeline/capstone`.
2. Create the root-owned env file from `ops/vm/pipeline.env.example`.
3. Start the stack:

```bash
sudo systemctl start capstone-stack
```

4. Enable the schedule lane timers you want:

```bash
sudo systemctl enable --now capstone-schedule-lane-incremental_daily.timer
sudo systemctl enable --now capstone-schedule-lane-weekly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-monthly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-yearly_refresh.timer
```

The timers call the existing `schedule_lane_queue` wrapper inside the orchestrator container, so the VM path reuses the same batch plan, retry, checkpoint, and Postgres ops ledger logic as local runs.

## First manual pipeline run

For the first VM smoke test, initialize the ops stores and start with the non-Comtrade bootstrap batches only:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh ops-init-all
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh bootstrap-non-comtrade
```

World Bank energy is intentionally excluded from that lane because its extractor reads the country universe from Comtrade's `dim_country` outputs. Run it only after the Comtrade bootstrap has completed:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh country-trade-and-energy
```

## Runtime lifecycle helpers

The repo includes [scripts/vm_runtime_ctl.sh](/Users/chromazone/Documents/Python/Data%20Enginering%20Zoomcamp/Capstone_monthly/scripts/vm_runtime_ctl.sh), which now understands `primary`, `legacy`, and `recovery` targets.

Examples:

```bash
make vm-status
scripts/vm_runtime_ctl.sh status primary
scripts/vm_runtime_ctl.sh status legacy
scripts/vm_runtime_ctl.sh start primary
scripts/vm_runtime_ctl.sh stop legacy
scripts/vm_runtime_ctl.sh destroy-compute-terraform legacy
```

Use the Terraform destroy path for staged decommission so snapshot schedules and boot/data disk lifecycles remain explicit.

## Staged migration sequence

1. Keep the legacy US runtime enabled.
2. Take fresh boot and data snapshots from the stopped legacy VM.
3. Update tfvars with the fresh snapshot names.
4. Apply Terraform to create the Europe primary runtime and primary snapshot schedules.
5. Validate mount path, service account ADC, Docker Compose, and schedule timers on the Europe VM.
6. Disable the legacy runtime and apply again to remove only the old US VM, old US data disk, and old US schedule.
7. Keep snapshots needed for rollback and recovery.

## Destroy

Destroy is intentionally guarded.

To allow Terraform to delete a non-empty bucket and BigQuery datasets that still contain tables, first set:

```json
"allow_force_destroy": true
```

in `infra/terraform/terraform.tfvars.json`, then run:

```bash
make infra-destroy
```

Only enable this when you truly want a full teardown.
