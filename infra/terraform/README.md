# Terraform Scaffold

This directory provisions the shared cloud resources and the optional VM runtime:

- one GCS bucket for bronze and silver assets
- one BigQuery `raw` dataset
- one BigQuery `analytics` dataset
- one user-managed VM runtime service account
- one optional free-tier-friendly Compute Engine VM with a secondary persistent disk
- one optional Compute Engine instance schedule policy for VM start and stop windows
- IAM grants for bucket object admin, dataset editors/viewers, and project-level `bigquery.jobUser`

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

- Terraform creates a user-managed service account and attaches it directly to the VM.
- The VM uses `cloud-platform` scope so Google client libraries can ask the metadata server for short-lived access tokens at runtime.
- The startup script deliberately does not set `GOOGLE_APPLICATION_CREDENTIALS`.
- `GOOGLE_AUTH_MODE=vm_metadata` tells the containers to ignore mounted key files and rely on metadata-based ADC instead.

This keeps GCP auth aligned with org policies that disallow JSON keys while staying easy to explain in an interview: the VM identity is the service account, and the metadata server brokers credentials on demand.

## First run

1. Copy the example vars file:

```bash
cp infra/terraform/terraform.tfvars.json.example infra/terraform/terraform.tfvars.json
```

2. Fill in your project id, bucket name, location, and IAM members. For the VM path, the important defaults are already set:

```json
{
  "create_compute_vm": true,
  "vm_name": "free-tier-vm",
  "vm_machine_type": "e2-micro",
  "vm_boot_disk_size_gb": 18,
  "vm_data_disk_size_gb": 12,
  "vm_data_mount_point": "/var/lib/pipeline",
  "vm_repo_root": "/var/lib/pipeline/capstone",
  "vm_env_file_path": "/etc/capstone/pipeline.env"
}
```

3. Apply Terraform:

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

After apply, Terraform outputs the VM name, zone, external IP, mount point, repo root, env file path, and attached service account.

## VM operator flow

The startup script prepares the VM, installs Docker, mounts the persistent disk at `/var/lib/pipeline`, writes systemd units, and enables `capstone-stack.service`.

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

## Instance schedule vs. in-VM timers

- The optional Compute Engine instance schedule only starts and stops the VM.
- The systemd timers on the VM decide when Bruin schedule lanes run.
- Keep the VM start schedule at least 15 minutes earlier than the first timer you enable, and keep start and stop operations at least 15 minutes apart.

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
