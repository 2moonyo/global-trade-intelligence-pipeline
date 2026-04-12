# Terraform Scaffold

This directory provisions the first cloud slice resources for the project:

- one GCS bucket for bronze and silver assets
- one BigQuery `raw` dataset
- one BigQuery `analytics` dataset
- one optional runtime service account for scheduled pipeline runs
- one optional free-tier-friendly Compute Engine VM with a secondary persistent disk
- one optional Compute Engine instance schedule policy for VM start and stop windows
- IAM grants for storage object admin, BigQuery dataset editors/viewers, and project-level `bigquery.jobUser`

## Auth

For local Terraform runs, use Application Default Credentials:

```bash
gcloud auth application-default login
```

The provider config does not use a service-account key file.

If you want to switch to a new Google account, update both the gcloud CLI login and Application Default Credentials:

```bash
gcloud auth login
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
gcloud config set project YOUR_PROJECT_ID
```

## First run

1. Copy the example vars file:

```bash
cp infra/terraform/terraform.tfvars.json.example infra/terraform/terraform.tfvars.json
```

2. Fill in your project id, bucket name, location, and any IAM members.

   If you also want the VM, set the following values in `infra/terraform/terraform.tfvars.json`:

```json
{
  "gcp_region": "us-central1",
  "gcp_zone": "us-central1-a",
  "create_compute_vm": true,
  "vm_name": "free-tier-vm",
  "vm_machine_type": "e2-micro",
  "vm_boot_image": "debian-cloud/debian-11",
  "vm_boot_disk_size_gb": 10,
  "vm_data_disk_name": "secondary-data-disk",
  "vm_data_disk_device_name": "data-disk",
  "vm_data_disk_size_gb": 20,
  "enable_vm_instance_schedule": true,
  "vm_schedule_timezone": "UTC",
  "vm_start_schedule": "45 5 * * *",
  "vm_stop_schedule": "45 10 * * *"
}
```

   The example start schedule above wakes the VM 15 minutes before a 06:00 in-VM cron or systemd timer. Google documents that scheduled start and stop operations can take up to 15 minutes to begin, so keep the VM start schedule at least 15 minutes earlier than the first job you need to run, and keep start and stop operations at least 15 minutes apart.

3. Apply Terraform:

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

Or from the project root with the new `Makefile`:

```bash
make tfvars-init
make cloud-bootstrap
```

After apply, Terraform outputs the VM name, zone, external IP, mount point, and attached service account.

## VM notes

- The VM is attached to the Terraform-created pipeline service account by default when `create_pipeline_service_account = true`.
- The VM startup script formats and mounts the secondary persistent disk at `/mnt/disks/capstone-data` on first boot.
- The instance schedule only starts and stops the VM. Your in-VM cron or systemd timers still control when `docker compose` or Bruin commands actually run.

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

This will delete the Terraform-managed bucket contents and BigQuery dataset contents. Only enable it when you truly want a full teardown.

## Runtime config

The Python cloud scripts can read `infra/terraform/terraform.tfvars.json` directly, so `.env` is optional for them.

If you also want dbt's BigQuery target to pick up the same values, render a dotenv file from Terraform inputs:

```bash
python infra/terraform/render_dotenv.py > .env
```

That keeps Terraform as the source of truth while still satisfying tools like dbt that expect environment variables.

If you use the root `Makefile`, dbt can also be run without manually creating `.env`:

```bash
make dbt-bigquery-debug
make dbt-bigquery-build
```

The Makefile evaluates Terraform-derived environment variables in the same shell as the `dbt` command.
