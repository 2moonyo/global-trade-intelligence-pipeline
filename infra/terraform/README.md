# Terraform Scaffold

This directory provisions the first cloud slice resources for the project:

- one GCS bucket for bronze and silver assets
- one BigQuery `raw` dataset
- one BigQuery `analytics` dataset
- one optional runtime service account for scheduled pipeline runs
- IAM grants for storage object admin, BigQuery dataset editors/viewers, and project-level `bigquery.jobUser`

## Auth

For local Terraform runs, use Application Default Credentials:

```bash
gcloud auth application-default login
```

The provider config does not use a service-account key file.

## First run

1. Copy the example vars file:

```bash
cp infra/terraform/terraform.tfvars.json.example infra/terraform/terraform.tfvars.json
```

2. Fill in your project id, bucket name, location, and any IAM members.

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
