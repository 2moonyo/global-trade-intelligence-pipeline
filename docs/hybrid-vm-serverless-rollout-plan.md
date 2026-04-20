# Hybrid VM + Cloud Run Rollout Plan

## Architecture Summary

The project now supports two explicit execution profiles:

- `all_vm`: default VM-first baseline. All datasets remain owned by the GCP VM, systemd timers, Docker Compose, Bruin wrappers, Python runners, GCS, BigQuery, and dbt exactly as before.
- `hybrid_vm_serverless`: Comtrade remains VM-owned; PortWatch, Brent, FX, Events, and World Bank Energy scheduled refreshes are owned by Cloud Run Jobs triggered by Cloud Scheduler.

Both profiles preserve the same warehouse contract:

```text
Ingest -> Bronze/Silver local runtime state -> GCS bronze/silver prefixes -> BigQuery raw tables -> dbt analytics marts -> Looker/dashboard outputs
```

Serverless execution is additive. It does not move Comtrade, does not introduce service-account JSON keys, and does not create serverless-only GCS or BigQuery schemas.

## Implementation Checklist

- [x] Add execution ownership config in `ops/execution_profiles.json`.
- [x] Add Python helpers for profile validation and dataset ownership lookup.
- [x] Keep VM manual dataset wrappers available by filtering only scheduled queue execution.
- [x] Add `OPS_POSTGRES_ENABLED=false` support for stateless Cloud Run Jobs while preserving VM Postgres by default.
- [x] Add Cloud Run Job wrapper script that reuses existing dataset-batch execution.
- [x] Add serverless preflight checks for runtime ownership, Events seed availability, and World Bank `dim_country` hydration.
- [x] Add best-effort upload of serverless logs/manifests to `metadata/serverless_runs/...`.
- [x] Include only `data/seed/events/events_seed_extended_2015.csv` in the pipeline image from the otherwise ignored `data/` tree.
- [x] Add additive Terraform resources for Cloud Run Jobs, Cloud Scheduler, service accounts, IAM, and env/secret injection.
- [x] Add docs and operator instructions for `all_vm`, `hybrid_vm_serverless`, validation, and rollback.

## Data Flow By Dataset

- Comtrade: VM only. Existing Comtrade batching, quota handling, bronze persistence, routing, publish/load, and dbt behavior remain unchanged.
- PortWatch: Cloud Run in hybrid mode runs existing extract -> silver -> publish GCS -> BigQuery load -> dbt build batch commands.
- Brent: Cloud Run in hybrid mode injects `FRED_API_KEY` from Secret Manager and runs existing extract -> silver -> publish/load -> dbt.
- FX: Cloud Run in hybrid mode runs existing ECB extract -> silver -> publish/load -> dbt.
- Events: Cloud Run image includes the canonical extended 2015+ seed CSV and runs existing silver -> publish/load -> dbt.
- World Bank Energy: Cloud Run preflight hydrates `data/silver/comtrade/dimensions/dim_country.parquet` from the existing Comtrade GCS contract, falling back to `raw.dim_country` in BigQuery, then runs the existing World Bank batch.

## Terraform Rollout

1. Keep `execution_profile = "all_vm"` for the first apply to prove no serverless resources are created.
2. Build and push the pipeline container image to Artifact Registry.
3. Set `execution_profile = "hybrid_vm_serverless"` and `serverless_container_image = "REGION-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG"`.
4. Keep `serverless_scheduler_paused = true` for the first hybrid apply.
5. Render/update the VM runtime env so the VM has `EXECUTION_PROFILE=hybrid_vm_serverless` and `EXECUTION_RUNTIME=vm`.
6. Validate the VM scheduled queue skips non-Comtrade batches in hybrid mode.
7. Execute one Cloud Run Job manually with `gcloud run jobs execute ... --wait`.
8. Set `serverless_scheduler_paused = false` only after manual Cloud Run validation and VM ownership validation both pass.

## Validation Checklist

Static checks:

```bash
for f in ops/batch_plan.json ops/execution_profiles.json; do python -m json.tool "$f" >/dev/null; done
python -m py_compile \
  warehouse/execution_profiles.py \
  warehouse/run_dataset_batch.py \
  warehouse/run_batch_queue.py \
  warehouse/serverless_preflight.py \
  warehouse/upload_serverless_artifacts.py
bash -n scripts/run_serverless_batch.sh scripts/run_pipeline.sh scripts/run_dbt.sh
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
```

Profile checks:

```bash
python warehouse/execution_profiles.py --profile all_vm --runtime vm --plan-path ops/batch_plan.json --output json
python warehouse/execution_profiles.py --profile hybrid_vm_serverless --runtime vm --plan-path ops/batch_plan.json --output json
python warehouse/execution_profiles.py --profile hybrid_vm_serverless --runtime cloud_run --plan-path ops/batch_plan.json --output json
```

Bruin checks:

```bash
bruin validate --fast ./bruin/pipelines/portwatch_bootstrap_phase_1/pipeline.yml
bruin validate --fast ./bruin/pipelines/brent_bootstrap_phase_1/pipeline.yml
bruin validate --fast ./bruin/pipelines/fx_bootstrap_phase_1/pipeline.yml
bruin validate --fast ./bruin/pipelines/worldbank_energy_bootstrap_full/pipeline.yml
```

Terraform plan checks:

```bash
terraform -chdir=infra/terraform plan -var='execution_profile=all_vm'
terraform -chdir=infra/terraform plan \
  -var='execution_profile=hybrid_vm_serverless' \
  -var='serverless_container_image=REGION-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG'
```

Runtime smoke checks:

```bash
gcloud run jobs execute capstone-events-incremental --region REGION --wait
bq ls PROJECT:raw
scripts/run_dbt.sh parse
```

Confirm that raw tables remain the tables listed in `models/sources/silver_sources.yml` and that dbt marts build against the same source names.

## Rollback

- Set `execution_profile = "all_vm"` and apply Terraform to remove Cloud Run/Scheduler resources from ownership.
- If immediate action is needed, pause Cloud Scheduler jobs first.
- Render/update `/etc/capstone/pipeline.env` with `EXECUTION_PROFILE=all_vm` and `EXECUTION_RUNTIME=vm`.
- Re-enable VM timers for non-Comtrade lanes if they were operationally disabled.
- No GCS or BigQuery migration is required because hybrid mode writes to the same contract paths and tables.

## Risks And Controls

- Duplicate scheduling: controlled by VM queue profile filtering and initially paused Cloud Scheduler jobs.
- Postgres coupling: Cloud Run sets `OPS_POSTGRES_ENABLED=false`; BigQuery ops mirror remains enabled.
- Events seed availability: Docker image includes only the approved extended 2015+ seed CSV from `data/`.
- World Bank dependency on Comtrade countries: preflight hydrates `dim_country.parquet` from existing Comtrade outputs.
- dbt concurrency: schedules are staggered and can remain paused during rollout.
