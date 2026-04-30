# Macro-Political Trade Intelligence Pipeline

This repository packages a portfolio MVP data pipeline for studying how trade, chokepoints, macro signals, energy dependence, and disruptive events interact over time.

In plain language: the project collects public and API-based data, stores it in cloud storage, cleans it into analytical tables, and builds dashboard-ready models that help answer questions like:

- which countries are exposed to specific maritime chokepoints
- how trade flows changed around shocks such as the Red Sea attacks or Panama Canal drought
- how commodity dependence, oil prices, FX, and energy vulnerability add context to trade risk
- where the data is strong, sparse, delayed, or too expensive to obtain in real time

In technical language: the baseline system runs on a GCP VM with Docker Compose, Bruin CLI orchestration, GCS, BigQuery, dbt, optional Cloud Run Jobs for bounded non-Comtrade refreshes, and Looker/BI-ready marts.

![Executive data flow diagram](<docs/ER Diagrams and flow/1.Capstone_Executive_Data_Flow-Executive Flow.drawio.png>)

## TL;DR Quick VM Setup

Use this path when you want the fastest repeatable route from a new GCP project to a running VM pipeline.

Prerequisites on your laptop:

- Google Cloud SDK with `gcloud`
- Terraform
- `make`
- `uv`
- a GCP project with billing enabled
- FRED and UN Comtrade API keys

### 1. Create The GCP Project Context

Create or select a GCP project, then authenticate locally:

```bash
gcloud auth login
gcloud config configurations create capstone-vm
gcloud config configurations activate capstone-vm
gcloud config set account YOUR_EMAIL
gcloud config set project YOUR_GCP_PROJECT_ID
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_GCP_PROJECT_ID
```

Enable the baseline APIs:

```bash
gcloud services enable \
  serviceusage.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com
```

### 2. Create Terraform Inputs

Generate the editable Terraform values file:

```bash
make tfvars-init
```

Edit `infra/terraform/terraform.tfvars.json` and set at least:

- `project_id`
- `gcp_location`
- `primary_region`
- `primary_zone`
- `gcs_bucket_name`
- IAM member lists for the human/operator accounts that need access

For the current VM-first baseline, keep:

```json
{
  "execution_profile": "all_vm",
  "legacy_compute_vm_enabled": false,
  "primary_compute_vm_enabled": true,
  "primary_boot_restore_from_snapshot": false,
  "primary_data_restore_from_snapshot": false,
  "recovery_boot_disk_enabled": false,
  "recovery_data_disk_enabled": false,
  "recovery_vm_enabled": false
}
```

### 3. Create `.env` And Fill Secrets

Create local runtime files and `.env`:

```bash
./scripts/bootstrap_local.sh
```

Fill the required values in `.env`:

```bash
FRED_API_KEY=your_fred_key
COMTRADE_API_KEY_DATA=your_primary_comtrade_key
COMTRADE_API_KEY_DATA_A=
COMTRADE_API_KEY_DATA_B=
POSTGRES_USER=capstone
POSTGRES_PASSWORD=choose_a_password
POSTGRES_DB=capstone
EXECUTION_PROFILE=all_vm
EXECUTION_RUNTIME=vm
```

Do not commit `.env` or real API keys. The bootstrap path uses this file only to seed the approved secret flow into Google Secret Manager and the VM runtime env.

### 4. Run The Simple VM Bootstrap

From your laptop:

```bash
make vm-bootstrap VM_BOOTSTRAP_ARGS="--reset-known-host --show-resolved"
```

`make vm-bootstrap` is the simple wrapper around `scripts/vm_bootstrap.sh`. That command:

- applies Terraform
- syncs approved `.env` secrets to Google Secret Manager
- copies the repo to `/var/lib/pipeline/capstone` on the VM
- renders `/etc/capstone/pipeline.env`
- starts the Docker Compose stack through `capstone-stack`

### 5. Trigger The VM Pipeline Scripts

SSH to the VM and run the full first-time bootstrap:

```bash
gcloud compute ssh capstone-vm-eu --zone europe-west1-b
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_full_bootstrap.sh
```

If you want to run the same sequence step by step, use:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_noncomtrade_phase_1_all.sh
./scripts/vm_batches/run_noncomtrade_phase_2_all.sh
./scripts/vm_batches/run_comtrade_all_days.sh
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline scripts/run_pipeline.sh country-trade-and-energy
```

Check the stack and recent logs:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml ps
tail -n 20 logs/comtrade/comtrade_extract_manifest.jsonl
tail -n 20 logs/portwatch/portwatch_extract_manifest.jsonl
```

After the bootstrap succeeds, enable recurring timers:

```bash
sudo systemctl enable --now capstone-schedule-lane-incremental_daily.timer
sudo systemctl enable --now capstone-schedule-lane-weekly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-monthly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-yearly_refresh.timer
systemctl list-timers 'capstone-schedule-lane-*'
```

## Recommended Path

Use the VM path first.

The VM path is the operational baseline because it preserves:

- persistent disk state for large Comtrade extracts
- retry/checkpoint files
- local logs and manifests
- Docker Compose runtime parity
- shell wrappers that still contain useful compatibility logic
- a safer recovery story for long-running data collection

Cloud Run is additive. In the hybrid profile, it is used for bounded non-Comtrade scheduled refreshes. Comtrade stays on the VM because quota handling, large bronze state, and rerun recovery are more important than stateless elegance.

![VM internal architecture diagram](<docs/ER Diagrams and flow/3.Capstone_Executive_Data_Flow-VM_Internal Architecture.drawio.png>)

## Architecture

The pipeline shape is:

```text
Ingest -> Bronze files -> Silver parquet -> GCS -> BigQuery raw -> dbt marts -> BI/dashboard
```

The main dataset families are:

| Dataset | Role | Notes |
| --- | --- | --- |
| Comtrade | Core trade fact data | Reporter, partner, commodity, month, and flow. This is the backbone of the project. |
| PortWatch | Chokepoint traffic signal | Used for maritime stress and event impact context. Coverage can be sparse. |
| Brent / WTI | Oil price context | Pulled through FRED and used as a global macro signal. |
| FX | Currency context | ECB monthly FX features used in macro marts. |
| Events | Curated disruption layer | Manual event register converted into event-month-location bridges. |
| World Bank Energy | Structural energy vulnerability | Annual country energy indicators broadcast to month grain for context. |

The warehouse is not a pure in-database medallion layout. Bronze and silver live mainly as files; BigQuery `raw` is a landing layer for curated silver outputs; dbt creates staging, dimensions, facts, and marts.

## External Setup Links

Use these official pages for setup details:

- GCP project creation: https://cloud.google.com/resource-manager/docs/creating-managing-projects
- GCP API Library / enabling APIs: https://cloud.google.com/apis/docs/getting-started
- FRED API key docs: https://fred.stlouisfed.org/docs/api/api_key.html
- UN Comtrade developer portal: https://comtradedeveloper.un.org/apis
- UN Comtrade Python package and metadata reference examples: https://github.com/uncomtrade/comtradeapicall
- Cloud Run job execution and argument overrides: https://cloud.google.com/run/docs/execute/jobs

## What You Need Before Setup

For a first-time VM deployment, prepare:

| Item | Where it comes from | Where it goes |
| --- | --- | --- |
| GCP project ID | GCP Console project selector | `infra/terraform/terraform.tfvars.json` as `project_id` |
| GCP region/location | Your chosen deployment region | `terraform.tfvars.json` as `gcp_location`, `primary_region`, `primary_zone` |
| GCS bucket name | Your chosen globally unique bucket name | `terraform.tfvars.json` as `gcs_bucket_name` |
| FRED API key | FRED account API key page | `.env` as `FRED_API_KEY` |
| Comtrade API keys | UN Comtrade developer portal | `.env` as `COMTRADE_API_KEY_DATA`, optional `_A`, `_B` |
| Postgres runtime values | Chosen by project operator | `.env`, then Secret Manager and `/etc/capstone/pipeline.env` |
| Runtime VM env | Rendered by bootstrap | `/etc/capstone/pipeline.env` on the VM |

Do not commit real `.env` files or API keys.

## GCP UI Setup For Non-Technical Users

1. Open the Google Cloud Console.
2. Open the project selector and choose `Create project`.
3. Give the project a human-readable name.
4. Choose a project ID carefully. GCP project IDs are globally unique and are difficult to change later.
5. Link billing if the project asks for it. Compute Engine, BigQuery, Cloud Run, Artifact Registry, and logging can create costs.
6. Open `APIs & Services -> Library`.
7. Enable the baseline APIs:

```bash
gcloud services enable \
  serviceusage.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com
```

For hybrid serverless, also enable:

```bash
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com
```

The command-line setup is preferred once the project exists, because it is repeatable and less error-prone than clicking through every service.

## API Keys

### FRED

Create or view a FRED key from the FRED API key page:

https://fred.stlouisfed.org/docs/api/api_key.html

Put the key in `.env`:

```bash
FRED_API_KEY=your_fred_key_here
```

FRED is used for Brent and WTI oil price series. It keeps the project cheap, but it is not the same as paying for a low-latency real-time markets feed.

### UN Comtrade

Create a subscription key in the UN Comtrade developer portal. Put the primary key in:

```bash
COMTRADE_API_KEY_DATA=your_primary_comtrade_key
```

If you have additional valid keys, add:

```bash
COMTRADE_API_KEY_DATA_A=your_second_key
COMTRADE_API_KEY_DATA_B=your_third_key
```

The extraction script can rotate through configured aliases when quota or throttling is encountered. This does not remove the quota problem; it only makes long runs easier to resume.

## Secret Flow

The project uses one secret propagation chain:

```text
local .env -> Google Secret Manager -> /etc/capstone/pipeline.env -> Docker runtime env
```

Rules:

- `.env` is for local setup and seeding Secret Manager.
- `/etc/capstone/pipeline.env` is the VM runtime file.
- Secret Manager is the cloud source of truth for approved runtime secrets.
- Do not create a second secret mechanism.
- Do not put a service-account JSON key on the VM.

The VM uses the attached GCP service account through metadata-based Application Default Credentials. In `pipeline.env`, keep:

```bash
GOOGLE_AUTH_MODE=vm_metadata
GOOGLE_APPLICATION_CREDENTIALS=
```

## First-Time VM Setup

Run this from your local machine.

```bash
gcloud auth login
gcloud config configurations create capstone-new-account
gcloud config configurations activate capstone-new-account
gcloud config set account YOUR_EMAIL
gcloud config set project NEW_PROJECT_ID
gcloud auth application-default login
gcloud auth application-default set-quota-project NEW_PROJECT_ID
```

Create Terraform inputs:

```bash
make tfvars-init
```

Edit `infra/terraform/terraform.tfvars.json`. At minimum set:

- `project_id`
- `gcp_location`
- `primary_region`
- `primary_zone`
- `gcs_bucket_name`
- IAM member lists for the human/operator accounts that need access

For the current Europe-first VM path, the important shape is:

```json
{
  "gcp_location": "europe-west1",
  "primary_region": "europe-west1",
  "primary_zone": "europe-west1-b",
  "legacy_compute_vm_enabled": false,
  "primary_compute_vm_enabled": true,
  "primary_boot_restore_from_snapshot": false,
  "primary_data_restore_from_snapshot": false,
  "recovery_boot_disk_enabled": false,
  "recovery_data_disk_enabled": false,
  "recovery_vm_enabled": false
}
```

Create local `.env`:

```bash
./scripts/bootstrap_local.sh
```

Fill the required secret values in `.env`, then bootstrap the VM:

```bash
make vm-bootstrap VM_BOOTSTRAP_ARGS="--reset-known-host --show-resolved"
```

That command applies Terraform, syncs approved secrets to Secret Manager, copies the repo to the VM at `/var/lib/pipeline/capstone`, renders `/etc/capstone/pipeline.env`, and starts the Docker Compose stack.

SSH to the VM:

```bash
gcloud compute ssh capstone-vm-eu --zone europe-west1-b
```

Run the full first-time bootstrap:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_full_bootstrap.sh
```

That runs:

1. Non-Comtrade phase 1
2. Non-Comtrade phase 2
3. Comtrade day 1 through day 6
4. World Bank energy after Comtrade has created the country dimension

Enable recurring timers only after the first bootstrap succeeds:

```bash
sudo systemctl enable --now capstone-schedule-lane-incremental_daily.timer
sudo systemctl enable --now capstone-schedule-lane-weekly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-monthly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-yearly_refresh.timer
systemctl list-timers 'capstone-schedule-lane-*'
```

## VM Repository on the Persistent Disk

The VM uses the persistent disk for both runtime state and the repository checkout.

| Path | Meaning |
| --- | --- |
| `/var/lib/pipeline` | Mounted persistent disk root. This survives normal VM stop/start cycles. |
| `/var/lib/pipeline/capstone` | Repo root used by VM scripts, systemd services, Docker Compose, and manual runs. |
| `/var/lib/pipeline/capstone/data` | Local bronze/silver runtime data mounted into containers as `/workspace/data`. |
| `/var/lib/pipeline/capstone/logs` | Local runtime logs mounted into containers as `/workspace/logs`. |
| `/var/lib/pipeline/capstone/runtime/postgres` | Optional local Postgres state for run metadata. |

The first-time command below is the simplest way to put the repo on the persistent disk:

```bash
make vm-bootstrap VM_BOOTSTRAP_ARGS="--reset-known-host --show-resolved"
```

By default, `vm-bootstrap` uses the copy transfer path. It packages your local repo, copies it to `/var/lib/pipeline/capstone`, renders `/etc/capstone/pipeline.env`, and restarts `capstone-stack`.

The copy path is useful because the VM does not need GitHub access. It also excludes local `.env`, secrets, `data`, `logs`, `runtime`, `target`, and cache folders so local development state does not overwrite persistent VM runtime state.

### Copy Local Changes to the VM

Use this when you have local edits that are not pushed to Git yet, or when you want the fastest way to refresh the VM from your laptop.

First collect the VM connection values. You can use the `VM_USER` and `VM_HOST` printed by `make vm-bootstrap VM_BOOTSTRAP_ARGS="--show-resolved"`, or get the host from GCP:

```bash
export VM_USER=your-vm-linux-user
export VM_HOST="$(gcloud compute instances describe capstone-vm-eu \
  --zone europe-west1-b \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
```

Then copy the repo:

```bash
scripts/vm_repo_copy.sh \
  --vm-user "$VM_USER" \
  --vm-host "$VM_HOST" \
  --ssh-key-path "$HOME/.ssh/google_compute_engine" \
  --vm-repo-dir /var/lib/pipeline/capstone
```

Restart the stack so the Docker images rebuild with the copied source:

```bash
ssh -i "$HOME/.ssh/google_compute_engine" "$VM_USER@$VM_HOST"
cd /var/lib/pipeline/capstone
sudo systemctl restart capstone-stack
sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml ps
```

This restart matters. The containers run from images built from the repo source, while `data`, `logs`, `target`, and a few runtime folders are mounted as volumes. Updating files under `/var/lib/pipeline/capstone` alone does not update code inside the running `pipeline`, `dbt`, or `orchestrator` containers until the stack rebuilds/restarts.

### Pull Pushed Changes onto the VM

Use this when changes are committed and pushed to a Git remote, and the VM has access to that remote. For private repos, configure a deploy key or another GitHub access method first.

```bash
scripts/vm_repo_sync.sh \
  --vm-user "$VM_USER" \
  --vm-host "$VM_HOST" \
  --ssh-key-path "$HOME/.ssh/google_compute_engine" \
  --vm-repo-dir /var/lib/pipeline/capstone \
  --repo-url git@github.com:YOUR_ORG/YOUR_REPO.git \
  --branch main
```

To deploy a specific commit instead of the branch head:

```bash
scripts/vm_repo_sync.sh \
  --vm-user "$VM_USER" \
  --vm-host "$VM_HOST" \
  --ssh-key-path "$HOME/.ssh/google_compute_engine" \
  --vm-repo-dir /var/lib/pipeline/capstone \
  --repo-url git@github.com:YOUR_ORG/YOUR_REPO.git \
  --branch main \
  --commit COMMIT_SHA
```

After a Git sync, restart the stack in the same way:

```bash
ssh -i "$HOME/.ssh/google_compute_engine" "$VM_USER@$VM_HOST"
cd /var/lib/pipeline/capstone
sudo systemctl restart capstone-stack
```

Prefer the copy path for early setup and non-technical handoff because it has fewer moving parts. Prefer the Git sync path once the repo is published and operators are deploying reviewed commits.

## Command Context Headers

Use these context blocks before the run commands below.

### Local Machine Header

Use this for development, dry runs, or small manual runs from your laptop:

```bash
cd /path/to/Capstone_monthly
uv sync
gcloud auth application-default login
set -a
source .env
set +a
export GOOGLE_AUTH_MODE=auto
export EXECUTION_PROFILE="${EXECUTION_PROFILE:-all_vm}"
export EXECUTION_RUNTIME="${EXECUTION_RUNTIME:-vm}"
export BATCH_PLAN_PATH="${BATCH_PLAN_PATH:-ops/batch_plan.json}"
```

Then use the universal local batch command:

```bash
scripts/run_pipeline.sh dataset-batch DATASET_NAME BATCH_ID \
  --trigger-type manual \
  --bruin-pipeline-name local.manual.BATCH_ID
```

Local runs are useful for development. They are not recommended for full Comtrade bootstrap unless you intentionally want large local bronze and silver state on your laptop.

### VM Repo Header

Use this after SSHing to the VM:

```bash
cd /var/lib/pipeline/capstone
```

Preferred VM runs use wrapper scripts such as:

```bash
./scripts/vm_batches/run_set.sh comtrade-day-1
```

The wrapper starts the Docker stack if needed, initializes ops tables, injects `/etc/capstone/pipeline.env`, and runs the dataset batch inside the `pipeline` container.

The universal VM batch command is:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline \
  scripts/run_pipeline.sh dataset-batch DATASET_NAME BATCH_ID \
  --trigger-type manual \
  --bruin-pipeline-name vm.manual.BATCH_ID
```

To resume from a failed step:

```bash
./scripts/vm_batches/run_set.sh comtrade-day-2 --start-at-task silver
```

or:

```bash
./scripts/vm_batches/run_set.sh comtrade-day-2 --start-at-step-order 3
```

### Cloud Run Header

Use this from your local machine after hybrid Cloud Run resources exist:

```bash
export PROJECT_ID=your-gcp-project-id
export REGION=europe-west1
gcloud config set project "$PROJECT_ID"
```

Cloud Run Jobs already use `/workspace/scripts/run_serverless_batch.sh`. You can execute an existing job with its default args:

```bash
gcloud run jobs execute capstone-portwatch-weekly --region "$REGION" --wait
```

You can also override the args for a one-off non-Comtrade batch without changing the Terraform default:

```bash
gcloud run jobs execute capstone-portwatch-weekly \
  --region "$REGION" \
  --args=portwatch,portwatch_bootstrap_phase_1 \
  --task-timeout=7200 \
  --wait
```

Comtrade is intentionally not run this way.

### Bruin Manual Header

Bruin is used to expose pipeline stages and lineage. It does not replace the VM wrappers yet.

Local validation:

```bash
bruin validate --fast --env default ./bruin/pipelines/portwatch_bootstrap_phase_1/pipeline.yml
```

Local run:

```bash
bruin run --environment default ./bruin/pipelines/portwatch_bootstrap_phase_1
```

Generic Bruin dataset batch run:

```bash
DATASET_NAME=portwatch BATCH_ID=portwatch_bootstrap_phase_1 \
bruin run --environment default ./bruin/pipelines/dataset_batch
```

VM Bruin run through the orchestrator container:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T \
  -e DATASET_NAME=portwatch \
  -e BATCH_ID=portwatch_bootstrap_phase_1 \
  orchestrator \
  bruin run --environment production --force ./bruin/pipelines/dataset_batch
```

If Bruin reports `no git repository found` inside the container, initialize a temporary git root in `/workspace` before the proof run:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T orchestrator git init /workspace
```

## Scheduling Logic

There are several scheduling layers.

| Layer | Where it lives | What it does |
| --- | --- | --- |
| VM systemd timers | `infra/terraform/variables.tf` as `vm_schedule_lane_timers` | Defines timer cadence for `incremental_daily`, `weekly_refresh`, `monthly_refresh`, and `yearly_refresh`. |
| VM timer service | `infra/terraform/templates/vm_startup.sh.tftpl` | Writes `capstone-schedule-lane@.service` and timer units on the VM. |
| Batch queue | `warehouse/run_batch_queue.py` and `ops/batch_plan.json` | Selects batches by `schedule_lane`, dependencies, retry state, and execution profile. |
| Hybrid Cloud Scheduler | `infra/terraform/serverless.tf` and `serverless_scheduled_batches` | Creates Cloud Scheduler jobs that execute Cloud Run Jobs for non-Comtrade refreshes. |
| Runtime ownership | `ops/execution_profiles.json` | In `all_vm`, all datasets are VM-owned. In `hybrid_vm_serverless`, Comtrade is VM-owned and non-Comtrade refreshes are Cloud Run-owned. |

Default VM timer cadence:

| Schedule lane | Timer unit | Default cadence |
| --- | --- | --- |
| `incremental_daily` | `capstone-schedule-lane-incremental_daily.timer` | Daily at 06:00 UTC |
| `weekly_refresh` | `capstone-schedule-lane-weekly_refresh.timer` | Monday at 06:15 UTC |
| `monthly_refresh` | `capstone-schedule-lane-monthly_refresh.timer` | First day of month at 06:30 UTC |
| `yearly_refresh` | `capstone-schedule-lane-yearly_refresh.timer` | January 1 at 06:45 UTC |

Bootstrap lanes are normally manual. If you want bootstrap timers, add them explicitly to `vm_schedule_lane_timers` and re-apply Terraform.

## Dataset Run Commands

### Full VM Bootstrap

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_full_bootstrap.sh
```

Inspect the live VM container logs:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml logs --tail=200 pipeline
```

### Comtrade Runs

Comtrade is VM-only. Do not run Comtrade on Cloud Run unless you deliberately redesign its state, quota, and recovery model.

| Batch | VM manual run | VM log check | Serverless |
| --- | --- | --- | --- |
| `comtrade_bootstrap_day_1` | `./scripts/vm_batches/run_set.sh comtrade-day-1` | `tail -n 80 logs/comtrade/comtrade_history_day_1.log` | VM only |
| `comtrade_bootstrap_day_2` | `./scripts/vm_batches/run_set.sh comtrade-day-2` | `tail -n 80 logs/comtrade/comtrade_history_day_2.log` | VM only |
| `comtrade_bootstrap_day_3` | `./scripts/vm_batches/run_set.sh comtrade-day-3` | `tail -n 80 logs/comtrade/comtrade_history_day_3.log` | VM only |
| `comtrade_bootstrap_day_4` | `./scripts/vm_batches/run_set.sh comtrade-day-4` | `tail -n 80 logs/comtrade/comtrade_history_day_4.log` | VM only |
| `comtrade_bootstrap_day_5` | `./scripts/vm_batches/run_set.sh comtrade-day-5` | `tail -n 80 logs/comtrade/comtrade_history_day_5.log` | VM only |
| `comtrade_bootstrap_day_6` | `./scripts/vm_batches/run_set.sh comtrade-day-6` | `tail -n 80 logs/comtrade/comtrade_history_day_6.log` | VM only |
| `comtrade_monthly_refresh` | `sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh dataset-batch comtrade comtrade_monthly_refresh --trigger-type manual --bruin-pipeline-name vm.manual.comtrade_monthly_refresh` | `tail -n 80 logs/comtrade/comtrade_history_monthly_refresh.log` | VM only |

Useful Comtrade manifest checks:

```bash
tail -n 20 logs/comtrade/comtrade_extract_manifest.jsonl
tail -n 20 logs/comtrade/comtrade_silver_manifest.jsonl
tail -n 20 logs/comtrade/load_comtrade_to_bigquery_manifest.jsonl
```

### Non-Comtrade VM And Cloud Run Commands

For Cloud Run log checks, replace `PROJECT_ID` and `REGION` first:

```bash
export PROJECT_ID=your-gcp-project-id
export REGION=europe-west1
```

| Batch | VM manual run | VM log check | Cloud Run manual run | Cloud Run log check |
| --- | --- | --- | --- | --- |
| `portwatch_bootstrap_phase_1` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-1-portwatch` | `tail -n 80 logs/portwatch/portwatch_extract_phase_1.log` | `gcloud run jobs execute capstone-portwatch-weekly --region "$REGION" --args=portwatch,portwatch_bootstrap_phase_1 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-portwatch-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `brent_bootstrap_phase_1` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-1-brent` | `tail -n 20 logs/brent/brent_extract_manifest.jsonl` | `gcloud run jobs execute capstone-brent-weekly --region "$REGION" --args=brent,brent_bootstrap_phase_1 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-brent-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `fx_bootstrap_phase_1` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-1-fx` | `tail -n 20 logs/fx/fx_extract_manifest.jsonl` | `gcloud run jobs execute capstone-fx-weekly --region "$REGION" --args=fx,fx_bootstrap_phase_1 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-fx-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `events_bootstrap_phase_1` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-1-events` | `tail -n 20 logs/events/events_silver_manifest.jsonl` | `gcloud run jobs execute capstone-events-incremental --region "$REGION" --args=events,events_bootstrap_phase_1 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-events-incremental"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `portwatch_bootstrap_phase_2` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-2-portwatch` | `tail -n 80 logs/portwatch/portwatch_extract_phase_2.log` | `gcloud run jobs execute capstone-portwatch-weekly --region "$REGION" --args=portwatch,portwatch_bootstrap_phase_2 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-portwatch-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `brent_bootstrap_phase_2` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-2-brent` | `tail -n 20 logs/brent/brent_extract_manifest.jsonl` | `gcloud run jobs execute capstone-brent-weekly --region "$REGION" --args=brent,brent_bootstrap_phase_2 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-brent-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `fx_bootstrap_phase_2` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-2-fx` | `tail -n 20 logs/fx/fx_extract_manifest.jsonl` | `gcloud run jobs execute capstone-fx-weekly --region "$REGION" --args=fx,fx_bootstrap_phase_2 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-fx-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `events_bootstrap_phase_2` | `./scripts/vm_batches/run_set.sh noncomtrade-phase-2-events` | `tail -n 20 logs/events/events_silver_manifest.jsonl` | `gcloud run jobs execute capstone-events-incremental --region "$REGION" --args=events,events_bootstrap_phase_2 --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-events-incremental"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `worldbank_energy_bootstrap_full` | `sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh dataset-batch worldbank_energy worldbank_energy_bootstrap_full --trigger-type manual --bruin-pipeline-name vm.manual.worldbank_energy_bootstrap_full` | `tail -n 20 logs/worldbank_energy/worldbank_energy_extract_manifest.jsonl` | `gcloud run jobs execute capstone-worldbank-energy-yearly --region "$REGION" --args=worldbank_energy,worldbank_energy_bootstrap_full --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-worldbank-energy-yearly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `portwatch_weekly_refresh` | `sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh dataset-batch portwatch portwatch_weekly_refresh --trigger-type manual --bruin-pipeline-name vm.manual.portwatch_weekly_refresh` | `tail -n 80 logs/portwatch/portwatch_extract_weekly_refresh.log` | `gcloud run jobs execute capstone-portwatch-weekly --region "$REGION" --args=portwatch,portwatch_weekly_refresh --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-portwatch-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `brent_weekly_refresh` | `sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh dataset-batch brent brent_weekly_refresh --trigger-type manual --bruin-pipeline-name vm.manual.brent_weekly_refresh` | `tail -n 20 logs/brent/brent_extract_manifest.jsonl` | `gcloud run jobs execute capstone-brent-weekly --region "$REGION" --args=brent,brent_weekly_refresh --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-brent-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `fx_weekly_refresh` | `sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh dataset-batch fx fx_weekly_refresh --trigger-type manual --bruin-pipeline-name vm.manual.fx_weekly_refresh` | `tail -n 20 logs/fx/fx_extract_manifest.jsonl` | `gcloud run jobs execute capstone-fx-weekly --region "$REGION" --args=fx,fx_weekly_refresh --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-fx-weekly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `events_incremental_recent` | `sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh dataset-batch events events_incremental_recent --trigger-type manual --bruin-pipeline-name vm.manual.events_incremental_recent` | `tail -n 20 logs/events/events_silver_manifest.jsonl` | `gcloud run jobs execute capstone-events-incremental --region "$REGION" --args=events,events_incremental_recent --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-events-incremental"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |
| `worldbank_energy_yearly_refresh` | `sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh dataset-batch worldbank_energy worldbank_energy_yearly_refresh --trigger-type manual --bruin-pipeline-name vm.manual.worldbank_energy_yearly_refresh` | `tail -n 20 logs/worldbank_energy/worldbank_energy_extract_manifest.jsonl` | `gcloud run jobs execute capstone-worldbank-energy-yearly --region "$REGION" --args=worldbank_energy,worldbank_energy_yearly_refresh --task-timeout=7200 --wait` | `gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-worldbank-energy-yearly"' --project "$PROJECT_ID" --limit=50 --format="value(timestamp,textPayload)"` |

Cloud Run jobs also upload best-effort local artifacts to GCS:

```bash
gcloud storage ls \
  "gs://$GCS_BUCKET/${GCS_PREFIX:+$GCS_PREFIX/}metadata/serverless_runs/profile=hybrid_vm_serverless/"
```

## Bruin CLI Usage

Bruin is used here for visibility and orchestration structure. It groups related assets into pipelines and lets you validate or run a pipeline/asset explicitly.

Useful commands:

```bash
bruin environments list --config-file .bruin.yml
bruin validate --fast ./bruin/pipelines/dataset_batch/pipeline.yml
bruin validate --fast --env production ./bruin/pipelines/comtrade_bootstrap_day_1/pipeline.yml
bruin run --environment production --force ./bruin/pipelines/comtrade_bootstrap_day_1
bruin lineage ./bruin/pipelines/comtrade_bootstrap_day_1
```

Current Bruin shape:

- `bruin/pipelines/dataset_batch`: generic wrapper for one batch from `ops/batch_plan.json`
- `bruin/pipelines/schedule_lane_queue`: generic wrapper for one schedule lane
- `bruin/pipelines/monthly_refresh`: coarse wrapper
- stage-level pipelines for Comtrade days, PortWatch phase 1, Brent phase 1, FX phase 1, and World Bank full bootstrap

Why Bruin is not the only runner yet:

- shell wrappers still hold real VM compatibility behavior
- secret rendering and Docker Compose startup remain VM-specific
- Comtrade quota/retry/checkpoint behavior is Python-led
- the existing batch runner owns restart flags and ops logging

The safe migration path is additive: expose more true stage boundaries in Bruin while keeping the VM wrappers available.

## Comtrade Metadata Setup

Comtrade metadata is not optional. The pipeline uses metadata to map reporter codes, partner codes, flow codes, HS commodity descriptions, transport modes, customs codes, and quantity units.

The metadata extractor is:

```bash
ingest/comtrade/un_comtrade_tools_metadata.py
```

It calls Comtrade reference endpoints under:

```text
https://comtradeapi.un.org/files/v1/app/reference
```

It writes to:

| Runtime view | Path |
| --- | --- |
| Inside container | `/workspace/data/metadata/comtrade` |
| On VM persistent disk | `/var/lib/pipeline/capstone/data/metadata/comtrade` |
| In repo-relative local runs | `data/metadata/comtrade` |

Run metadata locally:

```bash
set -a
source .env
set +a
uv run python ingest/comtrade/un_comtrade_tools_metadata.py
```

Run metadata on the VM:

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline \
  scripts/run_pipeline.sh python ingest/comtrade/un_comtrade_tools_metadata.py
```

Expected files include:

- `reporters.csv`
- `partners.csv`
- `flows.csv`
- `hs_codes.csv`
- `transport_modes.csv`
- `customs_codes.csv`
- `qty_units.csv`
- `extraction_summary.json`

The silver builder requires at least `reporters.csv`, `partners.csv`, and `flows.csv`. Day 1 includes an explicit metadata step, and the silver path also has a precondition guard that bootstraps required metadata if it is missing.

## Why Comtrade Is Split Into Six Days

Comtrade is the hardest source in this project because the data is large, national reporting is uneven, and API quota is a real design constraint.

The six bootstrap days are deliberate:

| Day | Reporter group | Years | Commodities |
| --- | --- | --- | --- |
| Day 1 | Existing 16 reporters | 2020-2025 | Core food/energy commodities: `1001,1005,1006,1201,2709,2710` |
| Day 2 | Existing 16 reporters | 2015-2019 | Same core commodities |
| Day 3 | New 16 reporters | 2020-2025 | Same core commodities |
| Day 4 | New 16 reporters | 2015-2019 | Same core commodities |
| Day 5 | All 32 reporters | 2020-2026 | Extension commodities: `2711,3102,3105` |
| Day 6 | All 32 reporters | 2015-2019 | Same extension commodities |

The split exists for reliability:

- recent years and older history can be retried separately
- existing and new reporter groups can be proven separately
- core commodities can be stabilized before extension commodities
- failed quota windows do not force a full restart
- logs and checkpoints stay interpretable

The monthly history extractor compacts calls by sending:

- one reporter at a time
- up to four monthly periods in one request
- all selected commodity codes in one comma-separated `cmdCode`
- one flow at a time, usually `M` then `X`

This gets more data out of each request while keeping the response small enough to retry and reason about. The public preview API has a small record cap, and subscription tiers still have query quotas or practical throttling limits. The project therefore treats API calls as scarce and keeps checkpoint files under `data/metadata/comtrade/state` and `logs/comtrade`.

## Observability

Observability is intentionally file-first. Logs should help diagnose a run, not become the reason the run fails.

Important VM paths:

| What | Path |
| --- | --- |
| VM runtime env | `/etc/capstone/pipeline.env` |
| Repo on VM | `/var/lib/pipeline/capstone` |
| Container workspace | `/workspace` |
| Logs | `logs/<dataset>/...` |
| Comtrade state | `data/metadata/comtrade/state/...` |
| Serverless artifact uploads | `gs://BUCKET/PREFIX/metadata/serverless_runs/...` |
| Ops Postgres | `capstone-postgres` container |
| BigQuery ops mirror | raw ops tables, when enabled |

VM checks:

```bash
cd /var/lib/pipeline/capstone
sudo systemctl status capstone-stack --no-pager
sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml ps
sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml logs --tail=200 pipeline
```

Recent task logs:

```bash
find logs -maxdepth 3 -type f -mtime -1
tail -n 20 logs/comtrade/comtrade_extract_manifest.jsonl
tail -n 20 logs/portwatch/portwatch_extract_manifest.jsonl
```

Cloud Run checks:

```bash
gcloud run jobs executions list --job capstone-portwatch-weekly --region "$REGION"
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="capstone-portwatch-weekly"' \
  --project "$PROJECT_ID" \
  --limit=50 \
  --format="value(timestamp,textPayload)"
```

## VM Recovery Zone Fallback

The Terraform contains a manual recovery scaffold for a fallback VM:

- `fallback_zone`
- `recovery_boot_disk_enabled`
- `recovery_data_disk_enabled`
- `recovery_vm_enabled`
- `recovery_boot_source_snapshot`
- `recovery_data_source_snapshot`

This is not automatic failover. It is a controlled recovery option when the primary VM zone is unavailable, resource capacity is constrained, or the original VM cannot be started.

This mattered during the project because a selected VM region/zone was not always available for the desired machine. The fallback design lets an operator restore boot and data snapshots into another zone, start a recovery VM, and keep using the same VM-first pipeline model without redesigning the data system during an outage.

Basic recovery idea:

1. Confirm recent boot and data snapshots exist.
2. Set the recovery snapshot variables in `terraform.tfvars.json`.
3. Enable the recovery boot disk, data disk, and VM flags.
4. Apply Terraform.
5. SSH to the recovery VM.
6. Confirm `/var/lib/pipeline/capstone` and `/etc/capstone/pipeline.env`.
7. Start `capstone-stack` and resume the failed batch from the safest step.

## Data Availability Lessons

The project deliberately stayed cheap, which shaped the architecture.

Real-time macro, shipping, commodity, and geopolitical intelligence is expensive. Paid providers can offer cleaner feeds, faster updates, richer vessel data, and broader coverage. This project instead uses lower-cost sources such as Comtrade, PortWatch, FRED, ECB, World Bank, and a curated event register. That makes the project reproducible, but it also exposes real public-data limitations.

Lessons learned:

- Comtrade is official and valuable, but it is not real time. Some countries update late, some periods are missing, and wartime or disrupted periods can be sparse.
- PortWatch is useful for chokepoint stress, but it does not always provide complete chokepoint information for every recent period. The pipeline therefore records null days and coverage gaps instead of pretending the signal is complete.
- Macro data is easier to get cheaply at monthly, daily, or annual public-series grain than as a true real-time analytics feed.
- World Bank energy data is annual, so the project broadcasts it to month grain only as structural context, not as a monthly measurement.
- The cheapest reliable design is not "collect everything"; it is "collect enough, preserve lineage, and be honest about coverage."

## Commodity Aggregation Lessons

Commodity analysis needs careful grain discipline.

Different HS levels and commodity groupings are not directly comparable just because they have similar names. For example:

- `2709` crude oil and `2710` refined petroleum are related but not interchangeable.
- `2711` petroleum gases is part of the energy story but behaves differently from crude and refined products.
- `3102` nitrogenous fertilizers and `3105` mixed mineral or chemical fertilizers overlap conceptually, but they are not the same measurement.
- Fertilizer value, nitrogen content, physical weight, and strategic importance can move differently.

The project therefore keeps `cmd_code` in the canonical grain and uses commodity groups only as context. Aggregations are useful for dashboard storytelling, but they should not erase the fact that different commodities have different reporting quality, units, market behavior, and substitution logic.

Where possible, compare:

- the same HS code over time
- the same reporter and partner structure
- value with value, weight with weight, and quantity with quantity
- aggregate groups only when the rollup is analytically justified

The silver layer also carries rollup safety signals so downstream marts can avoid false comparability.

## Events Layer

The events layer was created because trade and traffic data alone do not explain why a disruption happened.

The source file is:

```text
data/seed/events/events_seed_extended_2015.csv
```

The builder is:

```bash
uv run python ingest/events/events_silver.py
```

It creates:

- `data/silver/events/dim_event.csv`
- `data/silver/events/dim_event.parquet`
- `data/silver/events/bridge_event_month_chokepoint_core.csv`
- `data/silver/events/bridge_event_month_maritime_region.csv`
- partitioned bridge parquet outputs under `data/silver/events/bridge_event_month_*`
- `logs/events/events_silver.log`
- `logs/events/events_silver_manifest.jsonl`

The events are manually curated and then expanded into analytical bridges:

- each event has an `event_id`
- each event has start/end dates
- lead, active, and lag windows are generated
- event severity is weighted across those windows
- events are linked to core chokepoints or broader maritime regions
- dbt turns those files into `dim_event`, `bridge_event_month`, `bridge_event_chokepoint`, `bridge_event_region`, `bridge_event_location`, and `mart_event_impact`

This adds an explanatory layer over the measured data. It lets the dashboard show that a trade movement or chokepoint stress signal occurred near a named event window, while still keeping the event register separate from the measured source data.

## Warehouse Model Families

Core trade and routing:

- `stg_comtrade_trade_base`
- `stg_comtrade_fact`
- `fct_reporter_partner_commodity_month`
- `fct_reporter_partner_commodity_route_month`
- `fct_reporter_partner_commodity_hub_month`
- `fct_reporter_partner_commodity_month_provenance`

Dimensions:

- `dim_country`
- `dim_time`
- `dim_commodity`
- `dim_trade_flow`
- `dim_chokepoint`
- `dim_event`
- `dim_location`

Marts:

- `mart_reporter_month_trade_summary`
- `mart_reporter_commodity_month_trade_summary`
- `mart_trade_exposure`
- `mart_reporter_month_chokepoint_exposure`
- `mart_reporter_month_chokepoint_exposure_with_brent`
- `mart_hub_dependency_month`
- `mart_macro_monthly_features`
- `mart_reporter_month_macro_features`
- `mart_reporter_energy_vulnerability`
- `mart_event_impact`

Semantic/dashboard marts include:

- `mart_dashboard_global_trade_overview`
- `mart_chokepoint_daily_signal`
- `mart_global_daily_market_signal`
- `mart_chokepoint_monthly_stress`
- `mart_global_monthly_system_stress_summary`
- `mart_chokepoint_monthly_stress_detail`
- `mart_chokepoint_monthly_hotspot_map`
- `mart_executive_monthly_system_snapshot`
- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_reporter_structural_vulnerability`
- `mart_trade_month_coverage_status`

## Dashboard Setup

Dashboard flow:

```text
Repo dbt models -> BigQuery analytics dataset -> Looker Studio data sources -> dashboard pages
```

First make sure the dbt marts exist in BigQuery:

```bash
uv run dbt build --profiles-dir . --target bigquery_dev
```

Warehouse builds should run seeds first so static dbt lookup tables are present in the target dataset. If you run dbt directly, prefer:

```bash
uv run dbt seed --profiles-dir . --target bigquery_dev
uv run dbt build --profiles-dir . --target bigquery_dev
```

If you are operating from the VM, run dbt through the existing runtime stack after the bootstrap has loaded raw BigQuery tables:

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T dbt sh -lc 'dbt seed --profiles-dir . --target bigquery_dev && dbt build --profiles-dir . --target bigquery_dev'
```

In Looker Studio:

1. Create a new report.
2. Choose `Add data` -> `BigQuery`.
3. Select your GCP project.
4. Select the dbt analytics dataset, usually `analytics` unless `DBT_BIGQUERY_DATASET` was changed.
5. Add one data source per semantic mart you want to use.
6. Keep joins/blends minimal. Prefer the semantic marts because they already encode the dashboard grain.

Recommended dashboard wiring:

| Dashboard page | Primary marts | Typical visuals |
| --- | --- | --- |
| Executive overview | `mart_dashboard_global_trade_overview`, `mart_executive_monthly_system_snapshot`, `mart_trade_month_coverage_status` | Scorecards, reporter trends, reporting completeness, latest complete month status |
| Chokepoint operations | `mart_chokepoint_daily_signal`, `mart_chokepoint_monthly_stress`, `mart_chokepoint_monthly_stress_detail`, `mart_chokepoint_monthly_hotspot_map` | Daily throughput trends, stress tables, chokepoint map, rolling signal charts |
| Trade exposure | `mart_reporter_partner_commodity_month_enriched`, `mart_reporter_month_exposure_map`, `mart_trade_exposure` | Reporter/partner/commodity drilldowns, country exposure map, top routes and commodities |
| Macro and system stress | `mart_global_daily_market_signal`, `mart_global_monthly_system_stress_summary`, `mart_macro_monthly_features`, `mart_reporter_month_macro_features` | Brent/FX context, global stress trend, macro scorecards |
| Structural vulnerability | `mart_reporter_structural_vulnerability` | Energy vulnerability scatter, supplier concentration table, event exposure bars, risk-band filters |

Suggested report controls:

- date range control using `month_start_date`
- reporter/country filter using `reporter_country_name`
- commodity filter using `cmd_code` or `commodity_name`
- chokepoint filter using `chokepoint_name`
- risk filter using `risk_band`

For map charts, use the semantic marts that already expose map-ready geography:

- `mart_reporter_month_exposure_map` for country-level exposure maps
- `mart_chokepoint_monthly_hotspot_map` for chokepoint maps

## BigQuery dbt

Run dbt against the BigQuery warehouse:

```bash
uv run dbt debug --profiles-dir . --target bigquery_dev
uv run dbt build --profiles-dir . --target bigquery_dev
```

If Terraform is your source of truth for names:

```bash
python infra/terraform/render_dotenv.py > .env
```

or:

```bash
make dbt-bigquery-debug
make dbt-bigquery-build
```

### dbt Documentation

A portable dbt docs HTML snapshot is available in the repo at [docs/dbt/index.html](docs/dbt/index.html).

The repo copy under `docs/dbt/` also carries the generated dbt metadata files used by the docs site, including `manifest.json`, `catalog.json`, `run_results.json`, `graph_summary.json`, `semantic_manifest.json`, and the static HTML bundle.

To regenerate the local docs artifacts under ignored `target/`:

```bash
make dbt-bigquery-docs-static
```

To refresh the repo-visible dbt docs bundle:

```bash
make dbt-bigquery-docs-publish
```

Both commands use the BigQuery profile and Terraform-derived environment, so run them from a machine or VM with Google Application Default Credentials available.

### Bruin Documentation Artifacts

Bruin does not generate a static documentation site in the same way dbt does. The useful repo artifacts are validation output and per-asset lineage JSON:

```bash
make bruin-docs-publish
```

That writes:

- `docs/bruin/validation.json`
- `docs/bruin/lineage/*.json`

The lineage files are generated from `bruin lineage --full --output json` and use repo-relative paths so they can travel with the project.

## Delivery Checklist

Before sharing or operating this repo in a new environment:

- remove or rotate any accidental local secrets
- confirm `.env` is not committed
- confirm `infra/terraform/terraform.tfvars.json` does not contain private values you do not want to share
- run `python -m json.tool ops/batch_plan.json`
- run `bruin validate --fast ./bruin/pipelines/dataset_batch/pipeline.yml`
- run `uv run dbt parse --profiles-dir . --target bigquery_dev`

## Reflection

The most important engineering lesson from this project is that reliability beat elegance almost every time.

The VM stayed central because it made long-running Comtrade recovery understandable. Bruin was added gradually because the wrappers still contained real operating knowledge. Secret Manager became real infrastructure because environment drift across shell, systemd, Docker, Bruin, and Cloud Run was one of the easiest ways to break an otherwise correct pipeline.

The data also taught architectural humility. Public data is powerful, but it is not always timely, complete, or comparable. A professional pipeline has to show missingness, preserve source lineage, and explain why a metric exists at a particular grain. That is why this project carries manifests, checkpoints, event bridges, commodity grain, and coverage tests instead of only producing final dashboard tables.
