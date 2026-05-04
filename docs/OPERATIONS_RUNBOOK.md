# Operations Runbook

This runbook is the operator-focused companion to the top-level `README.md`.

Use it when you need to:

- make code or dbt changes and rebuild the runtime safely
- provision a clean VM and run the pipeline from scratch
- rerun a failed batch without restarting the whole platform
- run one-off backfills or targeted refreshes
- confirm the actual dataset parameters currently encoded in `ops/batch_plan.json`

`ops/batch_plan.json` remains the source of truth for batch windows, reporters, commodities, and task order.

## Current orchestration map

| Layer | Path | Role |
| --- | --- | --- |
| Batch definitions | `ops/batch_plan.json` | Source of truth for batch ids, windows, task order, and batch-specific parameters. |
| Runtime ownership | `ops/execution_profiles.json` | Decides whether a batch stays on the VM or is dispatched to Cloud Run. |
| Generic batch runner | `warehouse/run_dataset_batch.py` | Executes one batch end to end and records ops metadata. |
| Generic queue runner | `warehouse/run_batch_queue.py` | Runs every enabled batch in one `schedule_lane` in dependency order. |
| Pipeline wrapper | `scripts/run_pipeline.sh` | Operator entry point inside `pipeline` and `orchestrator` containers. |
| Explicit weekly Bruin assets | `bruin/pipelines/*_weekly_refresh`, `bruin/pipelines/events_incremental_recent` | Stage-level refresh pipelines with direct runtime dispatch support. |
| Coarse Bruin wrappers | `bruin/pipelines/monthly_refresh`, `bruin/pipelines/schedule_lane_queue` | High-level orchestration wrappers for refresh lanes and queue runs. |
| VM helper scripts | `scripts/vm_bootstrap.sh`, `scripts/vm_batches/*.sh`, `ops/vm/README.md` | Bootstrap, sync, and VM-first operations workflow. |

## Fresh run on a clean VM

Use this path when you want a reproducible run from a fresh repo copy and fresh VM stack.

### 1. Bootstrap from the laptop

```bash
make vm-bootstrap VM_BOOTSTRAP_ARGS="--reset-known-host --show-resolved"
```

This applies Terraform, syncs approved secrets to Secret Manager, copies the repo to the VM, renders `/etc/capstone/pipeline.env`, and starts the stack.

### 2. Run the first full bootstrap on the VM

```bash
gcloud compute ssh capstone-vm-eu --zone europe-west1-b
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_full_bootstrap.sh
```

If you want the step-by-step sequence instead:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_noncomtrade_phase_1_all.sh
./scripts/vm_batches/run_noncomtrade_phase_2_all.sh
./scripts/vm_batches/run_comtrade_all_days.sh
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline scripts/run_pipeline.sh country-trade-and-energy
```

### 3. Validate the stack

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env -f docker/docker-compose.yml ps
tail -n 20 logs/comtrade/comtrade_extract_manifest.jsonl
tail -n 20 logs/portwatch/portwatch_extract_manifest.jsonl
```

### 4. Enable recurring timers

```bash
sudo systemctl enable --now capstone-schedule-lane-incremental_daily.timer
sudo systemctl enable --now capstone-schedule-lane-weekly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-monthly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-yearly_refresh.timer
systemctl list-timers 'capstone-schedule-lane-*'
```

## Rebuild rules after code changes

The runtime containers build code into the image. Updating files under `/var/lib/pipeline/capstone` is not enough by itself.

| What changed | Rebuild action |
| --- | --- |
| dbt SQL, macros, Python ingestion code, Bruin assets, or shell wrappers used inside the containers | Rebuild `pipeline` and `orchestrator`, then bring the stack back up. |
| `docker/pipeline/Dockerfile`, `docker/orchestrator/Dockerfile`, `requirements.txt`, `pyproject.toml`, or other dependency inputs | Use `docker compose build --pull ...`; if dependency state looks stale, use `--no-cache`. |
| Only docs outside the runtime path | No container rebuild required. |

### Standard rebuild on the VM

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f /var/lib/pipeline/capstone/docker/docker-compose.yml \
  build --pull pipeline orchestrator
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f /var/lib/pipeline/capstone/docker/docker-compose.yml \
  up -d
```

### Clean recreate when dependencies or image state look stale

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f /var/lib/pipeline/capstone/docker/docker-compose.yml \
  down
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f /var/lib/pipeline/capstone/docker/docker-compose.yml \
  build --no-cache pipeline orchestrator
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f /var/lib/pipeline/capstone/docker/docker-compose.yml \
  up -d
```

### Refresh approved secrets before a run

```bash
cd /var/lib/pipeline/capstone
./scripts/render_pipeline_env_from_secret_manager.sh \
  --output-file /etc/capstone/pipeline.env \
  --base-env-file /etc/capstone/pipeline.env \
  --show-keys
```

## Backfills and partial reruns

### Generic batch rerun

Run any batch directly through the shared runner:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline \
  scripts/run_pipeline.sh dataset-batch <dataset_name> <batch_id> \
  --trigger-type manual \
  --bruin-pipeline-name vm.manual.<batch_id>
```

Example:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline \
  scripts/run_pipeline.sh dataset-batch comtrade comtrade_monthly_refresh \
  --trigger-type manual \
  --bruin-pipeline-name vm.manual.comtrade_monthly_refresh
```

### Resume from a later task instead of rerunning the full batch

Start at a named task:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline \
  scripts/run_pipeline.sh dataset-batch comtrade comtrade_monthly_refresh \
  --start-at-task publish_gcs \
  --trigger-type manual
```

Start at a numeric step:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline \
  scripts/run_pipeline.sh dataset-batch portwatch portwatch_weekly_refresh \
  --start-at-step-order 3 \
  --trigger-type manual
```

`ops/batch_plan.json` defines the step order. Common task names are `extract`, `metadata`, `silver`, `routing`, `publish_gcs`, `load_bigquery`, and `dbt_build`.

### Rerun a whole schedule lane

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env \
  -f docker/docker-compose.yml \
  exec -T pipeline \
  scripts/run_pipeline.sh batch-queue weekly_refresh
```

Useful schedule lanes:

- `incremental_daily`
- `weekly_refresh`
- `monthly_refresh`
- `yearly_refresh`
- bootstrap lanes such as `comtrade_bootstrap_day_1` or `bootstrap_phase_2`

### Runtime-aware weekly refresh dispatch

For explicit Bruin weekly pipelines, let the helper decide whether to stay on the VM or dispatch to Cloud Run:

```bash
python warehouse/run_bruin_pipeline.py \
  --batch-id portwatch_weekly_refresh \
  --execution-profile hybrid_vm_serverless \
  --cloud-run-region "$REGION" \
  --wait
```

Force the VM:

```bash
python warehouse/run_bruin_pipeline.py \
  --batch-id portwatch_weekly_refresh \
  --execution-profile hybrid_vm_serverless \
  --target-runtime vm \
  --environment production
```

Force Cloud Run:

```bash
python warehouse/run_bruin_pipeline.py \
  --batch-id portwatch_weekly_refresh \
  --execution-profile all_vm \
  --target-runtime cloud_run \
  --cloud-run-region "$REGION" \
  --wait
```

### Cloud Run one-off backfills

For non-Comtrade datasets, you can override the default batch id at execution time:

```bash
gcloud run jobs execute capstone-portwatch-weekly \
  --region "$REGION" \
  --args=portwatch,portwatch_bootstrap_phase_2 \
  --task-timeout=7200 \
  --wait
```

Do not use this pattern for Comtrade unless you intentionally redesign its state, quota, and recovery approach.

## Dataset parameter matrix

### Comtrade reporter sets

Primary 16 configured reporters:

- `EUR`, `BGR`, `CHN`, `FRA`, `NLD`, `ROU`, `ESP`, `USA`, `RUS`, `IND`, `ZAF`, `EGY`, `TUR`, `IDN`, `BRA`, `PAN`

Secondary 16 configured reporters:

- `AUS`, `CAN`, `JPN`, `KOR`, `MYS`, `MEX`, `MAR`, `NOR`, `PHL`, `QAT`, `SAU`, `SGP`, `THA`, `ARE`, `GBR`, `VNM`

Core commodities:

- `1001`, `1005`, `1006`, `1201`, `2709`, `2710`

Extension commodities:

- `2711`, `3102`, `3105`

### Comtrade batches

| Batch id | Window | Reporters | Commodities | Notes |
| --- | --- | --- | --- | --- |
| `comtrade_bootstrap_day_1` | `2020-01` to `2025-12` | Primary 16 | Core | Includes explicit metadata step. |
| `comtrade_bootstrap_day_2` | `2015-01` to `2019-12` | Primary 16 | Core | Older-history continuation of day 1. |
| `comtrade_bootstrap_day_3` | `2020-01` to `2025-12` | Secondary 16 | Core | Recent window for the second reporter set. |
| `comtrade_bootstrap_day_4` | `2015-01` to `2019-12` | Secondary 16 | Core | Older-history continuation of day 3. |
| `comtrade_bootstrap_day_5` | `2020-01` to `2026-12` | All 32 | Extension | Recent window for extension commodities. |
| `comtrade_bootstrap_day_6` | `2015-01` to `2019-12` | All 32 | Extension | Older-history continuation of day 5. |
| `comtrade_monthly_refresh` | `2025-01` to current | All 32 | Core + extension | Monthly refresh window after all six bootstrap days complete. |

All Comtrade batches run both flows:

- `M`
- `X`

### Non-Comtrade batches

| Dataset | Batch id | Window encoded in batch plan | Key task parameters |
| --- | --- | --- | --- |
| PortWatch | `portwatch_bootstrap_phase_1` | `2020-01-01` to current | `portwatch_extract.py --start-date 2020-01-01`; publish/load since `2020-01` |
| PortWatch | `portwatch_bootstrap_phase_2` | `2015-01-01` to `2019-12-31` | publish/load `--since-year-month 2015-01 --until-year-month 2019-12` |
| PortWatch | `portwatch_weekly_refresh` | `2025-01-01` to current | weekly explicit Bruin pipeline and weekly refresh lane batch |
| Brent | `brent_bootstrap_phase_1` | `2020-01-01` to current | `brent_crude.py --start 2020-01-01`; publish/load since `2020-01` |
| Brent | `brent_bootstrap_phase_2` | `2015-01-01` to `2019-12-31` | publish/load `--since-year-month 2015-01 --until-year-month 2019-12` |
| Brent | `brent_weekly_refresh` | `2025-01-01` to current | publish/load since `2025-01` |
| FX | `fx_bootstrap_phase_1` | `2020-01-01` to current | `fx_rates.py --start 2020-01-01`; silver/publish/load since `2020-01` |
| FX | `fx_bootstrap_phase_2` | `2015-01-01` to `2019-12-31` | silver/publish/load `2015-01` to `2019-12` |
| FX | `fx_weekly_refresh` | `2025-01-01` to current | silver/publish/load since `2025-01` |
| Events | `events_bootstrap_phase_1` | `2015-01` to `2022-12` | publish/load `--since-year-month 2015-01 --until-year-month 2022-12` |
| Events | `events_bootstrap_phase_2` | `2023-01` to `2024-12` | publish/load `--since-year-month 2023-01 --until-year-month 2024-12` |
| Events | `events_incremental_recent` | `2025-01` to current | daily incremental explicit Bruin pipeline; publish/load since `2025-01` |
| World Bank Energy | `worldbank_energy_bootstrap_full` | `2015` to `2026` | `worldbank_energy.py extract --selector db-countries --energy-types all --start-year 2015 --end-year 2026` |
| World Bank Energy | `worldbank_energy_yearly_refresh` | `2026` to current | yearly refresh using `--start-year 2026` and `--since-year 2026` |

## Recommended operator patterns

- Use bootstrap batches when you are filling a missing historical window for the first time.
- Use refresh batches when history already exists and you only need the active recent window.
- Use `--start-at-task` after a late-stage failure such as `publish_gcs`, `load_bigquery`, or `dbt_build`.
- Rebuild containers before any production rerun after Python, dbt, Bruin, or shell changes.
- Prefer the explicit weekly Bruin pipelines when you want runtime-aware VM versus Cloud Run dispatch without memorizing job names.
