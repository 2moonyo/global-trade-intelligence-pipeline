# VM Runtime Guide

This directory holds the operator-facing pieces for the keyless GCE VM runtime.

## Why the VM does not use a JSON key

- The Compute Engine VM has a user-managed service account attached directly to the instance.
- Google client libraries, `dbt-bigquery`, and the Python pipeline resolve Application Default Credentials from the metadata server automatically.
- No `GOOGLE_APPLICATION_CREDENTIALS` file is needed on disk for GCP access.

## Runtime layout

- Primary runtime: `europe-west1-b` by default.
- Fallback recovery runtime: `europe-west1-d`, disabled by default.
- Legacy US runtime: optional and intended only for staged migration cleanup.

The default VM profile uses `e2-standard-2`, keeps runtime state on `/var/lib/pipeline`, and provisions a 4 GB swap file on that persistent disk during first boot.

## First-time VM setup

1. Apply Terraform from your laptop.
2. SSH to the target VM once.
3. Copy the repository bundle to `/var/lib/pipeline/capstone`.
4. Create the root-owned env file:

```bash
sudo install -d -m 0750 /etc/capstone
sudo cp /var/lib/pipeline/capstone/ops/vm/pipeline.env.example /etc/capstone/pipeline.env
sudo chmod 600 /etc/capstone/pipeline.env
sudo editor /etc/capstone/pipeline.env
```

5. Start the stack:

```bash
sudo systemctl start capstone-stack
```

## First manual run

Start by proving the non-Comtrade workloads on the VM:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh ops-init-all
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh bootstrap-non-comtrade
```

World Bank energy runs in its own post-Comtrade lane because it depends on the Comtrade country dimension:

```bash
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline scripts/run_pipeline.sh country-trade-and-energy
```

## Enable schedule lanes

The startup script writes one timer unit per configured schedule lane. Enable only the lanes you want:

```bash
sudo systemctl enable --now capstone-schedule-lane-incremental_daily.timer
sudo systemctl enable --now capstone-schedule-lane-weekly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-monthly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-yearly_refresh.timer
```

Bootstrap timers are optional. If you want them, add more entries to `vm_schedule_lane_timers` in Terraform and re-apply.

## Quick validation

```bash
sudo systemctl status capstone-stack
sudo systemctl list-timers 'capstone-schedule-lane-*'
/sbin/swapon --show
free -h
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline /workspace/.venv/bin/python -c "import google.auth; creds, project = google.auth.default(); print(type(creds).__name__); print(project)"
```

## Start, stop, destroy, and recovery targets

From your laptop, use the repo helpers:

```bash
make vm-status
scripts/vm_runtime_ctl.sh status primary
scripts/vm_runtime_ctl.sh status legacy
scripts/vm_runtime_ctl.sh status recovery
scripts/vm_runtime_ctl.sh start primary
scripts/vm_runtime_ctl.sh stop primary
scripts/vm_runtime_ctl.sh destroy-compute-terraform legacy
```

- `primary` is the default target for start/stop/delete commands.
- `legacy` is the staged migration target for the current US VM.
- `recovery` is the fallback target and remains disabled unless you explicitly enable it in tfvars.

Fallback is manual recovery only. A single zonal VM cannot provide realistic automatic zone failover through Terraform alone.
