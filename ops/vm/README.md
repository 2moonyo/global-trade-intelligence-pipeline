# VM Runtime Guide

This directory holds the operator-facing pieces for the keyless VM runtime.

## Why the VM does not use a JSON key

- The Compute Engine VM has a user-managed service account attached directly to the instance.
- Google client libraries, `dbt-bigquery`, and the Python pipeline resolve Application Default Credentials from the metadata server automatically.
- No `GOOGLE_APPLICATION_CREDENTIALS` file is needed on disk for GCP access.

## First-time VM setup

1. Apply Terraform from your laptop.
2. SSH to the VM once.
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
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T pipeline /workspace/.venv/bin/python -c "import google.auth; creds, project = google.auth.default(); print(type(creds).__name__); print(project)"
```
