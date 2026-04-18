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

Keep the non-secret settings here (`GCP_PROJECT_ID`, bucket/dataset names, auth mode, batch-plan path).
Approved secret values can then be refreshed from Secret Manager instead of being edited by hand:

```bash
cd /var/lib/pipeline/capstone
./scripts/render_pipeline_env_from_secret_manager.sh \
  --output-file /etc/capstone/pipeline.env \
  --base-env-file /etc/capstone/pipeline.env \
  --show-keys
```

5. Start the stack:

```bash
sudo systemctl start capstone-stack
```

## Configure VM Git access (public and private repos)

If your VM repository folder does not include `.git`, or if you need to pull updates directly from GitHub on the VM, set up Git access first.

### Public repository

No deploy key is required. Use an HTTPS remote on the VM:

```bash
cd /var/lib/pipeline/capstone
git remote set-url origin https://github.com/OWNER/REPO.git
git fetch --all
git pull --ff-only
```

### Private repository (recommended: deploy key)

1. Generate a dedicated keypair on the VM:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/github_deploy_key -C "vm-deploy-key" -N ""
cat ~/.ssh/github_deploy_key.pub
```

2. In GitHub, open the target repo settings and add the public key as a Deploy key (read-only is sufficient).

3. Configure SSH on the VM:

```bash
cat >> ~/.ssh/config <<'EOF'
Host github.com
	HostName github.com
	User git
	IdentityFile ~/.ssh/github_deploy_key
	IdentitiesOnly yes
EOF

chmod 600 ~/.ssh/config
ssh-keyscan github.com >> ~/.ssh/known_hosts
ssh -T git@github.com
```

4. Set the SSH remote and verify pull:

```bash
cd /var/lib/pipeline/capstone
git remote set-url origin git@github.com:OWNER/REPO.git
git fetch --all
git pull --ff-only
```

## Laptop helper scripts

Use these scripts from your laptop to operate a VM runtime consistently.

### 1) Git sync only

`scripts/vm_repo_sync.sh` only manages repository initialization/sync on the VM. It does not modify API keys.

```bash
scripts/vm_repo_sync.sh \
	--vm-user chromazone \
	--vm-host 104.199.42.249 \
	--ssh-key-path "$HOME/.ssh/google_compute_engine" \
	--vm-repo-dir /var/lib/pipeline/capstone \
	--repo-url git@github.com:OWNER/REPO.git \
	--branch cloud_migration
```

Pin to a specific commit (optional):

```bash
scripts/vm_repo_sync.sh \
	--vm-user chromazone \
	--vm-host 104.199.42.249 \
	--ssh-key-path "$HOME/.ssh/google_compute_engine" \
	--repo-url git@github.com:OWNER/REPO.git \
	--branch cloud_migration \
	--commit 0123abcd4567ef89deadbeefcafefeed12345678
```

If `--commit` is omitted, the script pulls the latest for the selected branch.
If `--commit` is provided, the script checks out that commit in detached HEAD mode.

### 2) API key insert/update

`scripts/vm_api_insert.sh` updates runtime keys in `/etc/capstone/pipeline.env` when setup changes.

Interactive mode:

```bash
scripts/vm_api_insert.sh \
	--vm-user chromazone \
	--vm-host 104.199.42.249 \
	--ssh-key-path "$HOME/.ssh/google_compute_engine" \
	--interactive-comtrade \
	--interactive-fred \
	--show-keys
```

Direct set mode:

```bash
scripts/vm_api_insert.sh \
	--vm-user chromazone \
	--vm-host 104.199.42.249 \
	--ssh-key-path "$HOME/.ssh/google_compute_engine" \
	--set COMTRADE_API_KEY_DATA=xxxxx \
	--set COMTRADE_API_KEY_DATA_A=yyyyy \
	--set FRED_API_KEY=zzzzz \
	--show-keys
```

## How to find VM_HOST, VM_USER, and SSH_KEY_PATH

Use this checklist before running `scripts/vm_repo_sync.sh` or `scripts/vm_api_insert.sh`.

### VM_HOST (from local machine)

Get the VM external IP with `gcloud`:

```bash
gcloud compute instances describe capstone-vm-eu \
	--zone europe-west1-b \
	--format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

If your team uses DNS, the hostname can be used instead of the external IP.

### VM_USER (Linux login user)

Preferred method from local machine:

```bash
gcloud compute ssh capstone-vm-eu --zone europe-west1-b --command 'whoami'
```

The output is the value to use for `--vm-user`.

From an already-open VM shell, run:

```bash
whoami
```

Do not use the VM instance name as the SSH user.

### SSH_KEY_PATH (local private key path)

On your local machine, confirm the Google Compute Engine private key exists:

```bash
ls -l ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine.pub
```

Use the private key path for script arguments:

```bash
--ssh-key-path "$HOME/.ssh/google_compute_engine"
```

Do not use the `.pub` file for `--ssh-key-path`.

If the key does not exist yet, generate/register it via:

```bash
gcloud compute ssh capstone-vm-eu --zone europe-west1-b --command 'echo ssh-bootstrap-ok'
```

### Git context checks (VM and local)

From VM, confirm repo remote and branch state:

```bash
cd /var/lib/pipeline/capstone
git remote -v
git branch --show-current
```

From local, verify the repo URL you plan to pass to `--repo-url`:

```bash
git remote get-url origin
```

For private repos, use SSH remotes and ensure VM deploy key setup is complete.

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

## On-demand batch sets (outside schedule timers)

Use the scripts under `scripts/vm_batches/` to run individual bootstrap sets without crafting ad-hoc docker commands.

These scripts:

- validate VM paths and env file
- bring the compose stack up
- optionally refresh selected keys from Secret Manager
- run `ops-init-all`
- call `scripts/run_pipeline.sh dataset-batch <dataset> <batch_id>` with consistent flags

### One-command dispatcher

From VM:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_set.sh --help
```

Available sets:

- `comtrade-day-1`
- `comtrade-day-2`
- `comtrade-day-3`
- `comtrade-day-4`
- `comtrade-day-5`
- `comtrade-day-6`
- `comtrade-all`
- `noncomtrade-phase-1-portwatch`
- `noncomtrade-phase-1-brent`
- `noncomtrade-phase-1-fx`
- `noncomtrade-phase-1-events`
- `noncomtrade-phase-1-all`
- `noncomtrade-phase-2-portwatch`
- `noncomtrade-phase-2-brent`
- `noncomtrade-phase-2-fx`
- `noncomtrade-phase-2-events`
- `noncomtrade-phase-2-all`

### Example: run one set now

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_set.sh comtrade-day-2
```

### Example: run one set and force secret refresh first

```bash
cd /var/lib/pipeline/capstone
SYNC_SECRETS_BEFORE_RUN=true SECRET_PROJECT_ID=fullcap-10111 \
./scripts/vm_batches/run_set.sh noncomtrade-phase-2-brent
```

### Example: run all non-Comtrade phase 1 batches

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_set.sh noncomtrade-phase-1-all
```

### Example: run all Comtrade days

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_set.sh comtrade-all
```

### Passing extra runner arguments

Anything after the set name is forwarded to `dataset-batch`.

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_set.sh comtrade-day-3 --plan-path ops/batch_plan.json --trigger-type manual
```

### Direct script calls (without dispatcher)

From VM:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_comtrade_day_1.sh
./scripts/vm_batches/run_comtrade_day_2.sh
./scripts/vm_batches/run_comtrade_day_3.sh
./scripts/vm_batches/run_comtrade_day_4.sh
./scripts/vm_batches/run_comtrade_day_5.sh
./scripts/vm_batches/run_comtrade_day_6.sh

./scripts/vm_batches/run_noncomtrade_phase_1_portwatch.sh
./scripts/vm_batches/run_noncomtrade_phase_1_brent.sh
./scripts/vm_batches/run_noncomtrade_phase_1_fx.sh
./scripts/vm_batches/run_noncomtrade_phase_1_events.sh

./scripts/vm_batches/run_noncomtrade_phase_2_portwatch.sh
./scripts/vm_batches/run_noncomtrade_phase_2_brent.sh
./scripts/vm_batches/run_noncomtrade_phase_2_fx.sh
./scripts/vm_batches/run_noncomtrade_phase_2_events.sh
```

## End-to-end operator workflow: edit, push, pull, rebuild, run

This is the recommended lifecycle when you change code locally and need the VM to run the updated version.

### 1) Local development and validation

From local repo:

```bash
cd /Users/chromazone/Documents/Python/Data\ Enginering\ Zoomcamp/Capstone_monthly
git checkout -b chore/vm-batch-ops-docs

# make code/doc changes

# optional local checks
python3 -m py_compile ingest/portwatch/portwatch_extract.py
for f in scripts/vm_batches/*.sh; do bash -n "$f"; done
```

### 2) Commit and push

```bash
cd /Users/chromazone/Documents/Python/Data\ Enginering\ Zoomcamp/Capstone_monthly
git add -A
git commit -m "Add VM batch set runners and operator runbook"
git push -u origin chore/vm-batch-ops-docs
```

If you use PR workflow, merge this branch before pulling on the VM.

### 3) Pull updates on VM

SSH to VM, then:

```bash
cd /var/lib/pipeline/capstone
git status
git fetch --all
git checkout cloud_migration
git pull --ff-only
```

If VM has local changes, resolve those first. Prefer a clean working tree before pulling.

### 4) Rebuild docker dependencies/images on VM

Use compose build to pick up Dockerfile and Python dependency changes:

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml build --pull pipeline orchestrator
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml up -d
```

If dependency state looks stale or corrupted, use a clean recreate:

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml down
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml build --no-cache pipeline orchestrator
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml up -d
```

### 5) Refresh selected secrets from Secret Manager (optional but recommended)

```bash
cd /var/lib/pipeline/capstone
./scripts/render_pipeline_env_from_secret_manager.sh \
  --output-file /etc/capstone/pipeline.env \
  --base-env-file /etc/capstone/pipeline.env \
  --project fullcap-10111 \
  --show-keys
```

This preserves the current `/etc/capstone/pipeline.env` runtime contract while refreshing only approved secret-backed keys from Secret Manager.

Batch helpers can also refresh the same keys automatically before a manual run:

```bash
cd /var/lib/pipeline/capstone
SYNC_SECRETS_BEFORE_RUN=true SECRET_PROJECT_ID=fullcap-10111 \
./scripts/vm_batches/run_set.sh comtrade-day-1 --trigger-type manual
```

### 6) Run the target set out of schedule

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_set.sh noncomtrade-phase-2-all
```

### 7) Observe progress and failures

Pipeline runs and task runs:

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml exec -T postgres \
	psql -U capstone -d capstone \
	-c "SELECT pipeline_run_id,dataset_name,batch_id,status,started_at,finished_at,error_summary FROM ops.pipeline_run ORDER BY started_at DESC LIMIT 20;" \
	-c "SELECT pipeline_run_id,task_name,step_order,status,started_at,finished_at,error_summary FROM ops.task_run ORDER BY started_at DESC LIMIT 80;"
```

Container health:

```bash
cd /var/lib/pipeline/capstone
sudo docker compose --env-file /etc/capstone/pipeline.env -f /var/lib/pipeline/capstone/docker/docker-compose.yml ps
```

### 8) Keep schedule timers and manual runs separate

- schedule timers are for regular cadence lanes
- `scripts/vm_batches/run_set.sh` is for operator-triggered out-of-schedule execution
- you can keep timers enabled while still running these scripts manually when needed


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
