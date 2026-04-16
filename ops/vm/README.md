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
