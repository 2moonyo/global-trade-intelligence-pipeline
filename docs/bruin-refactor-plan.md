# Bruin Refactor Plan

## Purpose

This document captures the current Bruin migration state for the VM-first pipeline and the intended additive rollout path.

It is not a rewrite proposal.
It is a controlled refactor plan for exposing real stage boundaries in Bruin while preserving the working VM scheduler, wrappers, env model, and recovery behavior.

## Current Baseline

The operational baseline remains:

- GCP VM with persistent disk
- systemd timers/services
- docker compose runtime
- shell wrappers and Python runners
- GCS bronze and silver where implemented
- BigQuery warehouse loads
- dbt downstream builds

Bruin is currently additive to that baseline, not the owner of the full runtime contract.

## Guardrails

- Preserve the VM-first execution path.
- Do not replace wrappers that still provide real compatibility or bootstrap value.
- Do not introduce a second secret-management path.
- Keep Secret Manager, env-file injection, and keyless VM metadata auth intact.
- Prefer the smallest safe diff over broad standardization.
- Keep legacy wrapper pipelines available until new stage-level pipelines are proven.

## What Is Implemented Now

### Bruin config

`.bruin.yml` now contains:

- `default` environment
- `production` environment
- shared `google_cloud_platform` connection name `gcp-default`
- env-var-backed `project_id` and `location`
- `use_application_default_credentials: true`

This preserves the current local ADC and VM metadata auth pattern instead of introducing inline credentials.

### Existing wrapper pipelines still preserved

The coarse-grained wrapper pipelines remain available:

- `bruin/pipelines/dataset_batch`
- `bruin/pipelines/schedule_lane_queue`
- `bruin/pipelines/monthly_refresh`

These remain the safe baseline during migration.

### Additive stage-level pipelines

Two explicit stage-level pipelines now exist:

- `bruin/pipelines/portwatch_bootstrap_phase_1`
- `bruin/pipelines/comtrade_bootstrap_day_1`

These expose real step order with explicit `depends` while still reusing existing repo scripts for bootstrap and auth behavior.

PortWatch phase 1 stages:

1. `extract`
2. `silver`
3. `publish_gcs`
4. `load_bigquery`
5. `dbt_build`

Comtrade day 1 stages:

1. `extract`
2. `metadata`
3. `silver`
4. `routing`
5. `publish_gcs`
6. `load_bigquery`
7. `dbt_build`

### Recovery improvement already in place

`warehouse/run_dataset_batch.py` now supports:

- `--start-at-task`
- `--start-at-step-order`

This reduces manual mid-run recovery orchestration without changing the current batch-runner baseline.

### CI validation

A lightweight validation workflow now exists at:

- `.github/workflows/bruin-validate.yml`

It:

- checks `.bruin.yml` parsing with `bruin environments list`
- runs `bruin validate --fast` across the current Bruin pipelines
- avoids deployment behavior
- uses placeholder GCP env vars so CI does not require live credentials

## Why The Rollout Is Additive

The repo still contains important execution behavior outside Bruin:

- VM bootstrap and auth normalization
- queue-aware batch orchestration
- retry and checkpoint behavior
- dataset-specific compatibility logic in shell and Python wrappers

Because of that, the migration path is:

1. expose true stage boundaries in Bruin
2. keep wrapper pipelines available
3. validate stage-level paths incrementally
4. only consider changing defaults after VM evidence is stable

## Validation Posture

Current validation is intentionally lightweight:

- `bruin validate --fast`
- Python compile checks for new Bruin asset files
- YAML sanity checks for workflow/config files

In the current local environment, Bruin validation consistently returns warning-only output related to the local used-tables/sql-parser startup path.
That warning profile predates the additive stage-level pipelines and has remained stable across the new changes.

## Remaining Migration Steps

### Short term

1. Prove the additive PortWatch and Comtrade stage-level pipelines in real VM runs.
2. Decide whether another non-Comtrade lane should be exposed at stage level.
3. Decide whether another Comtrade bootstrap day should be exposed at stage level.

### Medium term

1. Improve `.bruin.yml` only as needed for real pipeline usage.
2. Keep stage-level assets aligned with `ops/batch_plan.json` so Bruin reflects real runtime structure.
3. Evaluate whether selected wrapper pipelines can become secondary paths instead of primary ones.

### Not in scope now

- replacing the VM baseline
- removing wrappers blindly
- moving execution to serverless
- making Bruin the sole owner of recovery or scheduling semantics

## Recommended Operator Guidance

Use the current paths as follows:

- Use wrapper pipelines for the safest known baseline.
- Use the additive stage-level pipelines to improve lineage visibility and prove step boundaries.
- Use `docs/agent-worklog.md` as the authoritative continuity journal for the next migration steps.

## Exit Criteria For This Refactor Slice

This Bruin refactor slice is successful when:

- the VM baseline still runs without new manual fixes
- `.bruin.yml` is explicit and stable
- at least one non-Comtrade and one Comtrade lane are represented as additive stage-level Bruin pipelines
- CI performs lightweight Bruin validation
- the repo has a clear written handoff path for the next migration steps
