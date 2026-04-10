# Project State Snapshot

Last updated: 2026-04-10
Repository root: `/Users/chromazone/Documents/Python/Data Enginering Zoomcamp/Capstone_monthly`

## Project Objective

This capstone builds a BigQuery-backed dbt semantic layer for a Looker Studio dashboard about:

- global trade
- chokepoint disruption
- Brent oil context
- country exposure and trade reliance
- structural vulnerability
- historical event and trade context

The intended dashboard story order is:

1. Ships and chokepoint system status now
2. Recent daily volatility and comparison to prior daily behavior
3. Monthly chokepoint stress and broader system change
4. Trade exposure and country reliance on chokepoints
5. Structural country vulnerability
6. Historical trade patterns and event context

## Stable Modeling Constraints

These constraints were treated as non-negotiable during implementation and should remain in force after migration:

- Preserve the existing Page 1 trade overview mart unless it is genuinely broken.
- Keep daily and monthly facts strictly separated.
- Do not join daily signal rows directly to monthly trade rows.
- Build marts specifically for Looker Studio, not for a generic BI tool.
- Prefer narrow, explicit, one-grain semantic marts over mega marts.
- Keep user-facing names clear and presentation-ready.
- Do not hide missingness in trade coverage.
- Do not imply route precision beyond modeled routing logic.
- Keep Page 4 as the primary analytical home for maps.
- Keep Page 1 uncluttered; any map there must remain secondary.

## Stage Completion Status

### Stage 1: Inspection

Completed.

Key findings captured during the work:

- PortWatch monthly support already existed.
- Brent daily and monthly support existed upstream.
- Trade coverage and missingness were already central to Page 1.
- Daily PortWatch required separate raw loading and semantic handling.
- Existing map support was upstream but not yet exposed cleanly for Looker.

### Stage 2: Daily Foundation

Completed in code.

Implemented:

- `models/staging/stg_portwatch_daily.sql`
- `models/staging/stg_brent_daily.sql`
- `models/marts/semantics/mart_chokepoint_daily_signal.sql`
- `models/marts/semantics/mart_global_daily_market_signal.sql`
- daily docs and tests
- PortWatch daily silver output and BigQuery/GCS publish-load path support

Post-implementation fix:

- A shared macro bug in `macros/fx_transform.sql` caused malformed division for PortWatch daily share fields.
- The bug was fixed by wrapping both numerator and denominator in parentheses inside `safe_divide`.
- A regression test was added: `tests/stg_portwatch_daily_share_bounds.sql`.

Deployment note:

- The code fix exists locally in the repo.
- Rebuild in the new GCP account is still required.

### Stage 3: Monthly Stress and Pattern

Completed in code.

Implemented:

- `models/marts/semantics/mart_chokepoint_monthly_stress.sql`
- `models/marts/semantics/mart_global_monthly_system_stress_summary.sql`
- docs and tests for monthly stress logic

Purpose:

- Page 3 monthly stress comparison
- monthly chokepoint baselines
- MoM change
- event overlay support

### Stage 4: Executive Overview

Completed in code and partially wired in Looker.

Preserved:

- `models/marts/semantics/mart_dashboard_global_trade_overview.sql`

Added:

- `models/marts/semantics/mart_trade_month_coverage_status.sql`
- `models/marts/semantics/mart_executive_monthly_system_snapshot.sql`
- `models/marts/semantics/mart_chokepoint_monthly_stress_detail.sql`

Later refinement:

- display-safe monthly stress fields were added to `mart_chokepoint_monthly_stress_detail`
- raw stress fields were retained unchanged

### Stage 5: Country Exposure and Trade Reliance

Completed in code, not yet fully wired in Looker.

Added:

- `models/marts/semantics/mart_reporter_partner_commodity_month_enriched.sql`
- `models/marts/semantics/mart_reporter_month_exposure_map.sql`
- `models/marts/semantics/mart_chokepoint_monthly_hotspot_map.sql`

Purpose:

- Page 4 country exposure tables
- Page 4 country map
- Page 4 chokepoint hotspot point map

Deployment note:

- These marts were added, documented, and tested in the repo.
- They were not yet shown in the last known Looker screenshots.

### Stage 6: Historical Context

Not started as a dedicated semantic layer stage.

Relevant upstream assets already exist:

- `models/marts/mart_event_impact.sql`
- event dimensions and bridges

But no final Page 6 semantic mart has been built yet.

### Structural Vulnerability Page

Not implemented as a Looker semantic page.

Relevant upstream marts already exist:

- `models/marts/mart_reporter_energy_vulnerability.sql`
- `models/marts/mart_hub_dependency_month.sql`

But the final Page 5 semantic layer and dashboard mapping were not completed.

## Active Semantic Mart Inventory

Current active semantic models in `models/marts/semantics`:

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress`
- `mart_chokepoint_monthly_stress_detail`
- `mart_global_monthly_system_stress_summary`
- `mart_chokepoint_daily_signal`
- `mart_global_daily_market_signal`
- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_chokepoint_monthly_hotspot_map`

## Dashboard Freeze State

Last known Looker Studio report title:

- `reportV2`

Freeze artifacts captured on disk:

- Page 1 screenshot: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.40.24.png`
- Looker data source manager screenshot: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.43.00.png`
- Page 2 screenshot: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.45.38.png`

Last known embedded data sources shown in the screenshots:

- `mart_chokepoint_monthly_stress_detail` alias `ds37`
- `mart_chokepoint_daily_signal` alias `ds55`
- `mart_dashboard_global_trade_overview` alias `ds4`
- `mart_global_daily_market_signal` alias `ds54`
- `mart_chokepoint_daily_signal` alias `ds34`
- `mart_executive_monthly_system_snapshot` alias `ds16`
- `mart_trade_month_coverage_status` alias `ds15`

Important note:

- `mart_chokepoint_daily_signal` appears twice as embedded sources (`ds55` and `ds34`).
- One duplicate appears unused and should be cleaned up during migration.

## Last Known Page Status

### Page 1: Executive Summary

Visible and working in Looker screenshot.

Last visible scorecards:

- Latest Total Trade: `183.1B`
- Reporting Completeness: `25.00%`
- Stress Level: `SEVERE`
- Top Stressed Chokepoint: `Strait of Hormuz`
- Stressed Chokepoints: `2`

Page 1 visuals shown:

- trade trend over time
- reporting completeness over time
- stressed chokepoints score bar chart
- trade reporting data table
- latest chokepoint movement table

### Page 2: Daily System Pulse

Visible and working in Looker screenshot.

Last visible scorecards:

- Daily Coverage Status: `PARTIAL_COVERAGE`
- Observed Chokepoint Count: `2`
- Stress Chokepoint Count: `0`
- System Stress Level: `NORMAL`
- Stressed Chokepoints: `No data`

Page 2 visuals shown:

- trade trend over time
- stressed chokepoints score bar chart
- trade reporting data table
- latest chokepoint movement table

### Page 3

Semantic marts exist in code, but no screenshot-confirmed Looker page state was captured.

### Page 4

Semantic marts exist in code, but no screenshot-confirmed Looker page state was captured.

### Pages 5 and 6

Not productized in the semantic layer / Looker build.

## Current Dashboard Development Stage At Freeze

The last verified state before the GCP credit issue was:

- Page 1 was implemented in dbt and visibly wired in Looker.
- Page 2 was implemented in dbt and visibly wired in Looker.
- Page 3 was implemented in dbt but not frozen in a screenshot-confirmed Looker page.
- Page 4 was implemented in dbt but not frozen in a screenshot-confirmed Looker page.
- Page 5 and Page 6 remained unfinished as final semantic pages.

In practice, the project had reached Stage 5 in code, but the last screenshot-confirmed dashboard freeze only covered Pages 1 and 2.

## Current Deployment Reality

What was true at the time of handoff:

- The old GCP account ran out of credit.
- The repo contains the latest semantic-layer work.
- Some Looker data sources were already connected to the old BigQuery project.
- Page 1 and Page 2 were visibly functioning in Looker Studio.
- The final Stage 5 marts were in code, but not yet confirmed in Looker.
- The daily macro bug fix for `safe_divide` was applied in code after the daily build investigation.

## Known Risks at Handoff

- The repo worktree is dirty and includes many unrelated generated and log changes.
- Not all semantic-layer files may be committed yet.
- Stage 5 marts need a fresh build in the new account before Looker wiring.
- Daily marts require a rebuild after the `safe_divide` macro fix.
- Existing screenshots reflect the old project state, not the future migrated project.
- There are legacy DuckDB and Streamlit assets in the repo; they are not part of the target deployment.

## Recommended Read Order For Restart

1. `PROJECT_STATE.md`
2. `ARCHITECTURE.md`
3. `INFRA_INVENTORY.md`
4. `DASHBOARD_STATUS.md`
5. `DASHBOARD_FIELD_MAP.md`
6. `NEXT_STEPS.md`
7. `CODEX_HANDOVER_PROMPT.md`
