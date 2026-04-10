# Dashboard Status

Last updated: 2026-04-10

This file freezes the last known Looker Studio state before migration to a new GCP account.

## Freeze Artifact Paths

These local files are the visual freeze artifacts for the report:

- Page 1 Executive Summary screenshot: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.40.24.png`
- Looker data source manager screenshot: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.43.00.png`
- Page 2 Daily System Pulse screenshot: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.45.38.png`

## Report Freeze

Report name shown in screenshot:

- `reportV2`

Theme state shown:

- dark theme
- minimal blue accent for bars and lines

## Embedded Data Sources At Freeze Time

The screenshot of the data source manager showed these embedded sources:

| Data source | Alias | Status | Used in report |
| --- | --- | --- | --- |
| `mart_chokepoint_monthly_stress_detail` | `ds37` | Working | 4 charts |
| `mart_chokepoint_daily_signal` | `ds55` | Working | 0 charts |
| `mart_dashboard_global_trade_overview` | `ds4` | Working | 5 charts |
| `mart_global_daily_market_signal` | `ds54` | Working | 5 charts |
| `mart_chokepoint_daily_signal` | `ds34` | Working | 0 charts |
| `mart_executive_monthly_system_snapshot` | `ds16` | Working | 3 charts |
| `mart_trade_month_coverage_status` | `ds15` | Working | 3 charts |

Observations:

- `mart_chokepoint_daily_signal` was embedded twice.
- The screenshots suggest Page 1 and Page 2 were using a compact set of semantic marts, not blends across many sources.
- Stage 5 Page 4 marts were not yet present in the embedded source list.

## Page Status Matrix

| Page | Status in code | Status in Looker | Notes |
| --- | --- | --- | --- |
| Page 1 Executive Summary | Complete | Visible in screenshot | Primary executive page present |
| Page 2 Daily System Pulse | Complete | Visible in screenshot | Daily page present |
| Page 3 Monthly Stress and Pattern | Complete in code | Not confirmed in screenshot | Marts ready, page not frozen |
| Page 4 Country Exposure and Trade Reliance | Complete in code | Not confirmed in screenshot | Marts ready, page not frozen |
| Page 5 Structural Vulnerability | Partial upstream only | Not built | Existing upstream marts but no final semantic page |
| Page 6 Historical Event / Trade Context | Partial upstream only | Not built | Existing event assets but no final semantic page |

## Page 1 Freeze

Title shown:

- `Executive Summary`

Visible scorecards:

- Latest Total Trade: `183.1B`
- Reporting Completeness: `25.00%`
- Stress Level: `SEVERE`
- Top Stressed Chokepoint: `Strait of Hormuz`
- Stressed Chokepoints: `2`

Visible chart/table blocks:

- Trade Trend Over Time (USD)
- Reporting Completeness Over Time
- Stressed Chokepoints Score
- Trade Reporting Data
- Latest Chokepoint Movement

Observed behavior:

- The trade trend chart explicitly shows incomplete recent months.
- The stress bar chart uses the display-safe capped stress field.
- The bottom-right table uses stress level, stress direction, raw/capped stress metrics, and active event context.

Page 1 data source contract at freeze:

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress_detail`

## Page 2 Freeze

Title shown:

- `Daily System Pulse`

Visible scorecards:

- Daily Coverage Status: `PARTIAL_COVERAGE`
- Observed Chokepoint Count: `2`
- Stress Chokepoint Count: `0`
- System Stress Level: `NORMAL`
- Stressed Chokepoints: `No data`

Visible chart/table blocks:

- Trade Trend Over Time (USD)
- Stressed Chokepoints Score
- Trade Reporting Data
- Latest Chokepoint Movement

Observed behavior:

- Only 2 chokepoints were observed in the active daily range shown.
- The page was clearly reflecting the known PortWatch freshness asymmetry rather than hiding it.
- The stressed chokepoints bar chart uses the capped display score.
- The top-right scorecards are driven by the one-row-per-day global daily mart.

Page 2 data source contract at freeze:

- `mart_global_daily_market_signal`
- `mart_chokepoint_daily_signal`
- the same trade overview sources appear to have been reused for the supporting trade visuals

## Page 3 Status At Freeze

No screenshot was captured for Page 3.

Page 3 should later be wired from:

- `mart_chokepoint_monthly_stress`
- `mart_global_monthly_system_stress_summary`

## Page 4 Status At Freeze

No screenshot was captured for Page 4.

Page 4 should later be wired from:

- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_chokepoint_monthly_hotspot_map`

Important design freeze:

- the primary analytical map belongs on Page 4
- a default route-line global map remains deferred

## Page 5 Status At Freeze

Not built in Looker.

Relevant upstream assets exist:

- `mart_reporter_energy_vulnerability`
- `mart_hub_dependency_month`

But the final Page 5 semantic layer and visual design remain unfinished.

## Page 6 Status At Freeze

Not built in Looker.

Relevant upstream assets exist:

- event dimensions and bridges
- `mart_event_impact`

But the final semantic mart for event-month-country-trade context remains unfinished.

## Dashboard State Summary

What was visibly working before migration:

- Page 1
- Page 2
- embedded BigQuery sources for those pages

What was implemented in code but not yet frozen in Looker:

- Page 3 marts
- Page 4 marts

What remains unfinished:

- Page 5
- Page 6
- cleanup of duplicate embedded data sources
- final map wiring and validation in the new cloud account

## Migration Interpretation

Treat the screenshot state as the last trusted visual freeze, and treat the repo as the last trusted semantic logic freeze.

That split matters because:

- Page 1 and Page 2 have screenshot evidence and embedded source evidence.
- Page 3 and Page 4 have code, tests, and docs, but no screenshot-confirmed Looker freeze.
- The next cloud account should restore Page 1 and Page 2 first, then wire Pages 3 and 4, then continue into Pages 5 and 6.
