# Chokepoint Canonicalization Worklog

Date: 2026-04-20

## Root Cause

Chokepoint IDs were generated independently with `lower(trim(chokepoint_name))` in multiple staging and mart models. This split equivalent business chokepoints into different hashes before `dim_chokepoint` joined raw coordinates.

Confirmed local silver mismatches:

- Raw dim: `Hormuz Strait`; PortWatch/events: `Strait of Hormuz`
- Raw dim/routes: `Bab el-Mandeb`; PortWatch/events: `Bab el-Mandeb Strait`
- Route/raw label `Gibraltar Strait` was already internally consistent, so it was preserved as the canonical business label to avoid downstream churn.

## Files Changed

- `macros/shared_utils.sql`
- `models/staging/stg_dim_chokepoint.sql`
- `models/staging/stg_portwatch_stress_metrics.sql`
- `models/staging/stg_portwatch_daily.sql`
- `models/staging/stg_chokepoint_bridge.sql`
- `models/staging/events/stg_event_month_chokepoint.sql`
- `models/staging/stg_dim_trade_route_geography.sql`
- `models/marts/dimensions/dim_chokepoint.sql`
- `models/marts/dimensions/bridge_event_chokepoint.sql`
- `models/marts/fct_reporter_partner_commodity_route_month.sql`
- `models/marts/fct_reporter_partner_commodity_hub_month.sql`
- `models/marts/mart_trade_exposure.sql`
- `models/marts/mart_reporter_month_chokepoint_exposure.sql`
- `models/marts/semantics/mart_chokepoint_monthly_stress.sql`
- `models/marts/semantics/mart_reporter_partner_commodity_month_enriched.sql`
- `tests/stg_portwatch_stress_metrics_canonical_id.sql`
- `tests/dim_chokepoint_null_coordinates.sql`
- `tests/mart_chokepoint_monthly_hotspot_map_missing_coordinates.sql`
- `tests/mart_chokepoint_monthly_hotspot_map_unmatched_dim_chokepoints.sql`

## Canonical Mappings

- `Hormuz`, `Hormuz Strait`, `Strait of Hormuz` -> `Strait of Hormuz`
- `Bab el-Mandeb Strait`, `Bab el Mandeb`, `Bab-el-Mandeb` variants -> `Bab el-Mandeb`
- `Panama`, whitespace/tab/newline-polluted Panama labels, `Panama Canal` -> `Panama Canal`
- `Malacca`, `Strait of Malacca`, `Malacca Strait` -> `Malacca Strait`
- `Gibraltar`, `Strait of Gibraltar`, `Gibraltar Strait` -> `Gibraltar Strait`
- `Suez` -> `Suez Canal`
- Existing canonical labels for `Cape of Good Hope`, `Turkish Straits`, and `Open Sea` are case-standardized.

## Before/After Counts

Local silver source-Parquet audit:

| Check | Before | After |
| --- | ---: | ---: |
| Distinct cross-source chokepoint IDs | 11 | 9 |
| Simulated `dim_chokepoint` rows | 10 | 8 |
| Simulated `dim_chokepoint` null-coordinate rows | 2 | 0 |
| Simulated hotspot source rows without coordinates | 86 of 218 | 0 of 218 |
| Distinct hotspot names without coordinates | 2 | 0 |

Before missing coordinate labels:

- `Bab el-Mandeb Strait`
- `Strait of Hormuz`

## Validation Notes

- Source-Parquet DuckDB audits confirm the canonical mapping removes the observed coordinate misses.
- Direct `dbt parse`/`dbt compile` attempts hung locally with no useful output; commands were stopped and no repo profile/DuckDB configuration was changed.
- Existing `warehouse/analytics.duckdb` was not removed or modified.

## Remaining Edge Cases

- `chokepoint_sequence_str` can contain multiple route labels in one string and was not rewritten; no downstream ID hashes were found on that field.
- If future sources introduce new aliases, add them only to `canonicalize_chokepoint_name` and keep downstream marts deterministic.
