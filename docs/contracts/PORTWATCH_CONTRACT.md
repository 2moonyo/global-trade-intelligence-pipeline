# PortWatch Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted through `ingest/portwatch/portwatch_extract.py`
- Canonical daily and monthly silver parquet partitions are implemented through `ingest/portwatch/portwatch_silver.py`
- GCS publish is implemented in `warehouse/publish_portwatch_to_gcs.py`
- BigQuery raw landing is implemented for both daily and monthly PortWatch tables
- dbt staging, stress marts, map marts, and daily/monthly semantic marts consume PortWatch data

## Source Systems

- PortWatch ArcGIS REST daily chokepoint service
- PortWatch ArcGIS REST chokepoint lookup service

## Purpose

- Provide canonical daily and monthly chokepoint traffic signals
- Feed stress modelling, trade exposure, event overlay, and BI-ready chokepoint map/reporting surfaces

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Lookup metadata | `chokepoints_lookup` | one row per `portid` | none | `data/metadata/portwatch/chokepoints_lookup.parquet` | Saved during extract runs; also publishable to GCS. |
| Bronze local | `portwatch_daily` | one row per `date` x `portid` | `year=YYYY/month=MM/day=DD` | `data/bronze/portwatch/year=YYYY/month=MM/day=DD/portwatch_chokepoints_daily.parquet` | Append-only daily extract written by the extractor. |
| Silver daily | `portwatch_daily` | one row per `date_day` x `chokepoint_id` | `year=YYYY/month=MM` | `data/silver/portwatch/portwatch_daily/year=YYYY/month=MM/portwatch_daily.parquet` | Canonical daily cloud asset. |
| Silver monthly | `portwatch_monthly` | one row per `month_start_date` x `chokepoint_id` | `year=YYYY/month=MM` | `data/silver/portwatch/portwatch_monthly/year=YYYY/month=MM/portwatch_monthly.parquet` | Canonical monthly cloud asset. |
| Silver legacy local | `mart_portwatch_chokepoint_stress_monthly` | one row per `year_month` x `chokepoint_name` | `year=YYYY/month=MM` | `data/silver/portwatch/mart_portwatch_chokepoint_stress_monthly/year=YYYY/month=MM/portwatch_chokepoint_stress_monthly.parquet` | Retained for compatibility. |
| Silver auxiliary local | `portwatch_chokepoint_stress_monthly_all` | one row per `month_start_date` x `chokepoint_id` | none | `data/silver/portwatch/portwatch_chokepoint_stress_monthly_all.parquet` | Consolidated compatibility snapshot. |
| Silver auxiliary local | `portwatch_month_chokepoint_scaffold` | one row per `month_start_date` x `chokepoint_id` | none | `data/silver/portwatch/portwatch_month_chokepoint_scaffold.parquet` | Makes coverage gaps explicit. |
| Silver dimensions | `dim_portwatch_chokepoint`, `dim_month` | dimension-specific | none | `data/silver/portwatch/dimensions/*.parquet` | Descriptive joins and QA support. |
| GCS publish | metadata, bronze, daily silver, monthly silver, dimensions, scaffold | same as source asset | preserved from local shape | `gs://<bucket>/<prefix>/...` | Checksum-aware publish with month filters. |
| BigQuery raw landing | `raw.portwatch_daily`, `raw.portwatch_monthly` | table-specific | daily partitioned by `date_day`; monthly partitioned by `month_start_date`; both clustered by `chokepoint_id` | `raw.portwatch_daily`, `raw.portwatch_monthly` | Replaces touched month partitions by default and tracks load state. |
| dbt staging | `stg_portwatch_daily`, `stg_portwatch_stress_metrics`, `stg_chokepoint_stress_zscore` | model-specific | dbt-managed | analytics staging schema | Canonical home of daily and monthly derived stress metrics. |
| dbt marts | exposure, stress, daily signal, monthly signal, and map marts | model-specific | dbt-managed | analytics marts schema | PortWatch is joined to trade, events, geography, and Brent context. |
| Semantic marts | daily and monthly chokepoint/market marts | day, month, or latest-map grain | dbt-managed | analytics marts schema | Current BI-facing PortWatch surfaces. |

## Primary Business Keys And Refresh Strategy

| Asset | Primary key | Current refresh pattern | Rebuild strategy |
| --- | --- | --- | --- |
| bronze `portwatch_daily` | `date`, `portid` | daily or historical date-window rerun | write one bronze day partition at a time |
| silver `portwatch_daily` | `date_day`, `chokepoint_id` | rerun after bronze refresh | replace touched month partitions |
| silver `portwatch_monthly` | `month_start_date`, `chokepoint_id` | rerun after bronze refresh | replace touched month partitions |
| `portwatch_month_chokepoint_scaffold` | `month_start_date`, `chokepoint_id` | same as monthly fact | recompute requested month range |
| BigQuery `raw.portwatch_daily` | `date_day`, `chokepoint_id` | same run as cloud publish/load | delete touched month partitions then append selected months |
| BigQuery `raw.portwatch_monthly` | `month_start_date`, `chokepoint_id` | same run as cloud publish/load | delete touched month partitions then append selected months |
| `stg_portwatch_daily` | `date_day`, `chokepoint_id` | dbt rebuild | daily calendar scaffold and rolling metrics |
| `stg_portwatch_stress_metrics` | `month_start_date`, `chokepoint_id` | dbt rebuild | expanding and rolling metrics recompute forward from the earliest affected month |

## Minimum Required Columns

Bronze `portwatch_daily`:

- `date` timestamp not null
- `portid` string not null
- `portname` string not null
- `n_total` numeric not null
- `capacity` numeric not null
- `year`, `month`, `day` partition fields not null

Silver and raw `portwatch_daily`:

- `date_day`
- `year_month`
- `year`
- `month`
- `day`
- `chokepoint_id`
- `chokepoint_name`
- `n_total`
- `capacity`
- `n_tanker`
- `n_container`
- `n_dry_bulk`
- `capacity_tanker`
- `capacity_container`
- `capacity_dry_bulk`

Silver and raw `portwatch_monthly`:

- `month_start_date`
- `year_month`
- `year`
- `month`
- `chokepoint_id`
- `chokepoint_name`
- `avg_n_total`
- `max_n_total`
- `avg_capacity`
- `max_capacity`
- `avg_n_tanker`
- `avg_n_container`
- `avg_n_dry_bulk`
- `avg_capacity_tanker`
- `avg_capacity_container`
- `avg_capacity_dry_bulk`
- `days_observed`
- `tanker_share`
- `container_share`
- `dry_bulk_share`

Scaffold:

- `month_start_date`, `year_month`, `chokepoint_id`, `chokepoint_name`
- `has_portwatch_data_flag`
- `coverage_gap_flag`
- `days_in_month`
- `coverage_ratio`

## Stress Computation Contract

The canonical stress logic lives in dbt, not in silver or raw landing.

`stg_portwatch_stress_metrics` computes:

```text
vessel_size_index = avg_capacity / avg_n_total

priority_vessel_share =
  tanker_share     for Strait of Hormuz
  container_share  for Suez Canal and Panama Canal
  dry_bulk_share   for all other chokepoints
```

For each `chokepoint_id`, dbt builds a calendar scaffold between the observed first and last month, then calculates:

- expanding means and standard deviations using all prior months only
- rolling 6-month means and standard deviations using the previous 6 months only
- point-in-time z-scores for:
  - capacity
  - vessel count
  - vessel size proxy

Warm-up behavior:

- if fewer than 2 prior observations exist, z-scores remain null
- if the baseline standard deviation is zero, z-scores remain null
- if the scaffolded month has no PortWatch data, derived stress fields remain null

Primary derived measures:

```text
stress_index = 0.5 * z_score_count + 0.5 * z_score_capacity

stress_index_weighted =
  stress_index * (1.0 + 0.5 * priority_vessel_share)

stress_index_rolling_6m =
  0.5 * z_score_count_rolling_6m + 0.5 * z_score_capacity_rolling_6m

stress_index_weighted_rolling_6m =
  stress_index_rolling_6m * (1.0 + 0.5 * priority_vessel_share)
```

Daily signal logic lives in `stg_portwatch_daily`, `mart_chokepoint_daily_signal`, and `mart_global_daily_market_signal`. It exposes daily rolling windows, alert bands, and system-level daily coverage without changing the monthly stress contract.

Justification:

- count captures congestion and intensity of vessel movements
- capacity captures throughput scale
- vessel size captures whether observed stress is being driven by changing ship mix
- the weighted variant biases attention toward the economically dominant vessel class for a chokepoint without discarding total traffic
- using prior-window baselines avoids look-ahead bias in event windows and dashboard interpretation

## Current Operational Logging

Implemented logs and manifests exist for:

- extract: `logs/portwatch/portwatch_extract.log`, `logs/portwatch/portwatch_extract_manifest.jsonl`
- silver: `logs/portwatch/portwatch_silver.log`, `logs/portwatch/portwatch_silver_manifest.jsonl`
- GCS publish: `logs/portwatch/publish_portwatch_to_gcs.log`, `logs/portwatch/publish_portwatch_to_gcs_manifest.jsonl`
- BigQuery load: `logs/portwatch/load_portwatch_to_bigquery.log`, `logs/portwatch/load_portwatch_to_bigquery_manifest.jsonl`

## Downstream Use

- `mart_chokepoint_daily_signal` exposes daily chokepoint operations.
- `mart_global_daily_market_signal` combines daily PortWatch signals with Brent daily context.
- `mart_chokepoint_monthly_stress`, `mart_chokepoint_monthly_stress_detail`, and `mart_global_monthly_system_stress_summary` expose monthly stress.
- `mart_reporter_month_chokepoint_exposure` and `mart_trade_exposure` join trade exposure to PortWatch stress.
- `mart_chokepoint_monthly_hotspot_map` exposes latest-month chokepoint map points and exposure context.
- `mart_reporter_partner_commodity_month_enriched` repeats relevant PortWatch stress context at drilldown grain.

## Known Gaps

- Some compatibility files remain for older local workflows, but the current dbt raw contract is BigQuery `raw.portwatch_daily` and `raw.portwatch_monthly`.
- PortWatch source coverage can be sparse or delayed; semantic marts expose coverage fields rather than hiding missingness.
