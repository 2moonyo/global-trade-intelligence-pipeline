# PortWatch Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Implemented locally end to end
- First dataset provisioned for GCS and BigQuery
- Current Streamlit consumer exists

## Source Systems

- PortWatch ArcGIS REST daily chokepoint service
- PortWatch ArcGIS REST chokepoint lookup service

## Purpose

- Provide canonical monthly chokepoint traffic to feed stress modelling, trade exposure, and event-impact analysis
- Act as the first cloud-ready vertical slice

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Lookup metadata | `chokepoints_lookup` | one row per `portid` | none | `data/metadata/portwatch/chokepoints_lookup.parquet` | Saved during extract runs; also publishable to GCS. |
| Bronze local | `portwatch_daily` | one row per `date` x `portid` | `year=YYYY/month=MM/day=DD` | `data/bronze/portwatch/year=YYYY/month=MM/day=DD/portwatch_chokepoints_daily.parquet` | Append-only daily extract written by `ingest/portwatch/portwatch_extract.py`. |
| Silver legacy local | `mart_portwatch_chokepoint_stress_monthly` | one row per `year_month` x `chokepoint_name` | `year=YYYY/month=MM` | `data/silver/portwatch/mart_portwatch_chokepoint_stress_monthly/year=YYYY/month=MM/portwatch_chokepoint_stress_monthly.parquet` | Retained for local compatibility. |
| Silver canonical local | `portwatch_monthly` | one row per `month_start_date` x `chokepoint_id` | `year=YYYY/month=MM` | `data/silver/portwatch/portwatch_monthly/year=YYYY/month=MM/portwatch_monthly.parquet` | This is the contractual monthly asset for the cloud path. |
| Silver auxiliary local | `portwatch_chokepoint_stress_monthly_all` | one row per `month_start_date` x `chokepoint_id` | none | `data/silver/portwatch/portwatch_chokepoint_stress_monthly_all.parquet` | Combined monthly table still used by local DuckDB bootstrap. |
| Silver auxiliary local | `portwatch_month_chokepoint_scaffold` | one row per `month_start_date` x `chokepoint_id` | none | `data/silver/portwatch/portwatch_month_chokepoint_scaffold.parquet` | Makes coverage gaps explicit. |
| Silver dimensions | `dim_portwatch_chokepoint`, `dim_month` | dimension-specific | none | `data/silver/portwatch/dimensions/*.parquet` | Descriptive joins and QA support. |
| GCS publish | metadata, bronze, silver, dimensions, scaffold | same as source asset | preserved from local shape | `gs://<bucket>/<prefix>/...` | Implemented in `warehouse/publish_portwatch_to_gcs.py`. |
| DuckDB raw landing | `raw.portwatch_monthly` | one row per `month_start_date` x `chokepoint_id` | DuckDB table | `raw.portwatch_monthly` | Loaded today from `data/silver/portwatch/portwatch_chokepoint_stress_monthly_all.parquet`. |
| BigQuery raw landing | `raw.portwatch_monthly` | one row per `month_start_date` x `chokepoint_id` | partition by `month_start_date`, cluster by `chokepoint_id` | `raw.portwatch_monthly` | Implemented in `warehouse/load_portwatch_to_bigquery.py`. |
| dbt staging | `stg_portwatch_stress_metrics`, `stg_chokepoint_stress_zscore` | model-specific | dbt-managed | analytics schemas | Canonical home of derived stress metrics. |
| dbt marts | `mart_reporter_month_chokepoint_exposure`, `mart_trade_exposure`, `mart_event_impact`, `mart_reporter_month_chokepoint_exposure_with_brent` | model-specific | dbt-managed | analytics schemas | PortWatch is joined to trade, events, and Brent context here. |
| Dashboard | Streamlit pages 1, 3, and 4 | reporter-month or event grain | page-level query filters | `app/` | Current implemented consumer. |
| Target BI | Looker Studio | page-specific | query BigQuery/dbt models | future | Intended cloud serving target, not the current local implementation. |

## Primary Business Keys And Refresh Strategy

| Asset | Primary key | Current refresh pattern | Rebuild strategy |
| --- | --- | --- | --- |
| `portwatch_daily` | `date`, `portid` | daily or historical date-window rerun | write one bronze day partition at a time |
| `portwatch_monthly` | `month_start_date`, `chokepoint_id` | rerun after bronze refresh | replace only touched month partitions |
| `portwatch_month_chokepoint_scaffold` | `month_start_date`, `chokepoint_id` | same as monthly fact | recompute requested month range |
| DuckDB `raw.portwatch_monthly` | `month_start_date`, `chokepoint_id` | local bootstrap reload | full table replace is current local behavior |
| BigQuery `raw.portwatch_monthly` | `month_start_date`, `chokepoint_id` | same run as cloud publish/load | delete touched partitions then append selected months |
| `stg_portwatch_stress_metrics` | `month_start_date`, `chokepoint_id` | dbt rebuild | expanding and rolling metrics recompute forward from the earliest affected month |

## Minimum Required Columns

Bronze `portwatch_daily`:

- `date` timestamp not null
- `portid` string not null
- `portname` string not null
- `n_total` numeric not null
- `capacity` numeric not null
- `year`, `month`, `day` partition fields not null

Silver and landing `portwatch_monthly`:

- `month_start_date` date not null
- `year_month` string not null
- `year`, `month` not null
- `chokepoint_id` string not null
- `chokepoint_name` string not null
- `avg_n_total`, `avg_capacity` numeric not null
- `max_n_total`, `max_capacity` numeric not null
- `days_observed` integer not null
- `tanker_share`, `container_share`, `dry_bulk_share` numeric nullable only when the denominator is absent

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

Justification:

- count captures congestion and intensity of vessel movements
- capacity captures throughput scale
- vessel size captures whether observed stress is being driven by changing ship mix
- the weighted variant biases attention toward the economically dominant vessel class for a chokepoint without discarding total traffic
- using expanding prior-month baselines avoids look-ahead bias in event windows and dashboard interpretation

## Current Operational Logging

Implemented logs and manifests exist for:

- extract: `logs/portwatch/portwatch_extract.log`, `logs/portwatch/portwatch_extract_manifest.jsonl`
- silver: `logs/portwatch/portwatch_silver.log` and `logs/portwatch/portwatch_silver_manifest.jsonl`
- GCS publish: `logs/portwatch/publish_portwatch_to_gcs.log` and `logs/portwatch/publish_portwatch_to_gcs_manifest.jsonl`
- BigQuery load: `logs/portwatch/load_portwatch_to_bigquery.log` and `logs/portwatch/load_portwatch_to_bigquery_manifest.jsonl`

## Downstream Use

- Executive Overview uses the reporter exposure mart derived from PortWatch stress.
- Chokepoint Stress & Exposure uses `analytics_staging.stg_portwatch_stress_metrics` directly plus the reporter exposure mart.
- Events & Commodity Impact uses PortWatch z-scores through `stg_chokepoint_stress_zscore` and `mart_event_impact`.

## Known Gaps

- Local DuckDB bootstrap still points to the combined compatibility file `portwatch_chokepoint_stress_monthly_all.parquet`, not directly to the canonical partitioned `portwatch_monthly` folder.
- PortWatch is the only dataset that has been provisioned through GCS and BigQuery so far.
