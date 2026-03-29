# CONTRACTS.md

## PortWatch Chokepoint Traffic And Monthly Stress

Status: Draft for Stage A and the first cloud vertical slice

Source system:
- PortWatch ArcGIS REST daily chokepoint service
- PortWatch ArcGIS REST chokepoint lookup service

Purpose:
- Provide a canonical monthly chokepoint traffic and stress dataset that can be joined to trade exposure and event bridges.
- Power the first end-to-end cloud slice: bronze in GCS -> silver parquet in GCS -> BigQuery landing -> dbt marts -> Looker Studio.
- Support dashboard questions such as: which chokepoints are under abnormal traffic stress, which reporters are most exposed, and which months overlap with disruptive events.

First-slice scope:
- Chokepoints included in the initial vertical slice:
  - Suez Canal
  - Strait of Hormuz
  - Panama Canal
  - Cape of Good Hope
  - Bab el-Mandeb Strait
- Local data currently observed for this slice spans `2020-01` through `2026-03`.
- The upstream lookup currently contains 28 PortWatch chokepoints, but the first vertical slice is intentionally limited to the 5 names above.

### Lifecycle By Phase

| Phase | Asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Metadata | `chokepoints_lookup` | one row per `portid` | none | `gs://<bucket>/metadata/portwatch/chokepoints_lookup.parquet` | Reference lookup used to resolve names and keep stable chokepoint ids. |
| Bronze | `portwatch_daily` | one row per `date` x `portid` | `year=YYYY/month=MM/day=DD` | `gs://<bucket>/bronze/portwatch/year=YYYY/month=MM/day=DD/portwatch_chokepoints_daily.parquet` | Append-only raw snapshot from ArcGIS. |
| Silver primary | `portwatch_monthly` | one row per `month_start_date` x `chokepoint_id` | `year=YYYY/month=MM` | `gs://<bucket>/silver/portwatch/portwatch_monthly/year=YYYY/month=MM/portwatch_monthly.parquet` | Canonical monthly fact used for BigQuery landing. Built in Python. |
| Silver auxiliary | `portwatch_month_chokepoint_scaffold` | one row per `month_start_date` x `chokepoint_id` | none for now | `gs://<bucket>/silver/portwatch/portwatch_month_chokepoint_scaffold.parquet` | Completeness scaffold with coverage flags and explicit missing months. |
| Silver auxiliary | `dim_portwatch_chokepoint` | one row per `chokepoint_id` | none | `gs://<bucket>/silver/portwatch/dimensions/dim_portwatch_chokepoint.parquet` | Lookup-style dimension for descriptive joins and QA. |
| Silver auxiliary | `dim_month` | one row per `year_month` | none | `gs://<bucket>/silver/portwatch/dimensions/dim_month.parquet` | Month dimension derived from observed PortWatch months. |
| BigQuery landing | `raw.portwatch_monthly` | one row per `month_start_date` x `chokepoint_id` | partition by `month_start_date`, cluster by `chokepoint_id` | `raw.portwatch_monthly` | Physical monthly PortWatch fact without derived stress metrics. |
| dbt | `stg_portwatch_stress_metrics`, `stg_chokepoint_stress_zscore`, and downstream marts | model-specific | dbt-managed | curated analytics schema | dbt is the canonical home of expanding and rolling PortWatch stress metrics used by exposure and event marts. |
| BI | Looker Studio PortWatch views | monthly analytical views | query partitioned marts | Looker Studio | Latest stress, trend, reporter exposure, and event-context reporting. |

### Canonical Contract Decisions

1. `chokepoint_id` must survive every phase from lookup -> bronze -> silver -> BigQuery. The current notebook drops it from the monthly fact; the script refactor should add it back.
2. `month_start_date` must be present in the canonical monthly fact so BigQuery can partition on a real `DATE` column.
3. Bronze is append-only and auditable. Silver and BigQuery landing must be safe to rerun for any date range by replacing only the affected month partitions.
4. `year_month` stays as the canonical cross-source join key in `YYYY-MM` format, but `month_start_date` is the physical partition key.
5. Missing source coverage must be made explicit. We do not silently impute missing PortWatch months; we expose them through the scaffold asset and coverage flags.
6. Derived stress metrics do not belong in the silver or raw landing fact. They are computed in dbt from `raw.portwatch_monthly` so expanding and rolling windows can be rebuilt consistently after backfills.
7. For the first cloud slice, the only required BigQuery PortWatch landing table is `raw.portwatch_monthly`. Auxiliary scaffold and dimensions may remain in parquet until a downstream need is explicit.

### Primary Business Keys And Refresh Cadence

| Asset | Primary business key | Expected refresh cadence | Rebuild strategy |
| --- | --- | --- | --- |
| `chokepoints_lookup` | `portid` | on every ingest run or whenever lookup changes | full replace |
| `portwatch_daily` | `date`, `portid` | daily scheduled append and historical backfill by date range | write one partition per day; never mutate older bronze rows in place |
| `portwatch_monthly` | `month_start_date`, `chokepoint_id` | daily or scheduled rerun after bronze refresh | recompute and replace only touched month partitions |
| `portwatch_month_chokepoint_scaffold` | `month_start_date`, `chokepoint_id` | same run as monthly fact | recompute for requested month range |
| `dim_portwatch_chokepoint` | `chokepoint_id` | same run as monthly fact | full replace is acceptable at current size |
| `dim_month` | `year_month` | same run as monthly fact | full replace is acceptable at current size |
| `raw.portwatch_monthly` | `month_start_date`, `chokepoint_id` | same run as silver publish | overwrite touched partitions or load with delete-and-insert by partition |
| `stg_portwatch_stress_metrics` | `month_start_date`, `chokepoint_id` | rebuilt after PortWatch monthly backfills or refreshes | recompute derived stress windows from the earliest affected month forward; full rebuild is acceptable at current size |

### Required Columns

#### Bronze: `portwatch_daily`

| Column | Type | Nullability | Notes |
| --- | --- | --- | --- |
| `date` | timestamp | not null | UTC observation date from ArcGIS epoch milliseconds. |
| `portid` | string | not null | Stable upstream chokepoint id. |
| `portname` | string | not null | Human-readable chokepoint name. |
| `n_total` | int64 | not null | Total vessel count for the day. |
| `capacity` | int64 | not null | Total capacity proxy for the day. |
| `n_tanker` | int64 | nullable | Preserved raw subtype metric. |
| `n_container` | int64 | nullable | Preserved raw subtype metric. |
| `n_dry_bulk` | int64 | nullable | Preserved raw subtype metric. |
| `capacity_tanker` | int64 | nullable | Preserved raw subtype metric. |
| `capacity_container` | int64 | nullable | Preserved raw subtype metric. |
| `capacity_dry_bulk` | int64 | nullable | Preserved raw subtype metric. |
| `year` | int64 | not null | Storage partition column. |
| `month` | string | not null | Zero-padded storage partition column. |
| `day` | string | not null | Zero-padded storage partition column. |

Raw pass-through columns such as `n_general_cargo`, `n_roro`, `capacity_general_cargo`, `capacity_roro`, `capacity_cargo`, and `ObjectId` may be retained in bronze, but they are not required by the first vertical slice contract.

#### Silver And BigQuery Landing: `portwatch_monthly`

| Column | Type | Nullability | Notes |
| --- | --- | --- | --- |
| `month_start_date` | date | not null | First day of the calendar month; BigQuery partition key. |
| `year_month` | string | not null | Canonical month key in `YYYY-MM` format. |
| `year` | int64 | not null | Calendar year. |
| `month` | string | not null | Zero-padded month number. |
| `chokepoint_id` | string | not null | Stable chokepoint identifier carried from lookup and bronze. |
| `chokepoint_name` | string | not null | Display name for joins and BI filters. |
| `avg_n_total` | float64 | not null | Average daily vessel count within the month. |
| `max_n_total` | int64 | not null | Maximum daily vessel count within the month. |
| `avg_capacity` | float64 | not null | Average daily capacity proxy within the month. |
| `max_capacity` | int64 | not null | Maximum daily capacity proxy within the month. |
| `avg_n_tanker` | float64 | not null | Average daily tanker count. |
| `avg_n_container` | float64 | not null | Average daily container count. |
| `avg_n_dry_bulk` | float64 | not null | Average daily dry bulk count. |
| `avg_capacity_tanker` | float64 | not null | Average daily tanker capacity proxy. |
| `avg_capacity_container` | float64 | not null | Average daily container capacity proxy. |
| `avg_capacity_dry_bulk` | float64 | not null | Average daily dry bulk capacity proxy. |
| `days_observed` | int64 | not null | Distinct source days observed in the month. |
| `tanker_share` | float64 | not null | Tanker share of tracked vessel classes. |
| `container_share` | float64 | not null | Container share of tracked vessel classes. |
| `dry_bulk_share` | float64 | not null | Dry bulk share of tracked vessel classes. |
#### Silver Auxiliary: `portwatch_month_chokepoint_scaffold`

Required columns:
- `month_start_date` date not null
- `year_month` string not null
- `chokepoint_id` string not null
- `portwatch_source_chokepoint_id` string not null
- `chokepoint_name` string not null
- `has_portwatch_data_flag` int8 not null
- `coverage_gap_flag` int8 not null
- `days_in_month` int16 not null
- `coverage_ratio` float64 not null

The monthly metric columns from `portwatch_monthly` may be null in the scaffold when a month has no PortWatch coverage for that chokepoint.

#### dbt Derived Stress Metrics: `stg_portwatch_stress_metrics`

Required columns:
- `month_start_date` date not null
- `year_month` string not null
- `chokepoint_id` string not null
- `chokepoint_name` string not null
- `z_score_capacity` float64 nullable during warm-up
- `z_score_count` float64 nullable during warm-up
- `z_score_vessel_size` float64 nullable during warm-up
- `stress_index` float64 nullable during warm-up
- `stress_index_weighted` float64 nullable during warm-up
- `stress_index_rolling_6m` float64 nullable during warm-up
- `stress_index_weighted_rolling_6m` float64 nullable during warm-up

`chokepoint_id` in this dbt model is the canonical analytics key derived from `chokepoint_name` so it joins consistently to event and trade exposure marts. `portwatch_source_chokepoint_id` preserves the original raw PortWatch identifier for lineage and backfill audits.

These metrics are derived in dbt from the raw monthly PortWatch fact using expanding point-in-time windows and rolling 6-month companion windows. Backfills are expected to change forward-looking derived rows for the affected chokepoint series.

### Known Data Quality Checks And Failure Actions

| Layer | Check | Blocking | Failure action |
| --- | --- | --- | --- |
| Bronze | Partition integrity: every row in `year=YYYY/month=MM/day=DD` must have the same `date`, `year`, `month`, and `day` as the folder path. | Yes | Fail the asset run and do not publish the partition. |
| Bronze | Required-field completeness: `date`, `portid`, `portname`, `n_total`, and `capacity` must be non-null after type coercion. | Yes | Drop invalid rows during transform; fail the partition if zero valid rows remain. |
| Bronze | Uniqueness at daily grain: no duplicate rows for `date`, `portid`. | Yes | Fail the run for that day and do not continue to silver for affected months. |
| Silver | Uniqueness at monthly grain: exactly one row per `month_start_date`, `chokepoint_id`. | Yes | Fail the silver build and block BigQuery landing. |
| Silver | Coverage sanity: `days_observed` must be between 1 and `days_in_month` for fact rows, and scaffold `coverage_ratio` must stay between 0 and 1. | Yes | Fail the affected month partition and block downstream publish. |
| Silver | Share sanity: `tanker_share + container_share + dry_bulk_share` should equal 1 within a small tolerance when the vessel-class denominator is positive. | No | Emit warning to logs and manifest; continue publish. |
| Landing | Partition alignment: `year_month` must match `month_start_date`, and loaded rows must only touch requested month partitions. | Yes | Abort the load job and keep prior landing partitions unchanged. |
| Downstream | dbt source freshness and schema expectation for `raw.portwatch_monthly`. | Yes | Block dbt marts and BI refresh for that run. |

### Known Gaps And Assumptions

- The current local notebook writes the monthly fact to `data/silver/portwatch/mart_portwatch_chokepoint_stress_monthly/...`. For the cloud vertical slice, the canonical silver asset should be published as `portwatch_monthly` so storage naming matches the BigQuery landing name.
- The current local monthly fact keeps `chokepoint_name` but not `chokepoint_id`. This contract treats that as an interface gap to fix in the script refactor, not as the desired final shape.
- The current local scaffold shows real coverage gaps. Missing months are expected for some chokepoints, especially earlier history and Bab el-Mandeb coverage. These gaps should remain explicit rather than being backfilled with synthetic values.
- At current size, full replacement of the lookup and dimension parquet files is acceptable. The monthly fact and BigQuery landing should still be partition-aware to keep reruns fast and query costs controlled.

### Minimum Run Metadata Required For Safe Reruns

Each callable PortWatch asset run should emit a minimal manifest record with at least:
- `run_id`
- `asset_name`
- `dataset_name`
- `requested_start_date`
- `requested_end_date`
- `requested_chokepoints`
- `status`
- `started_at`
- `finished_at`
- `source_row_count`
- `output_row_count`
- `partitions_written`
- `error_summary`

This is enough to make date-range reruns auditable before adding full observability later.
