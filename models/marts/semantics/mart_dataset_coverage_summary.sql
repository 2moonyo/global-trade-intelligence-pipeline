-- Dashboard-ready dataset coverage and freshness summary mart.
-- Grain: one row per dataset_name.

with dataset_settings as (
  select 'comtrade' as dataset_name, 'trade' as dataset_family, 'reporter-partner-commodity month' as grain_description, 'monthly' as expected_frequency, 60 as expected_lag_days
  union all
  select 'portwatch_daily', 'maritime', 'date_day + chokepoint_id', 'daily', 7
  union all
  select 'portwatch_monthly', 'maritime', 'month_start_date + chokepoint_id', 'monthly', 35
  union all
  select 'brent_daily', 'macro_market', 'trading day + benchmark', 'daily', 7
  union all
  select 'brent_monthly', 'macro_market', 'month_start_date + benchmark', 'monthly', 35
  union all
  select 'fx_monthly', 'macro_market', 'year_month + currency pair view', 'monthly', 35
  union all
  select 'worldbank_energy', 'structural_energy', 'reporter-year-indicator broadcast to January month', 'annual', 400
  union all
  select 'events', 'curated_events', 'event start month', 'event_driven', 30
),
monthly_rollup as (
  select
    dataset_name,
    max(dataset_family) as dataset_family,
    count(*) as expected_period_count,
    countif(observed_records > 0) as observed_period_count,
    {{ safe_divide('countif(observed_records > 0)', 'count(*)') }} as row_presence_score,
    avg(coalesce(coverage_ratio, 0)) as average_coverage_ratio,
    max(case when month_start_date = max_month_start_date then coverage_ratio end) as expected_scope_score
  from (
    select
      m.*,
      max(month_start_date) over (partition by dataset_name) as max_month_start_date
    from {{ ref('mart_dataset_coverage_monthly') }} as m
  )
  group by 1
),
source_stats as (
  select
    'comtrade' as dataset_name,
    min(ref_date) as first_data_date,
    max(ref_date) as latest_data_date,
    max({{ month_start_from_year_month('year_month') }}) as latest_data_month
  from {{ ref('stg_comtrade_fact') }}

  union all

  select
    'portwatch_daily' as dataset_name,
    min(date_day) as first_data_date,
    max(date_day) as latest_data_date,
    max(month_start_date) as latest_data_month
  from {{ ref('stg_portwatch_daily') }}

  union all

  select
    'portwatch_monthly' as dataset_name,
    min(month_start_date) as first_data_date,
    max(month_start_date) as latest_data_date,
    max(month_start_date) as latest_data_month
  from {{ ref('stg_portwatch_stress_metrics') }}

  union all

  select
    'brent_daily' as dataset_name,
    min(date_day) as first_data_date,
    max(date_day) as latest_data_date,
    max({{ month_start_from_year_month('year_month') }}) as latest_data_month
  from {{ ref('stg_brent_daily') }}

  union all

  select
    'brent_monthly' as dataset_name,
    min(month_start_date) as first_data_date,
    max(month_start_date) as latest_data_date,
    max(month_start_date) as latest_data_month
  from {{ ref('stg_brent_monthly') }}

  union all

  select
    'fx_monthly' as dataset_name,
    min(month_start_date) as first_data_date,
    max(month_start_date) as latest_data_date,
    max(month_start_date) as latest_data_month
  from {{ ref('stg_fx_monthly') }}

  union all

  select
    'worldbank_energy' as dataset_name,
    min(dt) as first_data_date,
    max(dt) as latest_data_date,
    max({{ month_start_from_year_month('year_month') }}) as latest_data_month
  from {{ ref('stg_energy_vulnerability') }}

  union all

  select
    'events' as dataset_name,
    min(event_start_date) as first_data_date,
    max(event_start_date) as latest_data_date,
    max(date_trunc(event_start_date, month)) as latest_data_month
  from {{ ref('dim_event') }}
),
dataset_pipeline_map as (
  select 'comtrade' as dataset_name, 'comtrade' as pipeline_dataset_name
  union all
  select 'portwatch_daily', 'portwatch'
  union all
  select 'portwatch_monthly', 'portwatch'
  union all
  select 'brent_daily', 'brent'
  union all
  select 'brent_monthly', 'brent'
  union all
  select 'fx_monthly', 'fx'
  union all
  select 'worldbank_energy', 'worldbank_energy'
  union all
  select 'events', 'events'
),
ops_loaded as (
  select
    dpm.dataset_name,
    max(coalesce(pr.finished_at, pr.started_at, pr.recorded_at)) as latest_loaded_at
  from dataset_pipeline_map as dpm
  left join {{ ref('stg_ops_pipeline_run') }} as pr
    on lower(pr.dataset_name) = dpm.pipeline_dataset_name
  group by 1
),
raw_loaded as (
  select
    'brent_daily' as dataset_name,
    max(load_ts) as latest_loaded_at
  from {{ ref('stg_brent_daily') }}

  union all

  select
    'worldbank_energy' as dataset_name,
    max({{ safe_cast('ingest_ts', 'timestamp') }}) as latest_loaded_at
  from {{ ref('stg_energy_vulnerability') }}
),
latest_loaded as (
  select
    ds.dataset_name,
    case
      when rl.latest_loaded_at is null then ol.latest_loaded_at
      when ol.latest_loaded_at is null then rl.latest_loaded_at
      else greatest(rl.latest_loaded_at, ol.latest_loaded_at)
    end as latest_loaded_at
  from dataset_settings as ds
  left join ops_loaded as ol
    on ds.dataset_name = ol.dataset_name
  left join raw_loaded as rl
    on ds.dataset_name = rl.dataset_name
),
scored as (
  select
    ds.dataset_name,
    ds.dataset_family,
    ds.grain_description,
    ds.expected_frequency,
    ds.expected_lag_days,
    ss.first_data_date,
    ss.latest_data_date,
    ss.latest_data_month,
    ll.latest_loaded_at,
    mr.expected_period_count,
    mr.observed_period_count,
    greatest(least(coalesce(mr.row_presence_score, 0), 1), 0) as row_presence_score,
    case
      when ss.latest_data_date is null then 0.0
      when date_diff(current_date(), ss.latest_data_date, day) <= ds.expected_lag_days then 1.0
      when date_diff(current_date(), ss.latest_data_date, day) <= (ds.expected_lag_days * 2) then 0.7
      when date_diff(current_date(), ss.latest_data_date, day) <= (ds.expected_lag_days * 4) then 0.4
      else 0.1
    end as freshness_score,
    greatest(least(coalesce(mr.expected_scope_score, 0), 1), 0) as expected_scope_score
  from dataset_settings as ds
  left join monthly_rollup as mr
    on ds.dataset_name = mr.dataset_name
  left join source_stats as ss
    on ds.dataset_name = ss.dataset_name
  left join latest_loaded as ll
    on ds.dataset_name = ll.dataset_name
)

select
  dataset_name,
  dataset_family,
  grain_description,
  expected_frequency,
  expected_lag_days,
  first_data_date,
  latest_data_date,
  latest_data_month,
  latest_loaded_at,
  expected_period_count,
  observed_period_count,
  row_presence_score,
  freshness_score,
  expected_scope_score,
  (
    0.50 * row_presence_score
    + 0.30 * freshness_score
    + 0.20 * expected_scope_score
  ) as coverage_score,
  (
    0.50 * row_presence_score
    + 0.30 * freshness_score
    + 0.20 * expected_scope_score
  ) as overall_coverage_score,
  case
    when (
      0.50 * row_presence_score
      + 0.30 * freshness_score
      + 0.20 * expected_scope_score
    ) >= 0.90 then 'Good'
    when (
      0.50 * row_presence_score
      + 0.30 * freshness_score
      + 0.20 * expected_scope_score
    ) >= 0.70 then 'Partial'
    when (
      0.50 * row_presence_score
      + 0.30 * freshness_score
      + 0.20 * expected_scope_score
    ) >= 0.40 then 'Weak'
    else 'Poor'
  end as coverage_status,
  case
    when latest_data_date is null then 'Unknown'
    when date_diff(current_date(), latest_data_date, day) <= expected_lag_days then 'Current'
    when date_diff(current_date(), latest_data_date, day) <= (expected_lag_days * 2) then 'Lagging'
    else 'Stale'
  end as freshness_status,
  case
    when latest_data_date is null then true
    when (
      0.50 * row_presence_score
      + 0.30 * freshness_score
      + 0.20 * expected_scope_score
    ) < 0.90 then true
    when date_diff(current_date(), latest_data_date, day) > expected_lag_days then true
    else false
  end as warning_flag,
  case
    when dataset_name = 'comtrade'
      and date_diff(current_date(), latest_data_date, day) > expected_lag_days
      then 'Comtrade is lagging its expected refresh window; recent declines may reflect official reporting delay rather than economic movement.'
    when dataset_name = 'comtrade'
      and (
        0.50 * row_presence_score
        + 0.30 * freshness_score
        + 0.20 * expected_scope_score
      ) < 0.90
      then 'Comtrade coverage is partial; some reporter-months are missing or thin and should be interpreted as incomplete public reporting.'
    when dataset_name in ('portwatch_daily', 'portwatch_monthly')
      and (
        0.50 * row_presence_score
        + 0.30 * freshness_score
        + 0.20 * expected_scope_score
      ) < 0.90
      then 'PortWatch coverage is partial or sparse; the dashboard preserves null days and chokepoint gaps instead of imputing throughput.'
    when dataset_name in ('brent_daily', 'brent_monthly', 'fx_monthly')
      and date_diff(current_date(), latest_data_date, day) > expected_lag_days
      then 'Macro market context is stale relative to the expected refresh window.'
    when dataset_name = 'worldbank_energy'
      and (
        0.50 * row_presence_score
        + 0.30 * freshness_score
        + 0.20 * expected_scope_score
      ) < 0.90
      then 'World Bank energy is annual and broadcast to month grain; missingness reflects structural annual coverage rather than monthly measurement.'
    when dataset_name = 'events'
      and date_diff(current_date(), latest_data_date, day) > expected_lag_days
      then 'Curated events data is stale relative to the expected update window; absence of new records is not proof of calm.'
    else null
  end as dashboard_warning
from scored
