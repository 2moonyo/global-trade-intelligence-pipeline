-- Descriptive event lead-lag panel for bloc trade and stress context.
-- Grain: one row per event_id + bloc_code + relative_month_offset.
-- This model is intended for descriptive lead-lag analysis and does not identify causality.

with event_base as (
  select
    de.event_id,
    de.event_name,
    de.event_type as event_category,
    de.event_start_date,
    dt.month_start_date as event_start_month
  from {{ ref('dim_event') }} as de
  left join {{ ref('dim_time') }} as dt
    on dt.year_month = {{ year_month_from_date('de.event_start_date') }}
  where de.event_start_date is not null
),
bloc_list as (
  select distinct
    bloc_code,
    bloc_name
  from {{ ref('dim_country_bloc_membership') }}
),
event_chokepoint_set as (
  select distinct
    cb.event_id,
    {{ canonical_chokepoint_id('cb.chokepoint_name') }} as chokepoint_id,
    {{ canonicalize_chokepoint_name('cb.chokepoint_name') }} as chokepoint_name
  from {{ ref('stg_chokepoint_bridge') }} as cb
  where cb.chokepoint_name is not null
),
event_chokepoint_identity as (
  select
    event_id,
    count(distinct chokepoint_id) as distinct_chokepoint_count,
    min(chokepoint_id) as single_chokepoint_id,
    min(chokepoint_name) as single_chokepoint_name
  from event_chokepoint_set
  group by 1
),
event_bridge_monthly as (
  select
    event_id,
    month_start_date,
    max(severity_weight) as event_severity_weight
  from {{ ref('stg_chokepoint_bridge') }}
  group by 1, 2
),
event_month_spine as (
  select
    eb.event_id,
    eb.event_name,
    eb.event_category,
    eb.event_start_date,
    eb.event_start_month,
    dt.month_start_date,
    dt.year_month,
    cast(
      (
        extract(year from dt.month_start_date) - extract(year from eb.event_start_month)
      ) * 12
      + (
        extract(month from dt.month_start_date) - extract(month from eb.event_start_month)
      ) as {{ dbt.type_int() }}
    ) as relative_month_offset
  from event_base as eb
  inner join {{ ref('dim_time') }} as dt
    on dt.month_start_date between {{ date_add_months('eb.event_start_month', -6) }}
      and {{ date_add_months('eb.event_start_month', 6) }}
),
event_month_chokepoint_stress as (
  select
    ems.event_id,
    ems.month_start_date,
    ecs.chokepoint_id,
    ms.stress_index,
    ms.stress_index_weighted,
    abs(ms.z_score_historical) as abs_z_score_historical
  from event_month_spine as ems
  left join event_chokepoint_set as ecs
    on ems.event_id = ecs.event_id
  left join {{ ref('mart_chokepoint_monthly_stress') }} as ms
    on ecs.chokepoint_id = ms.chokepoint_id
   and ems.month_start_date = ms.month_start_date
),
event_month_stress_agg as (
  select
    event_id,
    month_start_date,
    avg(stress_index) as avg_stress_index,
    avg(stress_index_weighted) as avg_stress_index_weighted,
    max(abs_z_score_historical) as max_abs_z_score_historical,
    count(distinct chokepoint_id) as linked_chokepoint_count,
    count(distinct case when stress_index is not null then chokepoint_id end) as stress_observed_chokepoint_count
  from event_month_chokepoint_stress
  group by 1, 2
),
event_bloc_panel as (
  select
    ems.event_id,
    ems.event_name,
    ems.event_category,
    ems.event_start_date,
    ems.event_start_month,
    case
      when coalesce(eci.distinct_chokepoint_count, 0) = 1 then eci.single_chokepoint_id
      when coalesce(eci.distinct_chokepoint_count, 0) > 1 then 'MULTIPLE_CHOKEPOINTS'
      else null
    end as chokepoint_id,
    case
      when coalesce(eci.distinct_chokepoint_count, 0) = 1 then eci.single_chokepoint_name
      when coalesce(eci.distinct_chokepoint_count, 0) > 1 then 'Multiple chokepoints'
      else null
    end as chokepoint_name,
    bl.bloc_code,
    bl.bloc_name,
    ems.relative_month_offset,
    case
      when ems.relative_month_offset < 0 then 'Pre-event'
      when ems.relative_month_offset = 0 then 'Event month'
      else 'Post-event'
    end as relative_period_label,
    ems.month_start_date,
    ems.year_month,
    bm.bloc_total_trade_value_usd,
    bm.bloc_food_trade_value_usd,
    bm.bloc_oil_trade_value_usd,
    bm.bloc_energy_trade_value_usd,
    bm.brent_price_usd,
    bm.brent_mom_change,
    sa.avg_stress_index,
    sa.avg_stress_index_weighted,
    sa.max_abs_z_score_historical,
    ebm.event_severity_weight,
    sa.linked_chokepoint_count,
    sa.stress_observed_chokepoint_count
  from event_month_spine as ems
  cross join bloc_list as bl
  left join event_chokepoint_identity as eci
    on ems.event_id = eci.event_id
  left join event_month_stress_agg as sa
    on ems.event_id = sa.event_id
   and ems.month_start_date = sa.month_start_date
  left join event_bridge_monthly as ebm
    on ems.event_id = ebm.event_id
   and ems.month_start_date = ebm.month_start_date
  left join {{ ref('mart_bloc_month_trade_macro_summary') }} as bm
    on bl.bloc_code = bm.bloc_code
   and ems.month_start_date = bm.month_start_date
),
with_baselines as (
  select
    *,
    avg(case when relative_month_offset between -6 and -1 then bloc_total_trade_value_usd end) over (
      partition by event_id, bloc_code
    ) as baseline_total_trade_value_usd,
    avg(case when relative_month_offset between -6 and -1 then bloc_food_trade_value_usd end) over (
      partition by event_id, bloc_code
    ) as baseline_food_trade_value_usd,
    avg(case when relative_month_offset between -6 and -1 then bloc_oil_trade_value_usd end) over (
      partition by event_id, bloc_code
    ) as baseline_oil_trade_value_usd,
    avg(case when relative_month_offset between -6 and -1 then brent_price_usd end) over (
      partition by event_id, bloc_code
    ) as baseline_brent_price_usd,
    avg(case when relative_month_offset between -6 and -1 then avg_stress_index_weighted end) over (
      partition by event_id, bloc_code
    ) as baseline_stress_index_weighted
  from event_bloc_panel
)

select
  event_id,
  event_name,
  event_category,
  event_start_date,
  event_start_month,
  chokepoint_id,
  chokepoint_name,
  bloc_code,
  bloc_name,
  relative_month_offset,
  relative_period_label,
  month_start_date,
  year_month,
  bloc_total_trade_value_usd,
  bloc_food_trade_value_usd,
  bloc_oil_trade_value_usd,
  bloc_energy_trade_value_usd,
  brent_price_usd,
  brent_mom_change,
  avg_stress_index,
  avg_stress_index_weighted,
  max_abs_z_score_historical,
  event_severity_weight,
  linked_chokepoint_count,
  stress_observed_chokepoint_count,
  baseline_total_trade_value_usd,
  baseline_food_trade_value_usd,
  baseline_oil_trade_value_usd,
  baseline_brent_price_usd,
  baseline_stress_index_weighted,
  {{ safe_divide(
    'bloc_total_trade_value_usd - baseline_total_trade_value_usd',
    'baseline_total_trade_value_usd'
  ) }} as total_trade_vs_pre_event_baseline_pct,
  {{ safe_divide(
    'bloc_food_trade_value_usd - baseline_food_trade_value_usd',
    'baseline_food_trade_value_usd'
  ) }} as food_trade_vs_pre_event_baseline_pct,
  {{ safe_divide(
    'bloc_oil_trade_value_usd - baseline_oil_trade_value_usd',
    'baseline_oil_trade_value_usd'
  ) }} as oil_trade_vs_pre_event_baseline_pct,
  {{ safe_divide(
    'brent_price_usd - baseline_brent_price_usd',
    'baseline_brent_price_usd'
  ) }} as brent_vs_pre_event_baseline_pct,
  case
    when avg_stress_index_weighted is null or baseline_stress_index_weighted is null then null
    else avg_stress_index_weighted - baseline_stress_index_weighted
  end as stress_vs_pre_event_baseline_delta
from with_baselines
