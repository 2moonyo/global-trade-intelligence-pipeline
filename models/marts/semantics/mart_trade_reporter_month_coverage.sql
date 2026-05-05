-- Dashboard-ready reporter-month trade coverage mart.
-- Grain: one row per reporter_iso3 + month_start_date.

with configured_reporters as (
  -- Keep the dashboard sort aligned with the configured Comtrade reporter batches.
  select 'EUR' as reporter_iso3, 1 as reporter_dashboard_sort_order, 1 as reporter_scope_cohort_sort_order, 'Primary 16 reporters' as reporter_scope_cohort
  union all select 'BGR', 2, 1, 'Primary 16 reporters'
  union all select 'CHN', 3, 1, 'Primary 16 reporters'
  union all select 'FRA', 4, 1, 'Primary 16 reporters'
  union all select 'NLD', 5, 1, 'Primary 16 reporters'
  union all select 'ROU', 6, 1, 'Primary 16 reporters'
  union all select 'ESP', 7, 1, 'Primary 16 reporters'
  union all select 'USA', 8, 1, 'Primary 16 reporters'
  union all select 'RUS', 9, 1, 'Primary 16 reporters'
  union all select 'IND', 10, 1, 'Primary 16 reporters'
  union all select 'ZAF', 11, 1, 'Primary 16 reporters'
  union all select 'EGY', 12, 1, 'Primary 16 reporters'
  union all select 'TUR', 13, 1, 'Primary 16 reporters'
  union all select 'IDN', 14, 1, 'Primary 16 reporters'
  union all select 'BRA', 15, 1, 'Primary 16 reporters'
  union all select 'PAN', 16, 1, 'Primary 16 reporters'
  union all select 'AUS', 17, 2, 'Secondary 16 reporters'
  union all select 'CAN', 18, 2, 'Secondary 16 reporters'
  union all select 'JPN', 19, 2, 'Secondary 16 reporters'
  union all select 'KOR', 20, 2, 'Secondary 16 reporters'
  union all select 'MYS', 21, 2, 'Secondary 16 reporters'
  union all select 'MEX', 22, 2, 'Secondary 16 reporters'
  union all select 'MAR', 23, 2, 'Secondary 16 reporters'
  union all select 'NOR', 24, 2, 'Secondary 16 reporters'
  union all select 'PHL', 25, 2, 'Secondary 16 reporters'
  union all select 'QAT', 26, 2, 'Secondary 16 reporters'
  union all select 'SAU', 27, 2, 'Secondary 16 reporters'
  union all select 'SGP', 28, 2, 'Secondary 16 reporters'
  union all select 'THA', 29, 2, 'Secondary 16 reporters'
  union all select 'ARE', 30, 2, 'Secondary 16 reporters'
  union all select 'GBR', 31, 2, 'Secondary 16 reporters'
  union all select 'VNM', 32, 2, 'Secondary 16 reporters'
),
country_lookup as (
  select
    iso3,
    country_name,
    region,
    subregion,
    row_number() over (
      partition by iso3
      order by
        case when is_country_map_eligible then 0 else 1 end,
        case when is_country_group then 1 else 0 end,
        country_name
    ) as country_rank
  from {{ ref('dim_country') }}
  where iso3 is not null
),
reporter_lookup as (
  select
    country_iso3 as reporter_iso3,
    country_name_looker,
    country_name_raw,
    row_number() over (
      partition by country_iso3
      order by
        case when is_current then 0 else 1 end,
        case when is_map_eligible then 0 else 1 end,
        reporter_code desc
    ) as reporter_rank
  from {{ ref('stg_reporters') }}
  where country_iso3 is not null
),
reporter_trade_importance as (
  select
    {{ canonical_country_iso3('reporter_iso3') }} as reporter_iso3,
    sum(trade_value_usd) as reporter_lifetime_trade_value_usd,
    count(distinct {{ canonical_country_iso3('partner_iso3') }}) as reporter_lifetime_partner_count
  from {{ ref('fct_reporter_partner_commodity_month') }}
  where reporter_iso3 is not null
  group by 1
),
reporter_trade_importance_ranked as (
  select
    reporter_iso3,
    reporter_lifetime_trade_value_usd,
    reporter_lifetime_partner_count,
    row_number() over (
      order by reporter_lifetime_trade_value_usd desc, reporter_lifetime_partner_count desc, reporter_iso3
    ) as reporter_trade_importance_rank
  from reporter_trade_importance
),
configured_reporter_scope as (
  select
    cr.reporter_iso3,
    coalesce(cl.country_name, rl.country_name_looker, rl.country_name_raw, cr.reporter_iso3) as reporter_name,
    cl.region as reporter_region,
    cl.subregion as reporter_subregion,
    cr.reporter_scope_cohort_sort_order,
    cr.reporter_scope_cohort,
    cr.reporter_dashboard_sort_order,
    rti.reporter_trade_importance_rank,
    rti.reporter_lifetime_trade_value_usd,
    rti.reporter_lifetime_partner_count,
    true as expected_reporter_flag
  from configured_reporters as cr
  left join country_lookup as cl
    on cr.reporter_iso3 = cl.iso3
   and cl.country_rank = 1
  left join reporter_lookup as rl
    on cr.reporter_iso3 = rl.reporter_iso3
   and rl.reporter_rank = 1
  left join reporter_trade_importance_ranked as rti
    on cr.reporter_iso3 = rti.reporter_iso3
),
observed_reporters as (
  select distinct
    {{ canonical_country_iso3('reporter_iso3') }} as reporter_iso3
  from {{ ref('fct_reporter_partner_commodity_month') }}
  where reporter_iso3 is not null
),
unexpected_reporters as (
  select
    o.reporter_iso3
  from observed_reporters as o
  left join configured_reporters as cr
    on o.reporter_iso3 = cr.reporter_iso3
  where cr.reporter_iso3 is null
),
unexpected_reporter_scope as (
  select
    ur.reporter_iso3,
    coalesce(cl.country_name, rl.country_name_looker, rl.country_name_raw, ur.reporter_iso3) as reporter_name,
    cl.region as reporter_region,
    cl.subregion as reporter_subregion,
    3 as reporter_scope_cohort_sort_order,
    'Additional observed reporters' as reporter_scope_cohort,
    1000 + coalesce(rti.reporter_trade_importance_rank, 999) as reporter_dashboard_sort_order,
    rti.reporter_trade_importance_rank,
    rti.reporter_lifetime_trade_value_usd,
    rti.reporter_lifetime_partner_count,
    false as expected_reporter_flag
  from unexpected_reporters as ur
  left join country_lookup as cl
    on ur.reporter_iso3 = cl.iso3
   and cl.country_rank = 1
  left join reporter_lookup as rl
    on ur.reporter_iso3 = rl.reporter_iso3
   and rl.reporter_rank = 1
  left join reporter_trade_importance_ranked as rti
    on ur.reporter_iso3 = rti.reporter_iso3
),
reporter_scope as (
  select * from configured_reporter_scope
  union all
  select * from unexpected_reporter_scope
),
month_spine as (
  select distinct
    month_start_date,
    year_month
  from {{ ref('mart_reporter_month_trade_summary') }}
),
trade_detail as (
  select
    {{ canonical_country_iso3('f.reporter_iso3') }} as reporter_iso3,
    dt.month_start_date,
    max(f.year_month) as year_month,
    count(*) as observed_trade_rows,
    count(distinct f.cmd_code) as observed_cmd_count,
    count(distinct {{ canonical_country_iso3('f.partner_iso3') }}) as observed_partner_count,
    count(distinct f.trade_flow) as observed_flow_count,
    sum(f.trade_value_usd) as total_trade_value_usd,
    {{ bool_or("lower(f.trade_flow) like '%import%'") }} as import_reported_flag,
    {{ bool_or("lower(f.trade_flow) like '%export%'") }} as export_reported_flag
  from {{ ref('fct_reporter_partner_commodity_month') }} as f
  left join {{ ref('dim_time') }} as dt
    on f.period = dt.period
  where f.reporter_iso3 is not null
    and dt.month_start_date is not null
  group by 1, 2
),
grid as (
  select
    rs.reporter_iso3,
    rs.reporter_name,
    rs.reporter_region,
    rs.reporter_subregion,
    rs.reporter_scope_cohort_sort_order,
    rs.reporter_scope_cohort,
    rs.reporter_dashboard_sort_order,
    rs.reporter_trade_importance_rank,
    rs.reporter_lifetime_trade_value_usd,
    rs.reporter_lifetime_partner_count,
    rs.expected_reporter_flag,
    ms.month_start_date,
    ms.year_month
  from reporter_scope as rs
  cross join month_spine as ms
),
latest_month as (
  select max(month_start_date) as latest_month_start_date
  from month_spine
)

select
  g.reporter_iso3,
  g.reporter_name,
  g.reporter_region,
  g.reporter_subregion,
  g.reporter_scope_cohort_sort_order,
  g.reporter_scope_cohort,
  g.reporter_dashboard_sort_order,
  g.reporter_trade_importance_rank,
  g.reporter_lifetime_trade_value_usd,
  g.reporter_lifetime_partner_count,
  g.month_start_date,
  g.year_month,
  g.expected_reporter_flag,
  case
    when coalesce(td.observed_trade_rows, 0) > 0 then true
    else false
  end as reported_trade_flag,
  coalesce(td.import_reported_flag, false) as import_reported_flag,
  coalesce(td.export_reported_flag, false) as export_reported_flag,
  coalesce(td.observed_trade_rows, 0) as observed_trade_rows,
  coalesce(td.observed_cmd_count, 0) as observed_cmd_count,
  coalesce(td.observed_partner_count, 0) as observed_partner_count,
  coalesce(td.observed_flow_count, 0) as observed_flow_count,
  coalesce(td.total_trade_value_usd, 0) as total_trade_value_usd,
  case
    when not g.expected_reporter_flag then null
    when coalesce(td.observed_trade_rows, 0) = 0 then 0.0
    when coalesce(td.observed_cmd_count, 0) >= 3 then 1.0
    else 0.6
  end as reporting_coverage_score,
  case
    when not g.expected_reporter_flag then 'Unknown'
    when coalesce(td.observed_trade_rows, 0) = 0 then 'Missing'
    when coalesce(td.observed_cmd_count, 0) >= 3 then 'Good'
    else 'Partial'
  end as reporter_coverage_status,
  case
    when not g.expected_reporter_flag then 'Reporter is outside the configured dashboard reporter scope and is sorted after the curated reporter list.'
    when coalesce(td.observed_trade_rows, 0) = 0
      then 'No trade rows are present for this reporter-month; recent Comtrade gaps may reflect reporting delay rather than real trade collapse.'
    when coalesce(td.observed_cmd_count, 0) < 3
      then 'Reporter-month is present but commodity breadth is thin; treat month-over-month movement as partial coverage.'
    else null
  end as reporter_dashboard_warning,
  case
    when g.month_start_date = lm.latest_month_start_date then true
    else false
  end as latest_month_flag
from grid as g
left join trade_detail as td
  on g.reporter_iso3 = td.reporter_iso3
 and g.month_start_date = td.month_start_date
cross join latest_month as lm
