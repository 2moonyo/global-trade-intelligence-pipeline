-- Dashboard-ready reporter-month trade coverage mart.
-- Grain: one row per reporter_iso3 + month_start_date.

with eligible_reporters as (
  select
    iso3 as reporter_iso3,
    country_name as reporter_name,
    region as reporter_region,
    subregion as reporter_subregion
  from {{ ref('dim_country') }}
  where is_country_map_eligible
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
  left join eligible_reporters as er
    on o.reporter_iso3 = er.reporter_iso3
  where er.reporter_iso3 is null
),
reporter_scope as (
  select
    reporter_iso3,
    reporter_name,
    reporter_region,
    reporter_subregion,
    true as expected_reporter_flag
  from eligible_reporters

  union all

  select
    ur.reporter_iso3,
    coalesce(dc.country_name, ur.reporter_iso3) as reporter_name,
    dc.region as reporter_region,
    dc.subregion as reporter_subregion,
    false as expected_reporter_flag
  from unexpected_reporters as ur
  left join {{ ref('dim_country') }} as dc
    on ur.reporter_iso3 = dc.iso3
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
    when not g.expected_reporter_flag then 'Reporter is outside the expected dashboard reporter scope.'
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
