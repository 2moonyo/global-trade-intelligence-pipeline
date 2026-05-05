-- Dashboard-ready bloc-month reporter coverage mart.
-- Grain: one row per bloc_code + month_start_date.
-- Important: rows are not additive across blocs because countries may belong to multiple blocs.

with scoped_bloc_membership as (
  select
    iso3,
    country_name,
    bloc_code,
    bloc_name,
    bloc_type
  from {{ ref('dim_country_bloc_membership') }}
  where bloc_code in ('OPEC', 'BRICS', 'G7', 'EU', 'OECD', 'WESTERN_ALIGNED_PROXY')
),
month_spine as (
  select distinct
    month_start_date,
    year_month
  from {{ ref('mart_trade_reporter_month_coverage') }}
),
member_month_grid as (
  select
    bm.iso3,
    bm.country_name,
    bm.bloc_code,
    bm.bloc_name,
    bm.bloc_type,
    ms.month_start_date,
    ms.year_month
  from scoped_bloc_membership as bm
  cross join month_spine as ms
),
latest_month as (
  select max(month_start_date) as latest_month_start_date
  from month_spine
),
aggregated as (
  select
    g.bloc_code,
    max(g.bloc_name) as bloc_name,
    max(g.bloc_type) as bloc_type,
    g.month_start_date,
    max(g.year_month) as year_month,
    count(distinct g.iso3) as expected_member_country_count,
    count(distinct case when coalesce(rc.reported_trade_flag, false) then g.iso3 end) as reported_member_country_count,
    count(distinct case when not coalesce(rc.reported_trade_flag, false) then g.iso3 end) as missing_member_country_count,
    sum(case when coalesce(rc.reported_trade_flag, false) then coalesce(rc.total_trade_value_usd, 0) else 0 end) as reported_trade_value_usd,
    sum(coalesce(rc.total_trade_value_usd, 0)) as member_trade_value_usd,
    string_agg(
      case when not coalesce(rc.reported_trade_flag, false) then g.country_name else null end,
      ', '
      order by g.country_name
    ) as missing_reporters_list
  from member_month_grid as g
  left join {{ ref('mart_trade_reporter_month_coverage') }} as rc
    on g.iso3 = rc.reporter_iso3
   and g.month_start_date = rc.month_start_date
  group by 1, 4
)

select
  a.bloc_code,
  a.bloc_name,
  a.bloc_type,
  a.month_start_date,
  a.year_month,
  a.expected_member_country_count,
  a.reported_member_country_count,
  a.missing_member_country_count,
  {{ safe_divide('a.reported_member_country_count', 'a.expected_member_country_count') }} as bloc_reporting_coverage_pct,
  a.reported_trade_value_usd,
  a.member_trade_value_usd,
  case
    when {{ safe_divide('a.reported_member_country_count', 'a.expected_member_country_count') }} >= 0.90 then 'Good'
    when {{ safe_divide('a.reported_member_country_count', 'a.expected_member_country_count') }} >= 0.70 then 'Partial'
    when {{ safe_divide('a.reported_member_country_count', 'a.expected_member_country_count') }} >= 0.40 then 'Weak'
    else 'Poor'
  end as bloc_coverage_status,
  case
    when {{ safe_divide('a.reported_member_country_count', 'a.expected_member_country_count') }} >= 0.90 then null
    when {{ safe_divide('a.reported_member_country_count', 'a.expected_member_country_count') }} >= 0.70
      then 'One or more member reporters are missing for this bloc-month; totals may understate true bloc trade.'
    when {{ safe_divide('a.reported_member_country_count', 'a.expected_member_country_count') }} >= 0.40
      then 'Bloc coverage is weak; treat cross-bloc trade comparisons cautiously because multiple members are missing.'
    else 'Bloc coverage is poor; most member reporters are missing and bloc trade totals are low reliability.'
  end as bloc_dashboard_warning,
  a.missing_reporters_list,
  case
    when a.month_start_date = lm.latest_month_start_date then true
    else false
  end as latest_month_flag
from aggregated as a
cross join latest_month as lm
