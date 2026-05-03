-- Dashboard-ready dataset coverage trend mart.
-- Grain: one row per dataset_name + month_start_date.

with trade_expected as (
  select max(expected_reporter_count) as expected_records
  from {{ ref('mart_trade_month_coverage_status') }}
),
trade_bounds as (
  select
    min(month_start_date) as min_month_start_date,
    max(month_start_date) as max_month_start_date
  from {{ ref('mart_trade_month_coverage_status') }}
),
trade_calendar as (
  select month_start_date
  from trade_bounds,
  {{ month_series('trade_bounds.min_month_start_date', 'trade_bounds.max_month_start_date') }}
),
trade_observed as (
  select
    month_start_date,
    max(year_month) as year_month,
    max(expected_reporter_count) as expected_records,
    max(reporters_with_data_in_month) as observed_records
  from {{ ref('mart_trade_month_coverage_status') }}
  group by 1
),
comtrade_monthly as (
  select
    'comtrade' as dataset_name,
    'trade' as dataset_family,
    c.month_start_date,
    {{ year_month_from_date('c.month_start_date') }} as year_month,
    coalesce(o.expected_records, te.expected_records) as expected_records,
    coalesce(o.observed_records, 0) as observed_records
  from trade_calendar as c
  cross join trade_expected as te
  left join trade_observed as o
    on c.month_start_date = o.month_start_date
),
portwatch_expected as (
  select count(distinct chokepoint_id) as expected_chokepoint_count
  from {{ ref('dim_chokepoint') }}
),
portwatch_daily_bounds as (
  select
    min(month_start_date) as min_month_start_date,
    max(month_start_date) as max_month_start_date,
    max(date_day) as latest_data_date
  from {{ ref('stg_portwatch_daily') }}
),
portwatch_daily_calendar as (
  select month_start_date
  from portwatch_daily_bounds,
  {{ month_series('portwatch_daily_bounds.min_month_start_date', 'portwatch_daily_bounds.max_month_start_date') }}
),
portwatch_daily_observed as (
  select
    month_start_date,
    countif(has_portwatch_daily_data_flag = 1) as observed_records
  from {{ ref('stg_portwatch_daily') }}
  group by 1
),
portwatch_daily_monthly as (
  select
    'portwatch_daily' as dataset_name,
    'maritime' as dataset_family,
    c.month_start_date,
    {{ year_month_from_date('c.month_start_date') }} as year_month,
    pe.expected_chokepoint_count * (
      case
        when c.month_start_date = date_trunc(pdb.latest_data_date, month)
          then cast(extract(day from pdb.latest_data_date) as {{ dbt.type_int() }})
        else date_diff(date_add(c.month_start_date, interval 1 month), c.month_start_date, day)
      end
    ) as expected_records,
    coalesce(o.observed_records, 0) as observed_records
  from portwatch_daily_calendar as c
  cross join portwatch_expected as pe
  cross join portwatch_daily_bounds as pdb
  left join portwatch_daily_observed as o
    on c.month_start_date = o.month_start_date
),
portwatch_monthly_bounds as (
  select
    min(month_start_date) as min_month_start_date,
    max(month_start_date) as max_month_start_date
  from {{ ref('stg_portwatch_stress_metrics') }}
),
portwatch_monthly_calendar as (
  select month_start_date
  from portwatch_monthly_bounds,
  {{ month_series('portwatch_monthly_bounds.min_month_start_date', 'portwatch_monthly_bounds.max_month_start_date') }}
),
portwatch_monthly_observed as (
  select
    month_start_date,
    count(distinct chokepoint_id) as observed_records
  from {{ ref('stg_portwatch_stress_metrics') }}
  group by 1
),
portwatch_monthly_rows as (
  select
    'portwatch_monthly' as dataset_name,
    'maritime' as dataset_family,
    c.month_start_date,
    {{ year_month_from_date('c.month_start_date') }} as year_month,
    pe.expected_chokepoint_count as expected_records,
    coalesce(o.observed_records, 0) as observed_records
  from portwatch_monthly_calendar as c
  cross join portwatch_expected as pe
  left join portwatch_monthly_observed as o
    on c.month_start_date = o.month_start_date
),
brent_expected as (
  select count(distinct benchmark_code) as expected_benchmark_count
  from {{ ref('stg_brent_monthly') }}
  where benchmark_code in ('BRENT_EU', 'WTI_US')
),
brent_daily_base as (
  select
    {{ month_start_from_year_month('year_month') }} as month_start_date,
    benchmark_code
  from {{ ref('stg_brent_daily') }}
  where benchmark_code in ('BRENT_EU', 'WTI_US')
),
brent_daily_bounds as (
  select
    min(month_start_date) as min_month_start_date,
    max(month_start_date) as max_month_start_date
  from brent_daily_base
),
brent_daily_calendar as (
  select month_start_date
  from brent_daily_bounds,
  {{ month_series('brent_daily_bounds.min_month_start_date', 'brent_daily_bounds.max_month_start_date') }}
),
brent_daily_observed as (
  select
    month_start_date,
    count(distinct benchmark_code) as observed_records
  from brent_daily_base
  group by 1
),
brent_daily_monthly as (
  select
    'brent_daily' as dataset_name,
    'macro_market' as dataset_family,
    c.month_start_date,
    {{ year_month_from_date('c.month_start_date') }} as year_month,
    be.expected_benchmark_count as expected_records,
    coalesce(o.observed_records, 0) as observed_records
  from brent_daily_calendar as c
  cross join brent_expected as be
  left join brent_daily_observed as o
    on c.month_start_date = o.month_start_date
),
brent_monthly_bounds as (
  select
    min(month_start_date) as min_month_start_date,
    max(month_start_date) as max_month_start_date
  from {{ ref('stg_brent_monthly') }}
  where benchmark_code in ('BRENT_EU', 'WTI_US')
),
brent_monthly_calendar as (
  select month_start_date
  from brent_monthly_bounds,
  {{ month_series('brent_monthly_bounds.min_month_start_date', 'brent_monthly_bounds.max_month_start_date') }}
),
brent_monthly_observed as (
  select
    month_start_date,
    count(distinct benchmark_code) as observed_records
  from {{ ref('stg_brent_monthly') }}
  where benchmark_code in ('BRENT_EU', 'WTI_US')
  group by 1
),
brent_monthly_rows as (
  select
    'brent_monthly' as dataset_name,
    'macro_market' as dataset_family,
    c.month_start_date,
    {{ year_month_from_date('c.month_start_date') }} as year_month,
    be.expected_benchmark_count as expected_records,
    coalesce(o.observed_records, 0) as observed_records
  from brent_monthly_calendar as c
  cross join brent_expected as be
  left join brent_monthly_observed as o
    on c.month_start_date = o.month_start_date
),
fx_pair_expected as (
  select
    max(pair_count) as expected_records
  from (
    select
      year_month,
      count(
        distinct concat(currency_view, '|', base_currency_code, '|', fx_currency_code)
      ) as pair_count
    from {{ ref('stg_fx_monthly') }}
    group by 1
  )
),
fx_bounds as (
  select
    min(month_start_date) as min_month_start_date,
    max(month_start_date) as max_month_start_date
  from {{ ref('stg_fx_monthly') }}
),
fx_calendar as (
  select month_start_date
  from fx_bounds,
  {{ month_series('fx_bounds.min_month_start_date', 'fx_bounds.max_month_start_date') }}
),
fx_observed as (
  select
    month_start_date,
    count(
      distinct concat(currency_view, '|', base_currency_code, '|', fx_currency_code)
    ) as observed_records
  from {{ ref('stg_fx_monthly') }}
  group by 1
),
fx_monthly_rows as (
  select
    'fx_monthly' as dataset_name,
    'macro_market' as dataset_family,
    c.month_start_date,
    {{ year_month_from_date('c.month_start_date') }} as year_month,
    fpe.expected_records,
    coalesce(o.observed_records, 0) as observed_records
  from fx_calendar as c
  cross join fx_pair_expected as fpe
  left join fx_observed as o
    on c.month_start_date = o.month_start_date
),
worldbank_yearly_observed as (
  select
    {{ month_start_from_year_month('year_month') }} as month_start_date,
    count(*) as observed_records
  from {{ ref('stg_energy_vulnerability') }}
  group by 1
),
worldbank_expected as (
  select max(observed_records) as expected_records
  from worldbank_yearly_observed
),
worldbank_monthly_rows as (
  select
    'worldbank_energy' as dataset_name,
    'structural_energy' as dataset_family,
    wo.month_start_date,
    {{ year_month_from_date('wo.month_start_date') }} as year_month,
    we.expected_records as expected_records,
    wo.observed_records
  from worldbank_yearly_observed as wo
  cross join worldbank_expected as we
),
event_months as (
  select
    date_trunc(event_start_date, month) as month_start_date,
    count(distinct event_id) as raw_event_count
  from {{ ref('dim_event') }}
  where event_start_date is not null
  group by 1
),
events_monthly as (
  select
    'events' as dataset_name,
    'curated_events' as dataset_family,
    em.month_start_date,
    {{ year_month_from_date('em.month_start_date') }} as year_month,
    1 as expected_records,
    case when em.raw_event_count > 0 then 1 else 0 end as observed_records
  from event_months as em
),
unioned as (
  select * from comtrade_monthly
  union all
  select * from portwatch_daily_monthly
  union all
  select * from portwatch_monthly_rows
  union all
  select * from brent_daily_monthly
  union all
  select * from brent_monthly_rows
  union all
  select * from fx_monthly_rows
  union all
  select * from worldbank_monthly_rows
  union all
  select * from events_monthly
)

select
  dataset_name,
  dataset_family,
  month_start_date,
  year_month,
  expected_records,
  observed_records,
  greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) as coverage_ratio,
  case
    when greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) >= 0.90 then 'Good'
    when greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) >= 0.70 then 'Partial'
    when greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) >= 0.40 then 'Weak'
    else 'Poor'
  end as coverage_status,
  case
    when greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) < 0.90 then true
    else false
  end as warning_flag,
  case
    when dataset_name = 'comtrade'
      and greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) < 0.90
      then 'Comtrade reporter coverage is incomplete for this month; recent official reporting delays can look like real economic movement.'
    when dataset_name in ('portwatch_daily', 'portwatch_monthly')
      and greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) < 0.90
      then 'PortWatch preserves null days and sparse chokepoint coverage instead of filling gaps.'
    when dataset_name in ('brent_daily', 'brent_monthly')
      and greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) < 0.90
      then 'Oil benchmark coverage is partial for this month; benchmark scope may be incomplete.'
    when dataset_name = 'fx_monthly'
      and greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) < 0.90
      then 'FX monthly coverage is partial; one or more currency views or pairs are missing.'
    when dataset_name = 'worldbank_energy'
      and greatest(least(coalesce({{ safe_divide('observed_records', 'expected_records') }}, 0), 1), 0) < 0.90
      then 'World Bank energy is annual and broadcast to month grain; missing rows reflect annual source scope rather than true monthly movement.'
    else null
  end as dashboard_warning
from unioned
