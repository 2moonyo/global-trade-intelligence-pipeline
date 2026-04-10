-- Monthly Looker Studio support mart for Page 1 trade coverage framing.
-- Grain: one row per month_start_date.

with overview as (
  select
    reporter_country_code,
    year_month_key,
    year_month,
    month_start_date,
    month_label,
    has_reported_trade_data_flag,
    reporters_with_data_in_month,
    expected_reporter_count,
    reporting_completeness_pct,
    complete_month_flag,
    latest_complete_month_flag,
    latest_month_flag
  from {{ ref('mart_dashboard_global_trade_overview') }}
),
month_rollup as (
  -- The source mart already repeats month-level coverage fields on every reporter row.
  -- This rollup collapses those repeated fields to a month-grain semantic source.
  select
    month_start_date,
    max(year_month_key) as year_month_key,
    max(year_month) as year_month,
    max(month_label) as month_label,
    max(reporters_with_data_in_month) as reporters_with_data_in_month,
    max(expected_reporter_count) as expected_reporter_count,
    countif(not has_reported_trade_data_flag) as missing_reporter_count,
    max(reporting_completeness_pct) as reporting_completeness_pct,
    max(complete_month_flag) as complete_month_flag,
    max(latest_complete_month_flag) as latest_complete_month_flag,
    max(latest_month_flag) as latest_month_flag
  from overview
  group by 1
),
latest_month as (
  select max(month_start_date) as latest_month_start_date
  from month_rollup
),
latest_complete_month as (
  select max(case when complete_month_flag then month_start_date end) as latest_complete_month_start_date
  from month_rollup
)

select
  mr.month_start_date,
  mr.year_month_key,
  mr.year_month,
  mr.month_label,
  mr.reporters_with_data_in_month,
  mr.expected_reporter_count,
  mr.missing_reporter_count,
  {{ safe_divide('mr.missing_reporter_count', 'mr.expected_reporter_count') }} as missing_reporter_pct,
  mr.reporting_completeness_pct,
  case
    when mr.missing_reporter_count > 0 then true
    else false
  end as coverage_gap_flag,
  case
    when mr.reporters_with_data_in_month = 0 then 'NO_TRADE_DATA'
    when mr.complete_month_flag then 'FULL_COVERAGE'
    else 'PARTIAL_COVERAGE'
  end as trade_reporting_status,
  mr.complete_month_flag,
  mr.latest_complete_month_flag,
  mr.latest_month_flag,
  lcm.latest_complete_month_start_date,
  {{ year_month_from_date('lcm.latest_complete_month_start_date') }} as latest_complete_year_month,
  case
    when lm.latest_month_start_date is null
      or lcm.latest_complete_month_start_date is null then null
    else cast(
      (
        extract(year from lm.latest_month_start_date) - extract(year from lcm.latest_complete_month_start_date)
      ) * 12
      + (
        extract(month from lm.latest_month_start_date) - extract(month from lcm.latest_complete_month_start_date)
      ) as {{ dbt.type_int() }}
    )
  end as months_between_latest_and_latest_complete
from month_rollup as mr
cross join latest_month as lm
cross join latest_complete_month as lcm
