with overview_month as (
  select
    month_start_date,
    max(reporters_with_data_in_month) as reporters_with_data_in_month,
    max(expected_reporter_count) as expected_reporter_count,
    countif(not has_reported_trade_data_flag) as missing_reporter_count,
    max(reporting_completeness_pct) as reporting_completeness_pct,
    max(complete_month_flag) as complete_month_flag,
    max(latest_complete_month_flag) as latest_complete_month_flag,
    max(latest_month_flag) as latest_month_flag
  from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
  group by 1
),
coverage_status as (
  select
    month_start_date,
    reporters_with_data_in_month,
    expected_reporter_count,
    missing_reporter_count,
    reporting_completeness_pct,
    complete_month_flag,
    latest_complete_month_flag,
    latest_month_flag
  from `chokepoint-capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
)

select
  om.month_start_date,
  om.reporters_with_data_in_month as overview_reporters_with_data_in_month,
  cs.reporters_with_data_in_month as coverage_reporters_with_data_in_month,
  om.expected_reporter_count as overview_expected_reporter_count,
  cs.expected_reporter_count as coverage_expected_reporter_count,
  om.missing_reporter_count as overview_missing_reporter_count,
  cs.missing_reporter_count as coverage_missing_reporter_count,
  om.reporting_completeness_pct as overview_reporting_completeness_pct,
  cs.reporting_completeness_pct as coverage_reporting_completeness_pct,
  om.complete_month_flag as overview_complete_month_flag,
  cs.complete_month_flag as coverage_complete_month_flag,
  om.latest_complete_month_flag as overview_latest_complete_month_flag,
  cs.latest_complete_month_flag as coverage_latest_complete_month_flag,
  om.latest_month_flag as overview_latest_month_flag,
  cs.latest_month_flag as coverage_latest_month_flag
from overview_month as om
inner join coverage_status as cs
  on om.month_start_date = cs.month_start_date
where om.reporters_with_data_in_month <> cs.reporters_with_data_in_month
   or om.expected_reporter_count <> cs.expected_reporter_count
   or om.missing_reporter_count <> cs.missing_reporter_count
   or om.reporting_completeness_pct <> cs.reporting_completeness_pct
   or om.complete_month_flag <> cs.complete_month_flag
   or om.latest_complete_month_flag <> cs.latest_complete_month_flag
   or om.latest_month_flag <> cs.latest_month_flag