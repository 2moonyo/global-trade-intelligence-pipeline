select *
from {{ ref('mart_trade_reporter_month_coverage') }}
where reporting_coverage_score is not null
  and (reporting_coverage_score < 0 or reporting_coverage_score > 1)
