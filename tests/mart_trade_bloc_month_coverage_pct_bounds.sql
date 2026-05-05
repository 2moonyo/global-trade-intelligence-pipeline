select *
from {{ ref('mart_trade_bloc_month_coverage') }}
where bloc_reporting_coverage_pct < 0
   or bloc_reporting_coverage_pct > 1
