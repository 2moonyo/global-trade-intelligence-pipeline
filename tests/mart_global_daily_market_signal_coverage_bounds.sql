-- Fails when daily coverage falls outside the valid 0 to 1 range.
select *
from {{ ref('mart_global_daily_market_signal') }}
where daily_coverage_ratio < 0
   or daily_coverage_ratio > 1
