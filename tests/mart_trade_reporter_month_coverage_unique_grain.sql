select
  reporter_iso3,
  month_start_date,
  count(*) as row_count
from {{ ref('mart_trade_reporter_month_coverage') }}
group by 1, 2
having count(*) > 1
