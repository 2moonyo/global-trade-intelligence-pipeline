select
  bloc_code,
  month_start_date,
  count(*) as row_count
from {{ ref('mart_trade_bloc_month_coverage') }}
group by 1, 2
having count(*) > 1
