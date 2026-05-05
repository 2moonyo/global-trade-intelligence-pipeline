select
  bloc_code,
  month_start_date,
  count(*) as row_count
from {{ ref('mart_bloc_month_trade_macro_summary') }}
group by 1, 2
having count(*) > 1
