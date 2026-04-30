select
  currency_view,
  base_currency_code,
  fx_currency_code,
  bloc_code,
  month_start_date,
  count(*) as row_count
from {{ ref('mart_brent_fx_trade_correlation_monthly') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
