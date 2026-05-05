select *
from {{ ref('stg_brent_monthly') }}
where benchmark_code = 'BRENT_EU'
  and avg_price_usd_per_bbl < 0