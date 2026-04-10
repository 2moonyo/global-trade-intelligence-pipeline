select *
from {{ ref('stg_brent_daily') }}
where benchmark_code = 'BRENT_EU'
  and price_usd_per_bbl < 0
