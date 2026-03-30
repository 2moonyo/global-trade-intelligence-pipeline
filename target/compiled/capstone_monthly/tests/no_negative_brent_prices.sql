select *
from `capfractal`.`analytics_staging`.`stg_brent_monthly`
where benchmark_code = 'BRENT_EU'
  and avg_price_usd_per_bbl < 0