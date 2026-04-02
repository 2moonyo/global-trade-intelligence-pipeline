select
  year_month,
  
    safe_cast(month_start_date as date)
   as month_start_date,
  
    safe_cast(year as INT64)
   as year,
  
    safe_cast(month as INT64)
   as month,
  benchmark_code,
  benchmark_name,
  region,
  source_series_id,
  
    safe_cast(avg_price_usd_per_bbl as FLOAT64)
   as avg_price_usd_per_bbl,
  
    safe_cast(min_price_usd_per_bbl as FLOAT64)
   as min_price_usd_per_bbl,
  
    safe_cast(max_price_usd_per_bbl as FLOAT64)
   as max_price_usd_per_bbl,
  
    safe_cast(month_start_price_usd_per_bbl as FLOAT64)
   as month_start_price_usd_per_bbl,
  
    safe_cast(month_end_price_usd_per_bbl as FLOAT64)
   as month_end_price_usd_per_bbl,
  
    safe_cast(mom_abs_change_usd as FLOAT64)
   as mom_abs_change_usd,
  
    safe_cast(mom_pct_change as FLOAT64)
   as mom_pct_change,
  
    safe_cast(trading_day_count as INT64)
   as trading_day_count
from `capfractal`.`raw`.`brent_monthly`