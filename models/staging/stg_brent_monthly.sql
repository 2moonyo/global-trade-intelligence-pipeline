select
  year_month,
  try_cast(month_start_date as date) as month_start_date,
  try_cast(year as integer) as year_num,
  try_cast(month as integer) as month_num,
  benchmark_code,
  benchmark_name,
  region,
  source_series_id,
  try_cast(avg_price_usd_per_bbl as double) as avg_price_usd_per_bbl,
  try_cast(min_price_usd_per_bbl as double) as min_price_usd_per_bbl,
  try_cast(max_price_usd_per_bbl as double) as max_price_usd_per_bbl,
  try_cast(month_start_price_usd_per_bbl as double) as month_start_price_usd_per_bbl,
  try_cast(month_end_price_usd_per_bbl as double) as month_end_price_usd_per_bbl,
  try_cast(mom_abs_change_usd as double) as mom_abs_change_usd,
  try_cast(mom_pct_change as double) as mom_pct_change,
  try_cast(trading_day_count as integer) as trading_day_count
from {{ source('raw', 'brent_monthly') }}