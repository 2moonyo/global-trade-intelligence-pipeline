select
  year_month,
  {{ safe_cast('month_start_date', 'date') }} as month_start_date,
  {{ safe_cast('year', dbt.type_int()) }} as year_num,
  {{ safe_cast('month', dbt.type_int()) }} as month_num,
  benchmark_code,
  benchmark_name,
  region,
  source_series_id,
  {{ safe_cast('avg_price_usd_per_bbl', dbt.type_float()) }} as avg_price_usd_per_bbl,
  {{ safe_cast('min_price_usd_per_bbl', dbt.type_float()) }} as min_price_usd_per_bbl,
  {{ safe_cast('max_price_usd_per_bbl', dbt.type_float()) }} as max_price_usd_per_bbl,
  {{ safe_cast('month_start_price_usd_per_bbl', dbt.type_float()) }} as month_start_price_usd_per_bbl,
  {{ safe_cast('month_end_price_usd_per_bbl', dbt.type_float()) }} as month_end_price_usd_per_bbl,
  {{ safe_cast('mom_abs_change_usd', dbt.type_float()) }} as mom_abs_change_usd,
  {{ safe_cast('mom_pct_change', dbt.type_float()) }} as mom_pct_change,
  {{ safe_cast('trading_day_count', dbt.type_int()) }} as trading_day_count
from {{ source('raw', 'brent_monthly') }}
