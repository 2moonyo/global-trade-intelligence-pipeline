select
  cast(period as integer) as period,
  cast(year as integer) as year,
  cast(month as integer) as month,
  cast(quarter as integer) as quarter,
  year_month,
  cast(date as date) as month_start_date
from {{ source('raw', 'dim_time') }}
