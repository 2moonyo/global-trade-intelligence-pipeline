{% set dim_time_lead_months = var('dim_time_lead_months', 12) %}
{% set dim_time_lag_months = var('dim_time_lag_months', 12) %}

with observed_months as (

  select distinct
    {{ month_start_from_year_month('year_month') }} as month_start_date
  from {{ source('raw', 'comtrade_fact') }}
  where year_month is not null
    and {{ regex_full_match('year_month', '^\\d{4}-\\d{2}$') }}

  union distinct

  select distinct
    {{ month_start_from_year_month('year_month') }} as month_start_date
  from {{ source('raw', 'portwatch_monthly') }}
  where year_month is not null
    and {{ regex_full_match('year_month', '^\\d{4}-\\d{2}$') }}

  union distinct

  select distinct
    cast(month_start_date as date) as month_start_date
  from {{ source('raw', 'brent_monthly') }}
  where month_start_date is not null

  union distinct

  select distinct
    {{ month_start_from_year_month('year_month') }} as month_start_date
  from {{ source('raw', 'ecb_fx_eu_monthly') }}
  where year_month is not null
    and {{ regex_full_match('year_month', '^\\d{4}-\\d{2}$') }}

  union distinct

  select distinct
    {{ month_start_from_year_month('year_month') }} as month_start_date
  from {{ source('raw', 'bridge_event_month_chokepoint_core') }}
  where year_month is not null
    and {{ regex_full_match('year_month', '^\\d{4}-\\d{2}$') }}

  union distinct

  select distinct
    {{ month_start_from_year_month('year_month') }} as month_start_date
  from {{ source('raw', 'bridge_event_month_maritime_region') }}
  where year_month is not null
    and {{ regex_full_match('year_month', '^\\d{4}-\\d{2}$') }}

  union distinct

  select distinct
    {{ year_start_date('year') }} as month_start_date
  from {{ source('raw', 'energy_vulnerability') }}
  where year is not null

),

bounds as (

  select
    min(month_start_date) as min_month_start,
    max(month_start_date) as max_month_start
  from observed_months

),

calendar as (

  select
    month_start_date
  from bounds,
  {{ month_series(
    date_add_months('min_month_start', -dim_time_lead_months),
    date_add_months('max_month_start', dim_time_lag_months)
  ) }}

)

select
  {{ period_int_from_date('month_start_date') }} as period,
  {{ year_int_from_date('month_start_date') }} as year,
  {{ month_int_from_date('month_start_date') }} as month,
  {{ quarter_int_from_date('month_start_date') }} as quarter,
  {{ year_month_from_date('month_start_date') }} as year_month,
  month_start_date
from calendar
