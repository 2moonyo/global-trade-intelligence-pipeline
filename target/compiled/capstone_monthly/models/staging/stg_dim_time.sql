


with observed_months as (

  select distinct
    
    safe_cast(concat(cast(year_month as string), '-01') as date)
   as month_start_date
  from `capfractal`.`raw`.`comtrade_fact`
  where year_month is not null
    and 
    regexp_contains(cast(year_month as string), r'^\d{4}-\d{2}$')
  

  union distinct

  select distinct
    
    safe_cast(concat(cast(year_month as string), '-01') as date)
   as month_start_date
  from `capfractal`.`raw`.`portwatch_monthly`
  where year_month is not null
    and 
    regexp_contains(cast(year_month as string), r'^\d{4}-\d{2}$')
  

  union distinct

  select distinct
    cast(month_start_date as date) as month_start_date
  from `capfractal`.`raw`.`brent_monthly`
  where month_start_date is not null

  union distinct

  select distinct
    
    safe_cast(concat(cast(year_month as string), '-01') as date)
   as month_start_date
  from `capfractal`.`raw`.`ecb_fx_eu_monthly`
  where year_month is not null
    and 
    regexp_contains(cast(year_month as string), r'^\d{4}-\d{2}$')
  

  union distinct

  select distinct
    
    safe_cast(concat(cast(year_month as string), '-01') as date)
   as month_start_date
  from `capfractal`.`raw`.`bridge_event_month_chokepoint_core`
  where year_month is not null
    and 
    regexp_contains(cast(year_month as string), r'^\d{4}-\d{2}$')
  

  union distinct

  select distinct
    
    safe_cast(concat(cast(year_month as string), '-01') as date)
   as month_start_date
  from `capfractal`.`raw`.`bridge_event_month_maritime_region`
  where year_month is not null
    and 
    regexp_contains(cast(year_month as string), r'^\d{4}-\d{2}$')
  

  union distinct

  select distinct
    
    safe_cast(concat(cast(year as string), '-01-01') as date)
   as month_start_date
  from `capfractal`.`raw`.`energy_vulnerability`
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
  
    unnest(generate_date_array(cast(
    date_add(cast(min_month_start as date), interval -12 month)
   as date), cast(
    date_add(cast(max_month_start as date), interval 12 month)
   as date), interval 1 month)) as month_start_date
  

)

select
  
    cast(format_date('%Y%m', cast(month_start_date as date)) as INT64)
   as period,
  cast(extract(year from cast(month_start_date as date)) as INT64) as year,
  cast(extract(month from cast(month_start_date as date)) as INT64) as month,
  cast(extract(quarter from cast(month_start_date as date)) as INT64) as quarter,
  
    format_date('%Y-%m', cast(month_start_date as date))
   as year_month,
  month_start_date
from calendar