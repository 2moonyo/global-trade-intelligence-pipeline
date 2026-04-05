
  
    

    create or replace table `capfractal`.`analytics_marts`.`dim_time`
      
    
    

    
    OPTIONS()
    as (
      with base as (
  select
    period as month_key,
    period,
    year,
    month,
    quarter,
    year_month,
    month_start_date
  from `capfractal`.`analytics_staging`.`stg_dim_time`
)

select
  month_key,
  period,
  year,
  month,
  quarter,
  year_month,
  month_start_date
from base
    );
  