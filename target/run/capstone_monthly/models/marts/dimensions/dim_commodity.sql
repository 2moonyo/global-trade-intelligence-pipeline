
  
    

    create or replace table `capfractal`.`analytics_marts`.`dim_commodity`
      
    
    

    
    OPTIONS()
    as (
      with base as (
  select
    cmd_code,
    hs2,
    hs4,
    hs6,
    commodity_name,
    commodity_group,
    food_flag,
    energy_flag,
    industrial_flag
  from `capfractal`.`analytics_staging`.`stg_dim_commodity`
)

select
  cmd_code,
  hs2,
  hs4,
  hs6,
  commodity_name,
  commodity_group,
  food_flag,
  energy_flag,
  industrial_flag
from base
    );
  