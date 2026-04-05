
  
    

    create or replace table `capfractal`.`analytics_marts`.`dim_trade_flow`
      
    
    

    
    OPTIONS()
    as (
      with base as (
  select
    flow_code,
    flow_desc,
    flow_group
  from `capfractal`.`analytics_staging`.`stg_dim_trade_flow`
)

select
  flow_code,
  flow_desc,
  flow_group
from base
    );
  