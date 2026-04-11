
  
    

    create or replace table `chokepoint-capfractal`.`analytics_marts`.`dim_trade_flow`
      
    
    

    
    OPTIONS()
    as (
      with base as (
  select
    flow_code,
    flow_desc,
    flow_group
  from `chokepoint-capfractal`.`analytics_staging`.`stg_dim_trade_flow`
)

select
  flow_code,
  flow_desc,
  flow_group
from base
    );
  