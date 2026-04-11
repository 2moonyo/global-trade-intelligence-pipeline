
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_flow
from `chokepoint-capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month`
where trade_flow is null



  
  
      
    ) dbt_internal_test