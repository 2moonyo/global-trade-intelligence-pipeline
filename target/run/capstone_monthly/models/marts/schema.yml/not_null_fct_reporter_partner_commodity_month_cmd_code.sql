
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cmd_code
from `capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month`
where cmd_code is null



  
  
      
    ) dbt_internal_test