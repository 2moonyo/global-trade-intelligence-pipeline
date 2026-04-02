
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from `capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month_provenance`
where period is null



  
  
      
    ) dbt_internal_test