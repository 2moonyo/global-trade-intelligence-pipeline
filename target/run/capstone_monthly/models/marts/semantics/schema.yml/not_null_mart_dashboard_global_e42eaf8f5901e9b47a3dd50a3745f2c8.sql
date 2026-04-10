
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_country_name
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where reporter_country_name is null



  
  
      
    ) dbt_internal_test