
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select month_start_date
from `capfractal`.`analytics_marts`.`mart_reporter_partner_commodity_month_enriched`
where month_start_date is null



  
  
      
    ) dbt_internal_test