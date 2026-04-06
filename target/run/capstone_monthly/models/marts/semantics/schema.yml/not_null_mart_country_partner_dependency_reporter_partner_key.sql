
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_partner_key
from `capfractal`.`analytics_marts`.`mart_country_partner_dependency`
where reporter_partner_key is null



  
  
      
    ) dbt_internal_test