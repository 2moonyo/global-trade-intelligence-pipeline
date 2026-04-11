
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select used_transshipment_hub_flag
from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_partner_commodity_month_enriched`
where used_transshipment_hub_flag is null



  
  
      
    ) dbt_internal_test