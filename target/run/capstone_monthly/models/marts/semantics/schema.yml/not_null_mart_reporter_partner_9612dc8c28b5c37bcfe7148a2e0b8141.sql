
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_id
from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_partner_commodity_month_enriched`
where chokepoint_id is null



  
  
      
    ) dbt_internal_test