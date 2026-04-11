
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select canonical_grain_key
from `chokepoint-capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month_provenance`
where canonical_grain_key is null



  
  
      
    ) dbt_internal_test