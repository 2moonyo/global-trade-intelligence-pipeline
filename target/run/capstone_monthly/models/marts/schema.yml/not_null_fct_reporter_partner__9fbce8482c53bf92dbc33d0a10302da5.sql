
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select partner_iso3
from `capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month_provenance`
where partner_iso3 is null



  
  
      
    ) dbt_internal_test