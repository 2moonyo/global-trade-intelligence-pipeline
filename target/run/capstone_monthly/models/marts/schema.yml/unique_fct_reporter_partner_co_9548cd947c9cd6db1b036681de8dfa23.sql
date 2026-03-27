
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    canonical_grain_key as unique_field,
    count(*) as n_records

from "analytics"."analytics_marts"."fct_reporter_partner_commodity_month_provenance"
where canonical_grain_key is not null
group by canonical_grain_key
having count(*) > 1



  
  
      
    ) dbt_internal_test