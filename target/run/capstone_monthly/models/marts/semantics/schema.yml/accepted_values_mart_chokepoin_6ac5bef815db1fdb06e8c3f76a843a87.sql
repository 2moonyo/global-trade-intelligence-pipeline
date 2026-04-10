
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        stress_hotspot_band as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
    group by stress_hotspot_band

)

select *
from all_values
where value_field not in (
    'INSUFFICIENT_BASELINE','SEVERE','ELEVATED','NORMAL'
)



  
  
      
    ) dbt_internal_test