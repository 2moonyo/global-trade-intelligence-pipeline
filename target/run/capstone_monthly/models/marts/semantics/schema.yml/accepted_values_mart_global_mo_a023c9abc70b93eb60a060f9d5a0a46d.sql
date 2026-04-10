
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        system_stress_level as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
    group by system_stress_level

)

select *
from all_values
where value_field not in (
    'NO_PORTWATCH_DATA','INSUFFICIENT_BASELINE','SEVERE','ELEVATED','NORMAL'
)



  
  
      
    ) dbt_internal_test