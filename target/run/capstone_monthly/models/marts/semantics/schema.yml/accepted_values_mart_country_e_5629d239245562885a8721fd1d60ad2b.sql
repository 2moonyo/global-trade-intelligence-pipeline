
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        dependency_level as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_country_event_impact`
    group by dependency_level

)

select *
from all_values
where value_field not in (
    'very_high','high','moderate','low','very_low'
)



  
  
      
    ) dbt_internal_test