
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_scope_type as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_analytics_marts`.`dim_event`
    group by event_scope_type

)

select *
from all_values
where value_field not in (
    'global','mixed','multi_chokepoint','chokepoint_specific','regional','unscoped'
)



  
  
      
    ) dbt_internal_test