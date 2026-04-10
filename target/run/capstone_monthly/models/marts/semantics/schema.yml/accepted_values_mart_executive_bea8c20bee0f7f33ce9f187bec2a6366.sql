
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        monthly_source_coverage_status as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
    group by monthly_source_coverage_status

)

select *
from all_values
where value_field not in (
    'NO_PORTWATCH_DATA','FULL_COVERAGE','PARTIAL_COVERAGE'
)



  
  
      
    ) dbt_internal_test