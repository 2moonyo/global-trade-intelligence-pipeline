
    
    

with all_values as (

    select
        monthly_source_coverage_status as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
    group by monthly_source_coverage_status

)

select *
from all_values
where value_field not in (
    'NO_PORTWATCH_DATA','FULL_COVERAGE','PARTIAL_COVERAGE'
)


