
    
    

with all_values as (

    select
        system_stress_level as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
    group by system_stress_level

)

select *
from all_values
where value_field not in (
    'NO_PORTWATCH_DATA','INSUFFICIENT_BASELINE','SEVERE','ELEVATED','NORMAL'
)


