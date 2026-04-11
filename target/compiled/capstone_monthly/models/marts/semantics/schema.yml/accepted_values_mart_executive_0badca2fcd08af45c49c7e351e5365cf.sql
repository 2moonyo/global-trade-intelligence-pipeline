
    
    

with all_values as (

    select
        coverage_gap_flag as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
    group by coverage_gap_flag

)

select *
from all_values
where value_field not in (
    True,False
)


