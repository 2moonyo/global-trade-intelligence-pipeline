
    
    

with all_values as (

    select
        stress_direction as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
    group by stress_direction

)

select *
from all_values
where value_field not in (
    'NO_PRIOR_MONTH','INSUFFICIENT_BASELINE','MORE_STRESSED','LESS_STRESSED','UNCHANGED'
)


