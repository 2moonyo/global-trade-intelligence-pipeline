
    
    

with all_values as (

    select
        previous_month_available_flag as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
    group by previous_month_available_flag

)

select *
from all_values
where value_field not in (
    True,False
)


