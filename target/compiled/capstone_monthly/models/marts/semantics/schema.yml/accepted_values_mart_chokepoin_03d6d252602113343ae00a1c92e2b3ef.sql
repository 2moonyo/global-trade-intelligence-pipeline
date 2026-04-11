
    
    

with all_values as (

    select
        top_5_stressed_chokepoint_flag as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
    group by top_5_stressed_chokepoint_flag

)

select *
from all_values
where value_field not in (
    True,False
)


