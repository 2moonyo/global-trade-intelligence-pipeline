
    
    

with all_values as (

    select
        stress_hotspot_band as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
    group by stress_hotspot_band

)

select *
from all_values
where value_field not in (
    'INSUFFICIENT_BASELINE','SEVERE','ELEVATED','NORMAL'
)


