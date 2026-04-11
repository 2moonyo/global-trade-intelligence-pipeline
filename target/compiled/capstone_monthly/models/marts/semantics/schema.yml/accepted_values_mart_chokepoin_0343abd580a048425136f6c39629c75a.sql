
    
    

with all_values as (

    select
        stress_severity_band as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_hotspot_map`
    group by stress_severity_band

)

select *
from all_values
where value_field not in (
    'INSUFFICIENT_BASELINE','EXTREME','SEVERE','ELEVATED','NORMAL'
)


