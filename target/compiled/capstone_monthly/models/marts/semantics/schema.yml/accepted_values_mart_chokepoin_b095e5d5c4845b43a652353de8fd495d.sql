
    
    

with all_values as (

    select
        alert_band as value_field,
        count(*) as n_records

    from `fullcap-10111`.`analytics_marts`.`mart_chokepoint_daily_signal`
    group by alert_band

)

select *
from all_values
where value_field not in (
    'NO_DATA','INSUFFICIENT_BASELINE','SEVERE','ELEVATED','NORMAL'
)


