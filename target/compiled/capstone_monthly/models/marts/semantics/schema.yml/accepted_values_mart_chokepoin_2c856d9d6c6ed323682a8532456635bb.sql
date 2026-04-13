
    
    

with all_values as (

    select
        latest_observed_day_flag as value_field,
        count(*) as n_records

    from `fullcap-10111`.`analytics_marts`.`mart_chokepoint_daily_signal`
    group by latest_observed_day_flag

)

select *
from all_values
where value_field not in (
    True,False
)


