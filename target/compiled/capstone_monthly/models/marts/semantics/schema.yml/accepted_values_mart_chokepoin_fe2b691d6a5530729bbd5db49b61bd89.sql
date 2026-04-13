
    
    

with all_values as (

    select
        has_portwatch_daily_data_flag as value_field,
        count(*) as n_records

    from `fullcap-10111`.`analytics_marts`.`mart_chokepoint_daily_signal`
    group by has_portwatch_daily_data_flag

)

select *
from all_values
where value_field not in (
    0,1
)


