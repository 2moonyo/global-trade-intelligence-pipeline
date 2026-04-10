
    
    

with all_values as (

    select
        latest_day_flag as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
    group by latest_day_flag

)

select *
from all_values
where value_field not in (
    True,False
)


