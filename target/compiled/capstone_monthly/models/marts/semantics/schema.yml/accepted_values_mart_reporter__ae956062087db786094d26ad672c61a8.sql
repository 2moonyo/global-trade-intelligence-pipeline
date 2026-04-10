
    
    

with all_values as (

    select
        latest_month_flag as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
    group by latest_month_flag

)

select *
from all_values
where value_field not in (
    True,False
)


