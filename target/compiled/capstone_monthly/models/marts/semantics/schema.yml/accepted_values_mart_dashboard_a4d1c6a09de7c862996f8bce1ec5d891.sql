
    
    

with all_values as (

    select
        latest_complete_month_flag as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
    group by latest_complete_month_flag

)

select *
from all_values
where value_field not in (
    True,False
)


