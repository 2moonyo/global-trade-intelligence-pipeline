
    
    

with all_values as (

    select
        missing_from_latest_month_flag as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
    group by missing_from_latest_month_flag

)

select *
from all_values
where value_field not in (
    True,False
)


