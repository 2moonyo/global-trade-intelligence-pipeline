
    
    

with all_values as (

    select
        risk_level as value_field,
        count(*) as n_records

    from "analytics"."analytics_marts"."mart_trade_route_geography"
    group by risk_level

)

select *
from all_values
where value_field not in (
    'high','medium','low'
)


