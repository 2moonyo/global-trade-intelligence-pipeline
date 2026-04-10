
    
    

with all_values as (

    select
        severity_level as value_field,
        count(*) as n_records

    from "analytics"."analytics_marts"."mart_trade_route_geography"
    group by severity_level

)

select *
from all_values
where value_field not in (
    'low','medium','high','critical'
)


