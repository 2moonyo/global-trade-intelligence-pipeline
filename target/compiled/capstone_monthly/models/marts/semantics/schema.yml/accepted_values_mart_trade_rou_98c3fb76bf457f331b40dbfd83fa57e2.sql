
    
    

with all_values as (

    select
        event_phase_label as value_field,
        count(*) as n_records

    from "analytics"."analytics_marts"."mart_trade_route_geography"
    group by event_phase_label

)

select *
from all_values
where value_field not in (
    'before','during','after','outside_window'
)


