
    
    

with all_values as (

    select
        event_phase_label as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_country_event_impact`
    group by event_phase_label

)

select *
from all_values
where value_field not in (
    'before','during','after','outside_window'
)


