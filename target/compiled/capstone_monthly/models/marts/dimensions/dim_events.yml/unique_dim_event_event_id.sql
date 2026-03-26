
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "analytics"."analytics_analytics_marts"."dim_event"
where event_id is not null
group by event_id
having count(*) > 1


