
    
    

select
    period as unique_field,
    count(*) as n_records

from "analytics"."analytics_staging"."stg_dim_time"
where period is not null
group by period
having count(*) > 1


