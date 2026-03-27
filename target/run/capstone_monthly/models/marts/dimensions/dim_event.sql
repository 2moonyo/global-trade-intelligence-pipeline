
  
    
    

    create  table
      "analytics"."analytics_analytics_marts"."dim_event__dbt_tmp"
  
    as (
      

with base as (

    select *
    from "analytics"."analytics_analytics_staging"."stg_event_raw"

),

chokepoint_coverage as (

    select
        event_id,
        count(distinct location_name) as chokepoint_count,
        max(case when is_global_event then 1 else 0 end) as has_global_flag
    from "analytics"."analytics_analytics_staging"."stg_event_location"
    where location_type = 'chokepoint'
    group by 1

),

noncore_location_coverage as (

    select
        event_id,
        count(distinct location_name) as noncore_location_count
    from "analytics"."analytics_analytics_staging"."stg_event_location"
    where location_type <> 'chokepoint'
    group by 1

),

final as (

    select
        b.event_id,
        b.event_source,
        b.event_name,
        b.event_type,
        b.event_start_date,
        b.event_end_date,

        case
            when b.base_severity_score >= 0.90 then 'critical'
            when b.base_severity_score >= 0.80 then 'high'
            when b.base_severity_score >= 0.70 then 'medium'
            else 'low'
        end as severity_level,

        case
            when coalesce(c.has_global_flag, 0) = 1 then 'global'
            when coalesce(c.chokepoint_count, 0) > 0 and coalesce(r.noncore_location_count, 0) > 0 then 'mixed'
            when coalesce(c.chokepoint_count, 0) > 1 then 'multi_chokepoint'
            when coalesce(c.chokepoint_count, 0) = 1 then 'chokepoint_specific'
            when coalesce(r.noncore_location_count, 0) > 0 then 'regional'
            else 'unscoped'
        end as event_scope_type,

        b.description,

        b.base_severity_score,
        b.lead_months,
        b.lag_months,
        b.raw_event_scope,
        b.source_class

    from base b
    left join chokepoint_coverage c
        on b.event_id = c.event_id
    left join noncore_location_coverage r
        on b.event_id = r.event_id

)

select *
from final
    );
  
  