

with event_months_union as (

    select
        event_id,
        year_month,
        month_start_date,
        event_phase,
        is_event_active,
        is_lead_period,
        is_lag_period,
        severity_weight,
        is_global_event
    from "analytics"."analytics_analytics_staging"."stg_event_location"

),

deduped as (

    select
        event_id,
        year_month,
        month_start_date,

        max(case when event_phase = 'active' then 1 else 0 end) as has_active_phase,
        max(case when is_event_active then 1 else 0 end) as is_event_active,
        max(case when is_lead_period then 1 else 0 end) as is_lead_period,
        max(case when is_lag_period then 1 else 0 end) as is_lag_period,
        max(severity_weight) as severity_weight,
        max(case when is_global_event then 1 else 0 end) as is_global_event

    from event_months_union
    group by 1,2,3

),

time_map as (

    select
        period as month_key,
        year_month,
        month_start_date
    from "analytics"."analytics_staging"."stg_dim_time"

)

select
    d.event_id,
    -- Keep event-month rows joinable even when dim_time coverage lags event windows.
    coalesce(
        t.month_key,
        case
            when regexp_full_match(d.year_month, '^\\d{4}-\\d{2}$')
                then try_cast(replace(d.year_month, '-', '') as integer)
            when d.month_start_date is not null
                then
                    cast(extract(year from d.month_start_date) as integer) * 100
                    + cast(extract(month from d.month_start_date) as integer)
            else null
        end
    ) as month_key,
    d.year_month,
    d.month_start_date,
    cast(d.has_active_phase as boolean) as has_active_phase,
    cast(d.is_event_active as boolean) as is_event_active,
    cast(d.is_lead_period as boolean) as is_lead_period,
    cast(d.is_lag_period as boolean) as is_lag_period,
    d.severity_weight,
    cast(d.is_global_event as boolean) as is_global_event

from deduped d
left join time_map t
    on d.year_month = t.year_month