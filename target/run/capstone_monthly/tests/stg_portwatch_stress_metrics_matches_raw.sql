
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with raw_portwatch as (
  select
    cast(chokepoint_id as string) as portwatch_source_chokepoint_id,
    cast(year_month as string) as year_month
  from `fullcap-10111`.`raw`.`portwatch_monthly`
  where year_month is not null
    and chokepoint_name is not null
),
stress_metrics as (
  select
    portwatch_source_chokepoint_id,
    year_month
  from `fullcap-10111`.`analytics_staging`.`stg_portwatch_stress_metrics`
),
missing_from_metrics as (
  select
    raw_portwatch.portwatch_source_chokepoint_id,
    raw_portwatch.year_month,
    'missing_from_metrics' as issue
  from raw_portwatch
  left join stress_metrics
    on raw_portwatch.portwatch_source_chokepoint_id = stress_metrics.portwatch_source_chokepoint_id
   and raw_portwatch.year_month = stress_metrics.year_month
  where stress_metrics.portwatch_source_chokepoint_id is null
),
unexpected_in_metrics as (
  select
    stress_metrics.portwatch_source_chokepoint_id,
    stress_metrics.year_month,
    'unexpected_in_metrics' as issue
  from stress_metrics
  left join raw_portwatch
    on raw_portwatch.portwatch_source_chokepoint_id = stress_metrics.portwatch_source_chokepoint_id
   and raw_portwatch.year_month = stress_metrics.year_month
  where raw_portwatch.portwatch_source_chokepoint_id is null
)

select *
from missing_from_metrics

union all

select *
from unexpected_in_metrics
  
  
      
    ) dbt_internal_test