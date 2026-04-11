
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select top_exposed_reporter_iso3 as from_field
    from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_hotspot_map`
    where top_exposed_reporter_iso3 is not null
),

parent as (
    select iso3 as to_field
    from `chokepoint-capfractal`.`analytics_marts`.`dim_country`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test