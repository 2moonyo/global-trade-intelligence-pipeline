
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select top_exposed_chokepoint_id as from_field
    from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
    where top_exposed_chokepoint_id is not null
),

parent as (
    select chokepoint_id as to_field
    from `chokepoint-capfractal`.`analytics_marts`.`dim_chokepoint`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test