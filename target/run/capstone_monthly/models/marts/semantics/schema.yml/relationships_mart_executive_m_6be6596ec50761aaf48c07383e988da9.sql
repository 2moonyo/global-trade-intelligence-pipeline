
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select top_stressed_chokepoint_id as from_field
    from `chokepoint-capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
    where top_stressed_chokepoint_id is not null
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