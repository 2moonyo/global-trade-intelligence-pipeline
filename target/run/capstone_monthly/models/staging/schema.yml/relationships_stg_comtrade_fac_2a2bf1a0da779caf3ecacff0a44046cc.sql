
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select cmd_code as from_field
    from "analytics"."analytics_staging"."stg_comtrade_fact"
    where cmd_code is not null
),

parent as (
    select cmd_code as to_field
    from "analytics"."analytics_staging"."stg_dim_commodity"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test