
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select partner_iso3 as from_field
    from "analytics"."analytics_staging"."stg_comtrade_trade_base"
    where partner_iso3 is not null
),

parent as (
    select iso3 as to_field
    from "analytics"."analytics_staging"."stg_dim_country"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test