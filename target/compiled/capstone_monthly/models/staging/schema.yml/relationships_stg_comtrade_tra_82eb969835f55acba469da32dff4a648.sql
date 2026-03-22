
    
    

with child as (
    select cmd_code as from_field
    from "analytics"."analytics_staging"."stg_comtrade_trade_base"
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


