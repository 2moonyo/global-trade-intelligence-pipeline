
    
    

with dbt_test__target as (

  select flow_code as unique_field
  from `chokepoint-capfractal`.`analytics_marts`.`dim_trade_flow`
  where flow_code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


