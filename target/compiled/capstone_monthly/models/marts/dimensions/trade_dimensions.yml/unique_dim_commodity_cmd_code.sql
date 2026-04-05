
    
    

with dbt_test__target as (

  select cmd_code as unique_field
  from `capfractal`.`analytics_marts`.`dim_commodity`
  where cmd_code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


