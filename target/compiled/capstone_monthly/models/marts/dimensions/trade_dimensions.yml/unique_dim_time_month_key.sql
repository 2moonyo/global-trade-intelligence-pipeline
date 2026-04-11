
    
    

with dbt_test__target as (

  select month_key as unique_field
  from `chokepoint-capfractal`.`analytics_marts`.`dim_time`
  where month_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


