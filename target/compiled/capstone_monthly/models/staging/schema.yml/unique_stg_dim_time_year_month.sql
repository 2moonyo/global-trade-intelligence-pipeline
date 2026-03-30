
    
    

with dbt_test__target as (

  select year_month as unique_field
  from `capfractal`.`analytics_staging`.`stg_dim_time`
  where year_month is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


