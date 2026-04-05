
    
    

with dbt_test__target as (

  select iso3 as unique_field
  from `capfractal`.`analytics_marts`.`dim_country`
  where iso3 is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


