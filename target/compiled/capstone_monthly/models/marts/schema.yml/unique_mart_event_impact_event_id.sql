
    
    

with dbt_test__target as (

  select event_id as unique_field
  from `chokepoint-capfractal`.`analytics_marts`.`mart_event_impact`
  where event_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


