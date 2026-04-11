
    
    

with dbt_test__target as (

  select canonical_grain_key as unique_field
  from `chokepoint-capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month_provenance`
  where canonical_grain_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


