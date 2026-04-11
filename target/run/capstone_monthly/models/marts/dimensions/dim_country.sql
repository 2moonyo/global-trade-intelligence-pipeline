
  
    

    create or replace table `chokepoint-capfractal`.`analytics_marts`.`dim_country`
      
    
    

    
    OPTIONS()
    as (
      with base as (
  select
    country_code,
    iso3,
    country_name,
    region,
    subregion,
    continent,
    is_eu,
    is_oecd
  from `chokepoint-capfractal`.`analytics_staging`.`stg_dim_country`
)

select
  country_code,
  iso3,
  country_name,
  region,
  subregion,
  continent,
  is_eu,
  is_oecd
from base
    );
  