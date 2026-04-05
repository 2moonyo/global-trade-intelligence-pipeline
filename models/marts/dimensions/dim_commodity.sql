with base as (
  select
    cmd_code,
    hs2,
    hs4,
    hs6,
    commodity_name,
    commodity_group,
    food_flag,
    energy_flag,
    industrial_flag
  from {{ ref('stg_dim_commodity') }}
)

select
  cmd_code,
  hs2,
  hs4,
  hs6,
  commodity_name,
  commodity_group,
  food_flag,
  energy_flag,
  industrial_flag
from base
