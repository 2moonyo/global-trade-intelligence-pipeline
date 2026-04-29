with source as (
  select
    cmdCode as cmd_code,
    hs2,
    hs4,
    hs6,
    commodity_name,
    commodity_group,
    cast(food_flag as boolean) as food_flag,
    cast(energy_flag as boolean) as energy_flag,
    cast(industrial_flag as boolean) as industrial_flag
  from {{ source('raw', 'dim_commodity') }}
),

ranked as (
  select
    *,
    row_number() over (
      partition by cmd_code
      order by
        regexp_replace(lower(commodity_name), r'[^a-z0-9 ]', ''),
        length(commodity_name),
        commodity_name,
        hs6,
        commodity_group
    ) as commodity_row_rank
  from source
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
from ranked
where commodity_row_rank = 1
