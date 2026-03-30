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
from `capfractal`.`raw`.`dim_commodity`