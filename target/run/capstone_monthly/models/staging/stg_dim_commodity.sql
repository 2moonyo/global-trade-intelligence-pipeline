

  create or replace view `fullcap-10111`.`analytics_staging`.`stg_dim_commodity`
  OPTIONS()
  as select
  cmdCode as cmd_code,
  hs2,
  hs4,
  hs6,
  commodity_name,
  commodity_group,
  cast(food_flag as boolean) as food_flag,
  cast(energy_flag as boolean) as energy_flag,
  cast(industrial_flag as boolean) as industrial_flag
from `fullcap-10111`.`raw`.`dim_commodity`;

