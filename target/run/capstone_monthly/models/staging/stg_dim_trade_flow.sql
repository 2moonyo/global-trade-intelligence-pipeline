

  create or replace view `fullcap-10111`.`analytics_staging`.`stg_dim_trade_flow`
  OPTIONS()
  as select
  flowCode as flow_code,
  flowDesc as flow_desc,
  flow_group
from `fullcap-10111`.`raw`.`dim_trade_flow`;

