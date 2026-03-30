

  create or replace view `capfractal`.`analytics_staging`.`stg_dim_trade_flow`
  OPTIONS()
  as select
  flowCode as flow_code,
  flowDesc as flow_desc,
  flow_group
from `capfractal`.`raw`.`dim_trade_flow`;

