
  
  create view "analytics"."analytics_staging"."stg_dim_trade_flow__dbt_tmp" as (
    select
  flowCode as flow_code,
  flowDesc as flow_desc,
  flow_group
from "analytics"."raw"."dim_trade_flow"
  );
