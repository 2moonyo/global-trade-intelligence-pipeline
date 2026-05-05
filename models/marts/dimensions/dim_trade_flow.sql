with base as (
  select
    flow_code,
    flow_desc,
    flow_group
  from {{ ref('stg_dim_trade_flow') }}
)

select
  flow_code,
  flow_desc,
  flow_group
from base
