select
  flowCode as flow_code,
  flowDesc as flow_desc,
  flow_group
from {{ source('raw', 'dim_trade_flow') }}
