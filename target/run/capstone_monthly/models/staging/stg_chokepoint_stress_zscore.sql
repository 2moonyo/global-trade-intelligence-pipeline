

  create or replace view `capfractal`.`analytics_staging`.`stg_chokepoint_stress_zscore`
  OPTIONS()
  as -- Compatibility layer over the canonical PortWatch stress metrics model.
-- Keeps the existing event-impact interface stable while sourcing all z-scores
-- from the dbt-derived point-in-time expanding baseline.

select
  chokepoint_id,
  year_month,
  throughput,
  mean_throughput,
  stddev_throughput,
  z_score,
  z_score_capacity,
  avg_n_total,
  z_score_count,
  vessel_size_index,
  z_score_vessel_size
from `capfractal`.`analytics_staging`.`stg_portwatch_stress_metrics`;

