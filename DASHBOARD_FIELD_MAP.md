# Dashboard Field Map

This file maps dashboard pages, charts, and recommended semantic marts.

## Looker Studio Design Rules

Keep these rules active when rebuilding the report:

- Use one mart per chart where possible.
- Avoid Looker blends unless absolutely necessary.
- Use `date_day` for daily visuals.
- Use `month_start_date` for monthly visuals.
- Do not use integer `YYYYMM` alone for date controls.
- Do not use non-map-safe marts directly in maps.
- Prefer precomputed metrics from dbt over Looker calculated fields.

## Page 1 Field Map

### Primary marts

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress_detail`

### Scorecards

| Scorecard | Mart | Field |
| --- | --- | --- |
| Latest Total Trade | `mart_dashboard_global_trade_overview` | `total_trade_value_usd` filtered to latest month and aggregated |
| Reporting Completeness | `mart_trade_month_coverage_status` | `reporting_completeness_pct` filtered to latest month |
| Stress Level | `mart_executive_monthly_system_snapshot` | `system_stress_level` filtered to latest month |
| Top Stressed Chokepoint | `mart_executive_monthly_system_snapshot` | `top_stressed_chokepoint_name` filtered to latest month |
| Stressed Chokepoints | `mart_executive_monthly_system_snapshot` | `stressed_chokepoint_count` filtered to latest month |

### Charts and tables

| Visual | Mart | Dimension | Metric(s) | Notes |
| --- | --- | --- | --- | --- |
| Trade Trend Over Time | `mart_dashboard_global_trade_overview` | `month_start_date` | `total_trade_value_usd` | aggregate to month |
| Trade Reporting Completeness Over Time | `mart_trade_month_coverage_status` | `month_start_date` | `reporting_completeness_pct` | month grain only |
| Stressed Chokepoints Score | `mart_chokepoint_monthly_stress_detail` | `chokepoint_name` | `stress_deviation_score_capped` | filter latest month, sort by `stress_rank_in_month` |
| Trade Reporting Data | `mart_dashboard_global_trade_overview` | reporter columns | trade metrics | reporter table |
| Latest Chokepoint Movement | `mart_chokepoint_monthly_stress_detail` | `chokepoint_name` | stress fields | filter latest month |

### Recommended filters

- `month_start_date`
- `reporter_country_name`

### Recommended latest-month filters

- Page 1 scorecards: `latest_month_flag = true`
- Ranked stress table: `latest_month_flag = true`

## Page 2 Field Map

### Primary marts

- `mart_global_daily_market_signal`
- `mart_chokepoint_daily_signal`

### Scorecards

| Scorecard | Mart | Field |
| --- | --- | --- |
| Daily Coverage Status | `mart_global_daily_market_signal` | `daily_source_coverage_status` |
| Observed Chokepoint Count | `mart_global_daily_market_signal` | `observed_chokepoint_count` |
| Stress Chokepoint Count | `mart_global_daily_market_signal` | `stressed_chokepoint_count` |
| System Stress Level | `mart_global_daily_market_signal` | `system_stress_level` |
| Stressed Chokepoints | `mart_global_daily_market_signal` | supporting text table or filtered count |

### Charts and tables

| Visual | Mart | Dimension | Metric(s) | Notes |
| --- | --- | --- | --- | --- |
| Daily system trend | `mart_global_daily_market_signal` | `date_day` | `avg_abs_chokepoint_signal_index`, `brent_price_usd` | separate axes if needed |
| Chokepoint score bar chart | `mart_chokepoint_daily_signal` | `chokepoint_name` | `signal_index_rolling_30d` or display-safe alternative if added later | latest day only |
| Latest daily table | `mart_chokepoint_daily_signal` | `chokepoint_name` | `pct_change_1d`, `z_score_rolling_30d`, `alert_band`, `direction_of_change` | latest day only |

### Recommended filters

- `date_day`
- `chokepoint_name`

### Important page rule

- Keep Page 2 daily-only.
- Do not row-join monthly trade into this page.

## Page 3 Field Map

### Primary marts

- `mart_chokepoint_monthly_stress`
- `mart_global_monthly_system_stress_summary`

### Planned visuals

| Visual | Mart | Dimension | Metric(s) |
| --- | --- | --- | --- |
| Monthly stress comparison | `mart_chokepoint_monthly_stress` | `month_start_date`, `chokepoint_name` | `stress_index`, `stress_index_weighted`, `z_score_historical` |
| Monthly system scorecards | `mart_global_monthly_system_stress_summary` | `month_start_date` | `avg_stress_index`, `stressed_chokepoint_count`, `monthly_coverage_ratio` |
| Event overlay table | `mart_chokepoint_monthly_stress` | `chokepoint_name` | `active_event_count`, `event_active_flag` |

## Page 4 Field Map

### Primary marts

- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_chokepoint_monthly_hotspot_map`

### Country map

| Visual | Mart | Geo dimension | Metric(s) | Notes |
| --- | --- | --- | --- | --- |
| Primary country exposure map | `mart_reporter_month_exposure_map` | `reporter_iso3` | `high_medium_chokepoint_exposed_trade_share` | recommended default choropleth metric |

Tooltip fields:

- `reporter_country_name`
- `total_chokepoint_exposed_trade_value_usd`
- `exposed_chokepoint_count`
- `top_exposed_chokepoint_name`
- `trade_value_weighted_stress_index_weighted`

### Chokepoint point map

| Visual | Mart | Point fields | Metric(s) | Notes |
| --- | --- | --- | --- | --- |
| Supporting chokepoint hotspot map | `mart_chokepoint_monthly_hotspot_map` | `latitude`, `longitude` | `stress_deviation_score_capped` or `stress_deviation_index_100` | supporting visual only |

Tooltip fields:

- `chokepoint_name`
- `stress_severity_band`
- `total_exposed_trade_value_usd`
- `exposed_reporter_count`
- `top_exposed_reporter_country_name`

### Detail tables

| Visual | Mart | Dimensions | Metrics |
| --- | --- | --- | --- |
| Reporter-partner-commodity exposure table | `mart_reporter_partner_commodity_month_enriched` | `reporter_country_name`, `partner_country_name`, `commodity_name`, `chokepoint_name`, `route_group` | `total_trade_value_usd`, `chokepoint_exposed_trade_share_of_reporter_total`, `partner_commodity_trade_share_of_reporter_chokepoint` |

### Recommended Page 4 filters

- `month_start_date`
- `reporter_country_name`
- `partner_country_name`
- `commodity_name`
- `chokepoint_name`
- `route_group`
- `route_confidence_score`

## Page 5 Future Field Map

Not yet productized, but likely future marts and fields:

- `mart_reporter_energy_vulnerability`
- future structural vulnerability semantic mart

Likely fields:

- `reporter_country_name`
- annual energy indicators
- dependency bands

## Page 6 Future Field Map

Not yet productized.

Likely future marts:

- future event-month-country-trade semantic mart
- existing `mart_event_impact` as upstream support

## Semantic Mart Filter Compatibility

| Mart | Daily / Monthly | Safe filters |
| --- | --- | --- |
| `mart_dashboard_global_trade_overview` | Monthly | reporter, month |
| `mart_trade_month_coverage_status` | Monthly | month |
| `mart_executive_monthly_system_snapshot` | Monthly | month |
| `mart_chokepoint_monthly_stress_detail` | Monthly | month, chokepoint |
| `mart_chokepoint_daily_signal` | Daily | date, chokepoint |
| `mart_global_daily_market_signal` | Daily | date |
| `mart_chokepoint_monthly_stress` | Monthly | month, chokepoint |
| `mart_global_monthly_system_stress_summary` | Monthly | month |
| `mart_reporter_partner_commodity_month_enriched` | Monthly | reporter, partner, commodity, chokepoint, route group, month |
| `mart_reporter_month_exposure_map` | Monthly | reporter, month |
| `mart_chokepoint_monthly_hotspot_map` | Monthly | chokepoint, month |

## Existing Looker Wiring Freeze

Embedded sources seen in the screenshot:

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress_detail`
- `mart_global_daily_market_signal`
- `mart_chokepoint_daily_signal`

Not yet seen in the screenshot:

- `mart_chokepoint_monthly_stress`
- `mart_global_monthly_system_stress_summary`
- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_chokepoint_monthly_hotspot_map`
