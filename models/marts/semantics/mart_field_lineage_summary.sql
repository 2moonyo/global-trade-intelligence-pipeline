-- Readable dashboard field lineage summary.
-- Grain: one row per dashboard_page + field_name.

select
  'Page 1 Global Trade Overview' as dashboard_page,
  'Executive Scorecards' as chart_group,
  'total_trade_value_usd' as field_name,
  'Total Trade Value (USD)' as display_name,
  'comtrade' as source_dataset,
  'mart_dashboard_global_trade_overview' as source_model,
  'mart_dashboard_global_trade_overview <- mart_reporter_month_trade_summary <- fct_reporter_partner_commodity_month <- stg_comtrade_fact' as upstream_models_summary,
  'reporter_country_code + month_start_date' as grain,
  'Monthly reporter total across all partners and commodities.' as calculation_summary,
  'Sum across reporters only after checking reporting completeness for the selected month.' as aggregation_guidance,
  'Recent Comtrade months can be incomplete because official reporter submissions arrive late.' as known_limitation,
  'Filter to latest_complete_month_flag = true for headline scorecards.' as recommended_filter

union all

select
  'Page 1 Global Trade Overview',
  'Coverage Context',
  'reporting_completeness',
  'Reporting Completeness',
  'comtrade',
  'mart_trade_month_coverage_status',
  'mart_trade_month_coverage_status <- mart_dashboard_global_trade_overview <- mart_reporter_month_trade_summary',
  'month_start_date',
  'Share of expected reporters with observed trade data in the month.',
  'Do not sum. Use the month-level ratio directly or average only across explicitly comparable months.',
  'Low completeness often reflects delayed official reporting rather than true zero trade.',
  'Use latest_month_flag or latest_complete_month_flag depending whether you want timeliness or completeness.'

union all

select
  'Page 2 Daily Chokepoint Signal',
  'Daily Stress Trends',
  'z_score_rolling_30d',
  '30-Day Z-Score',
  'portwatch_daily',
  'mart_chokepoint_daily_signal',
  'mart_chokepoint_daily_signal <- stg_portwatch_daily',
  'date_day + chokepoint_id',
  'Current daily throughput deviation relative to the trailing 30 observed days.',
  'Do not sum across chokepoints. Use average or compare individual chokepoint traces.',
  'Null means insufficient baseline or missing PortWatch daily observations.',
  'Filter on a single chokepoint or use date_day windows with enough history.'

union all

select
  'Page 2 Monthly Stress Detail',
  'Stress Severity',
  'stress_index',
  'Stress Index',
  'portwatch_monthly',
  'mart_chokepoint_monthly_stress_detail',
  'mart_chokepoint_monthly_stress_detail <- mart_chokepoint_monthly_stress <- stg_portwatch_stress_metrics',
  'month_start_date + chokepoint_id',
  'Monthly blended stress measure from vessel-count and throughput z-scores.',
  'Do not sum across chokepoints. Use ranking, average, or max depending on the visual.',
  'Missing PortWatch months remain null by design instead of being filled.',
  'Filter to stress_severity_band <> INSUFFICIENT_BASELINE for ranked comparisons.'

union all

select
  'Page 4 Reporter Exposure Map',
  'Latest Reporter Snapshot',
  'chokepoint_exposure_pct',
  'Chokepoint Exposure %',
  'comtrade + portwatch_monthly + events',
  'mart_reporter_month_energy_trade_dependency',
  'mart_reporter_month_energy_trade_dependency <- mart_reporter_structural_vulnerability <- mart_reporter_month_chokepoint_exposure + mart_reporter_energy_vulnerability',
  'reporter_iso3 + month_start_date',
  'Percent of reporter monthly trade value routed through modeled chokepoint exposures.',
  'Do not sum percentages. Use average for group views or pair with trade value for weighting.',
  'Exposure depends on modeled trade routes and can understate reality where routing confidence is low.',
  'Filter to latest_month_flag = true for map snapshots.'

union all

select
  'Page 5 Structural Vulnerability',
  'Risk Scorecards',
  'structural_risk_score',
  'Structural Risk Score',
  'comtrade + worldbank_energy + portwatch_monthly + events',
  'mart_reporter_structural_vulnerability',
  'mart_reporter_structural_vulnerability <- mart_reporter_month_trade_summary + mart_reporter_energy_vulnerability + mart_reporter_month_chokepoint_exposure + dim_event bridges',
  'reporter_iso3 + month_start_date',
  'Weighted 0-100 score combining energy import dependence, renewable gap, chokepoint exposure, supplier concentration, and event context.',
  'Use as a ranking or scatter axis, not as an additive measure.',
  'Annual World Bank inputs are broadcast to month grain and therefore provide structural context rather than monthly movement.',
  'Filter to latest_month_flag = true for latest-country comparisons.'

union all

select
  'Page 5 Structural Vulnerability',
  'Energy Structure',
  'energy_import_pct',
  'Energy Import %',
  'worldbank_energy',
  'mart_reporter_month_energy_trade_dependency',
  'mart_reporter_month_energy_trade_dependency <- mart_reporter_structural_vulnerability <- mart_reporter_energy_vulnerability <- stg_energy_vulnerability',
  'reporter_iso3 + month_start_date',
  'World Bank dependency on imported energy indicator matched by reporter-year and broadcast to month.',
  'Average or compare across reporters; do not sum.',
  'Annual structural indicator; recent months repeat the same yearly value until a new annual release lands.',
  'Filter to latest_month_flag = true and compare alongside structural_risk_score.'

union all

select
  'Page 5 Structural Vulnerability',
  'Energy Structure',
  'renewable_share_pct',
  'Renewable Share %',
  'worldbank_energy',
  'mart_reporter_month_energy_trade_dependency',
  'mart_reporter_month_energy_trade_dependency <- mart_reporter_structural_vulnerability <- mart_reporter_energy_vulnerability <- stg_energy_vulnerability',
  'reporter_iso3 + month_start_date',
  'World Bank renewable energy share broadcast from annual country-year data to the monthly reporter view.',
  'Average or compare across reporters; do not sum.',
  'Annual source repeated at month grain; not a true monthly signal.',
  'Filter to latest_month_flag = true for current structural snapshots.'

union all

select
  'Page 6 Bloc Trade And Macro',
  'Bloc Scale',
  'bloc_total_trade_value_usd',
  'Bloc Total Trade Value (USD)',
  'comtrade',
  'mart_bloc_month_trade_macro_summary',
  'mart_bloc_month_trade_macro_summary <- mart_reporter_commodity_month_trade_summary <- fct_reporter_partner_commodity_month <- stg_comtrade_fact',
  'bloc_code + month_start_date',
  'Sum of member-country monthly trade values inside each analytical bloc.',
  'Do not add across blocs because countries can belong to multiple blocs.',
  'Bloc totals can understate real activity when member reporting coverage is partial.',
  'Filter to bloc_reporting_coverage_pct >= 0.70 for cleaner bloc comparisons.'

union all

select
  'Page 6 Bloc Trade And Macro',
  'Bloc Composition',
  'food_share_of_bloc_trade_pct',
  'Food Share of Bloc Trade %',
  'comtrade',
  'mart_bloc_month_trade_macro_summary',
  'mart_bloc_month_trade_macro_summary <- mart_reporter_commodity_month_trade_summary + dim_commodity',
  'bloc_code + month_start_date',
  'Food-flagged trade value divided by bloc total trade value.',
  'Do not sum percentages. Compare within the same month or average across a filtered time window.',
  'Interpret with bloc reporting coverage because missing members change the denominator.',
  'Filter on one bloc and use contiguous months for trend charts.'

union all

select
  'Page 6 Bloc Trade And Macro',
  'Bloc Composition',
  'oil_share_of_bloc_trade_pct',
  'Oil Share of Bloc Trade %',
  'comtrade',
  'mart_bloc_month_trade_macro_summary',
  'mart_bloc_month_trade_macro_summary <- mart_reporter_commodity_month_trade_summary + dim_commodity',
  'bloc_code + month_start_date',
  'Oil proxy trade value using crude, refined petroleum, and LNG proxy commodity codes divided by bloc total trade.',
  'Do not sum percentages. Compare inside bloc or average over time.',
  'Commodity proxy is intentionally narrow and does not represent all energy products.',
  'Filter to bloc_reporting_coverage_pct >= 0.70 when using month-over-month comparisons.'

union all

select
  'Page 6 Bloc Trade And Macro',
  'Bloc Composition',
  'energy_share_of_bloc_trade_pct',
  'Energy Share of Bloc Trade %',
  'comtrade',
  'mart_bloc_month_trade_macro_summary',
  'mart_bloc_month_trade_macro_summary <- mart_reporter_commodity_month_trade_summary + dim_commodity',
  'bloc_code + month_start_date',
  'Energy-flagged trade value divided by bloc total trade value.',
  'Do not sum percentages. Use averages or within-month comparisons.',
  'Energy commodity tagging depends on the conformed commodity dimension and excludes some edge classifications.',
  'Filter to a single bloc and keep coverage warnings visible.'

union all

select
  'Page 6 Bloc Brent FX Correlation',
  'Macro Inputs',
  'brent_price_usd',
  'Brent Price (USD)',
  'brent_monthly',
  'mart_brent_fx_trade_correlation_monthly',
  'mart_brent_fx_trade_correlation_monthly <- mart_macro_monthly_features <- stg_brent_monthly',
  'currency_view + base_currency_code + fx_currency_code + bloc_code + month_start_date',
  'Monthly Brent benchmark price carried into the bloc correlation panel.',
  'Do not sum. Use average, latest value, or time-series display.',
  'Public Brent data is lower latency than trade but still reflects monthly aggregation rather than intramonth movement.',
  'Filter on one currency_view and one bloc for readable comparisons.'

union all

select
  'Page 6 Bloc Brent FX Correlation',
  'Macro Inputs',
  'brent_mom_change',
  'Brent MoM Change',
  'brent_monthly',
  'mart_brent_fx_trade_correlation_monthly',
  'mart_brent_fx_trade_correlation_monthly <- mart_macro_monthly_features <- stg_brent_monthly',
  'currency_view + base_currency_code + fx_currency_code + bloc_code + month_start_date',
  'Month-over-month percent change in the Brent benchmark series.',
  'Do not sum. Use as a time-series field or correlation input.',
  'Only comparable on contiguous monthly history and does not imply causality.',
  'Filter out null months and consider is_oil_shock_5pct for stress scenarios.'

union all

select
  'Page 6 Bloc Brent FX Correlation',
  'Macro Inputs',
  'fx_mom_change',
  'FX MoM Change',
  'fx_monthly',
  'mart_brent_fx_trade_correlation_monthly',
  'mart_brent_fx_trade_correlation_monthly <- mart_macro_monthly_features <- stg_fx_monthly',
  'currency_view + base_currency_code + fx_currency_code + bloc_code + month_start_date',
  'Month-over-month percent change in the selected monthly FX pair view.',
  'Do not sum. Use as a time-series or correlation input.',
  'Comparability depends on keeping one currency view and base/quote pair fixed.',
  'Filter to a single currency_view and currency pair.'

union all

select
  'Page 6 Bloc Brent FX Correlation',
  'Trade Change Inputs',
  'mom_change_food_trade_pct',
  'Food Trade MoM Change %',
  'comtrade',
  'mart_brent_fx_trade_correlation_monthly',
  'mart_brent_fx_trade_correlation_monthly <- mart_bloc_month_trade_macro_summary <- mart_reporter_commodity_month_trade_summary',
  'currency_view + base_currency_code + fx_currency_code + bloc_code + month_start_date',
  'Contiguous-month percent change in bloc food trade value.',
  'Do not sum. Use only for time-series comparison or rolling correlation windows.',
  'Null when prior month is missing; reliability falls when bloc reporter coverage is partial.',
  'Filter to food_trade_change_reliability_flag = true for cleaner correlation reads.'

union all

select
  'Page 6 Bloc Brent FX Correlation',
  'Trade Change Inputs',
  'mom_change_oil_trade_pct',
  'Oil Trade MoM Change %',
  'comtrade',
  'mart_brent_fx_trade_correlation_monthly',
  'mart_brent_fx_trade_correlation_monthly <- mart_bloc_month_trade_macro_summary <- mart_reporter_commodity_month_trade_summary',
  'currency_view + base_currency_code + fx_currency_code + bloc_code + month_start_date',
  'Contiguous-month percent change in bloc oil-proxy trade value.',
  'Do not sum. Use only for time-series comparison or rolling correlation windows.',
  'Oil proxy trade uses a narrow commodity subset and is sensitive to bloc reporting gaps.',
  'Filter to oil_trade_change_reliability_flag = true and keep bloc coverage visible.'
