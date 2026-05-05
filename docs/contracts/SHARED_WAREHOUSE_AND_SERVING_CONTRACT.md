# Shared Warehouse And Serving Contract

## Scope And Current Reality

This contract describes the shared behavior that applies across datasets.

The active baseline is VM-first:

1. source APIs or curated seed files
2. append-only or rerunnable bronze files under `data/bronze/*`
3. typed silver parquet or CSV assets under `data/silver/*`
4. optional metadata and run artifacts under `data/metadata/*` and `logs/*`
5. checksum-aware GCS publish where the dataset has a cloud path
6. BigQuery `raw.*` landing tables
7. dbt staging, dimensions, facts, marts, and semantic/dashboard marts
8. Looker Studio or equivalent BI querying BigQuery dbt outputs

The VM, persistent disk, shell wrappers, Bruin assets, and `ops/batch_plan.json` remain operationally important. Serverless is future/additive context and is not the baseline contract.

## Active Environment Contract

The current verified dbt runtime environment is:

| Concern | Current contract |
| --- | --- |
| dbt profile | `capstone_monthly` |
| active target | `bigquery_dev` |
| authentication | Google OAuth locally or VM metadata auth on GCP |
| raw dataset | `raw` by default, from `GCP_BIGQUERY_RAW_DATASET` when set |
| analytics dataset base | `analytics` by default, from `DBT_BIGQUERY_DATASET` or `GCP_BIGQUERY_ANALYTICS_DATASET` when set |
| dbt staging schema | `<analytics_dataset>_staging` |
| dbt marts schema | `<analytics_dataset>_marts` |
| dbt model materialization defaults | staging = `view`, marts = `table` |

Important environment truths:

- `profiles.yml` currently defines only the BigQuery target.
- `scripts/run_dbt.sh` sets `DBT_PROFILES_DIR` to the repo root and runs `--target ${DBT_TARGET:-bigquery_dev}`.
- Runtime configuration flows through `.env`, `/etc/capstone/pipeline.env`, Secret Manager, and VM/container env injection. Do not add competing secret sources.
- BigQuery raw tables are loaded by dataset-specific `warehouse/load_*_to_bigquery.py` scripts.
- Operational Postgres logging is optional in principle; local manifests and BigQuery/raw ops mirrors are the durable audit path.

## Architecture Reality

The warehouse is not a strict in-database medallion design. Bronze and silver live primarily as files. BigQuery `raw` is a landing layer for curated silver outputs, not a pure mirror of every bronze file.

Current raw source tables declared in dbt:

- trade and routing: `comtrade_fact`, `dim_country`, `dim_time`, `dim_commodity`, `dim_trade_flow`, `dim_chokepoint`, `dim_country_ports`, `route_applicability`, `dim_trade_routes`
- events: `dim_event`, `bridge_event_month_chokepoint_core`, `bridge_event_month_maritime_region`
- PortWatch: `portwatch_daily`, `portwatch_monthly`
- macro and energy: `brent_daily`, `brent_monthly`, `ecb_fx_eu_monthly`, `energy_vulnerability`
- operations mirror: `ops_pipeline_run`, `ops_task_run`, `ops_task_artifact`, `ops_partition_checkpoint`, `ops_retry_registry`

Each dataset contract names the canonical local asset, GCS prefix, BigQuery raw table, and dbt surface for that dataset.

## Cross-Source Canonical Rules

- `year_month` in `YYYY-MM` format is the primary human-readable analytical month key.
- `period` in `YYYYMM` integer form is the canonical monthly warehouse key for many trade and time joins.
- `month_start_date` is the preferred physical monthly date key when a real date column exists.
- ISO3 is the preferred country join key wherever a true country grain exists.
- `is_country_group` and `is_country_map_eligible` distinguish country groups from mappable countries in downstream marts.
- Chokepoint ids must use the shared canonical chokepoint mapping before hashing or joining.
- Derived analytical metrics belong in dbt unless there is a strong operational reason to materialize them earlier.
- Rerunnable and idempotent behavior matters more than minimizing file count at this project stage.

## Serving Layer Contract

### BigQuery And BI

The active serving path is:

```text
BigQuery raw -> dbt staging/dimensions/facts/marts -> dbt semantic marts -> Looker Studio or equivalent BI
```

The generated dbt documentation snapshot is available at `docs/dbt/index.html` and can be refreshed with:

```bash
make dbt-bigquery-docs-publish
```

### Core Analytical Marts

The core marts are reusable analytical surfaces and are not necessarily final dashboard grains:

- `fct_reporter_partner_commodity_month`
- `fct_reporter_partner_commodity_month_provenance`
- `fct_reporter_partner_commodity_month_lineage_detail`
- `fct_reporter_partner_commodity_route_month`
- `fct_reporter_partner_commodity_hub_month`
- `mart_reporter_month_trade_summary`
- `mart_reporter_commodity_month_trade_summary`
- `mart_trade_exposure`
- `mart_reporter_month_chokepoint_exposure`
- `mart_reporter_month_chokepoint_exposure_with_brent`
- `mart_hub_dependency_month`
- `mart_macro_monthly_features`
- `mart_reporter_month_macro_features`
- `mart_reporter_energy_vulnerability`
- `mart_event_impact`
- `mart_pipeline_progress`

### Semantic Dashboard Marts

The Looker-facing semantic layer lives under `models/marts/semantics`.

| Semantic mart | Grain | Primary use |
| --- | --- | --- |
| `mart_dashboard_global_trade_overview` | reporter country x month | Page 1 overview scorecards, trends, reporter ranking, and completeness messaging |
| `mart_trade_month_coverage_status` | month | Trade data coverage and latest complete month status |
| `mart_executive_monthly_system_snapshot` | month | Executive system snapshot over monthly chokepoint stress and coverage |
| `mart_chokepoint_daily_signal` | day x chokepoint | Daily chokepoint operations and alert bands |
| `mart_global_daily_market_signal` | day | Daily system-level chokepoint and Brent market signal |
| `mart_chokepoint_monthly_stress` | month x chokepoint | Monthly chokepoint stress, event overlay, and freshness |
| `mart_global_monthly_system_stress_summary` | month | System-level monthly PortWatch coverage and stress |
| `mart_chokepoint_monthly_stress_detail` | month x chokepoint | Drilldown stress bands and ranks |
| `mart_chokepoint_monthly_hotspot_map` | latest month x chokepoint | Chokepoint map points with stress and exposure context |
| `mart_reporter_partner_commodity_month_enriched` | reporter x partner x commodity x month x chokepoint/context | Trade exposure drilldown table |
| `mart_reporter_month_exposure_map` | latest month x eligible reporter country | Country map exposure snapshot |
| `mart_reporter_structural_vulnerability` | reporter x month | Page 5 structural vulnerability and concentration story |

Semantic marts should be business-readable, grain-explicit, and BI-friendly:

- human-readable names should be present beside ids
- map marts should expose mappable flags and stable geography fields
- completeness and latest-period fields may be repeated intentionally for BI scorecards
- Looker Studio should not need custom SQL to recover the intended grain

## Cross-Source Computed Models And Justification

## Canonical Time, Country, Commodity, And Geography Dimensions

`stg_dim_time`:

- is generated in dbt from observed months across trade, PortWatch, Brent, FX, energy, and event sources
- intentionally acts as a conformed monthly spine
- avoids coupling the warehouse to a single raw time table

`stg_dim_country` and `dim_country`:

- are rooted in curated Comtrade country assets
- canonicalize known labels and ISO3 edge cases
- expose group and map-eligibility flags for dashboards

`stg_dim_commodity` and `dim_commodity`:

- are rooted in Comtrade commodity metadata and observed fact commodities
- preserve HS code detail so dashboard aggregation does not erase source grain

`stg_dim_chokepoint` and `dim_chokepoint`:

- canonicalize chokepoint names before deriving ids
- retain numeric coordinates and BigQuery geography fields for map marts

## Reporter Summary Marts

`mart_reporter_month_trade_summary`:

- grain: one row per `reporter_iso3 + period + year_month`
- aggregates total, import, and export USD values and weights
- joins country and time dimensions

`mart_reporter_commodity_month_trade_summary`:

- grain: one row per `reporter_iso3 + cmd_code + period + year_month`
- provides commodity group context

Justification:

- these marts are cheap to query
- they give dashboards a stable summary layer
- they prevent repeated heavy scans of the canonical fact for common overview questions

## Reporter Chokepoint Exposure Marts

`mart_trade_exposure`:

- keeps route confidence explicit as part of the grain
- is the reusable analytical mart for event exposure country counting

`mart_reporter_month_chokepoint_exposure`:

- joins trade exposure to PortWatch stress and active events
- is the main combined reporter-risk surface

`mart_reporter_month_chokepoint_exposure_with_brent`:

- adds Brent context to the reporter-chokepoint exposure mart

Justification:

- separates structural trade dependence from traffic stress and event overlay
- lets dashboards mix risk signals without re-deriving them page by page

## Macro, Energy, And Structural Context Marts

`mart_macro_monthly_features`:

- grain: `year_month + fx_currency_code`
- joins Brent monthly and FX monthly

`mart_reporter_month_macro_features`:

- grain: `reporter_iso3 + period + fx_currency_code`
- joins reporter-month trade summaries to Brent monthly, FX monthly, and annual energy indicators broadcast by reporter and year

`mart_reporter_structural_vulnerability`:

- grain: `reporter_iso3 + month_start_date`
- combines annual energy vulnerability, monthly chokepoint exposure, historical event exposure, trade scale, supplier concentration, and top commodity concentration

Justification:

- macro context stays separate from core trade exposure logic
- annual energy values are intentionally broadcast to month grain because no true monthly source exists
- Page 5 structural risk stays at reporter-month grain by aggregating all lower-grain inputs before joining

## Event Impact Mart

`mart_event_impact`:

- final grain: one row per `event_id`
- join order is staged to avoid grain explosion

Current computation pattern:

1. build an `event_id + chokepoint_id + year_month` base
2. attach PortWatch z-scores
3. aggregate stress metrics to event grain
4. aggregate structural baseline throughput to event grain
5. compute realized throughput change vs baseline
6. separately count exposed countries via `mart_trade_exposure`
7. join conformed event attributes

Justification:

- event analytics must combine disruption intensity and structural relevance
- keeping event grain final makes the dashboard page stable and interpretable
- the mart deliberately avoids mixing trade and traffic joins too early, which would create duplicate event rows

## Quality And Governance Expectations

The highest-value blocking checks by layer are:

| Layer | Blocking checks |
| --- | --- |
| Bronze | partition-path consistency, required identifiers, parseable dates, non-empty extracts where a successful job is expected |
| Silver | uniqueness at declared grain, stable typing, join-key preservation, explicit handling of missing coverage |
| GCS publish | checksum-aware skipping, expected prefix shape, required source path presence |
| BigQuery raw landing | expected table existence, expected schema, expected date grain, no silent duplication from repeated loads |
| dbt staging | key not-null checks, unique grain checks, canonical code normalization |
| dbt marts | grain tests, allocation integrity for hub routing, event bridge deduplication, reporter exposure sanity |
| Semantic marts | one documented dashboard grain, BI-readable labels, map eligibility, latest-period fields |

## Migration And Hardening Notes

The next desired state is not a new architecture. It is hardening the current VM-first cloud path:

1. keep the VM persistent disk and runtime env stable
2. keep every dataset on explicit bronze -> silver -> GCS -> BigQuery -> dbt steps
3. keep Bruin assets aligned with real stage-level work while preserving shell wrappers
4. make logging durable without making optional sinks block successful data runs
5. add tests around grain, map eligibility, source coverage, and BigQuery load idempotency
6. preserve Looker-facing semantic marts as the preferred BI contract

Current maturity by dataset:

| Dataset | Local silver maturity | BigQuery maturity |
| --- | --- | --- |
| Comtrade | high, operationally complex | implemented, with partitioned fact loads and fixed route/dimension loads |
| PortWatch | high | implemented for daily and monthly raw tables |
| Brent | high | implemented for daily and monthly raw tables |
| FX | high for monthly silver | implemented for monthly raw table |
| World Bank energy | high for annual silver | implemented for annual raw table |
| Events | high for curated generated silver | implemented for event dimension and monthly bridge tables |
