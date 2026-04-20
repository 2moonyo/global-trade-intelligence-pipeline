# Comtrade Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted with checkpoint and registry support
- Canonical silver fact slices and dimensions are scriptable through `ingest/comtrade/comtrade_silver.py`
- Routing assets are scriptable through `python -m ingest.comtrade.routing`
- GCS publish is implemented in `warehouse/publish_comtrade_to_gcs.py`
- BigQuery raw landing is implemented in `warehouse/load_comtrade_to_bigquery.py`
- dbt staging, dimensions, facts, marts, and semantic/dashboard marts are BigQuery-facing

## Source Systems

- UN Comtrade Data API
- Comtrade metadata extracts under `data/metadata/comtrade`
- Routing support data generated from the repository routing package and local/geospatial support assets

## Purpose

- Provide canonical reporter-partner-commodity-month-flow trade facts
- Support dependence analysis, route enrichment, hub dependency, chokepoint exposure, country map exposure, and structural vulnerability analysis

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze monthly history | Comtrade JSON extracts used by the scripted silver builder | request-specific | `year=YYYY` | `data/bronze/comtrade/monthly_history/year=YYYY/*.json` | Current authoritative bronze history path for scripted silver rebuilds. |
| Bronze audit metadata | extraction registry, checkpoint, and run manifests | one row per job or checkpoint snapshot | append-only JSONL / JSON plus per-run artifact folders | `data/metadata/comtrade/state/extraction_registry.jsonl`, `data/metadata/comtrade/state/comtrade_checkpoint.json`, `logs/comtrade/*.jsonl`, `data/metadata/comtrade/ingest_reports/run_id=<run_id>/` | Operational record of quota hits, completed jobs, coverage gaps, silver slice writes, and routing outputs. |
| Legacy bronze annual/monthly/event extracts | older extract layouts | request-specific | mixed | `data/bronze/comtrade/year=YYYY/...`, `data/bronze/comtrade/events/...` | Historical compatibility paths; not the active VM batch contract. |
| Silver curated fact | `comtrade_fact` | source-like monthly trade rows | `year=YYYY/month=MM/reporter_iso3=ISO3/cmd_code=CODE/flow_code=FLOW` | `data/silver/comtrade/comtrade_fact/year=YYYY/month=MM/reporter_iso3=ISO3/cmd_code=CODE/flow_code=FLOW/comtrade_fact.parquet` | Canonical scripted storage layout. Each slice is overwritten in place and skipped when unchanged. |
| Silver compatibility summaries | `reporter_month`, `partner_month`, `cmd_month` | summary grains | none | `data/silver/comtrade/comtrade_fact/*.parquet` | Compatibility snapshots; not the BigQuery raw fact contract. |
| Silver curated dimensions | `dim_country`, `dim_time`, `dim_commodity`, `dim_trade_flow`, `dim_chokepoint`, `dim_country_ports` | dimension-specific | mostly none | `data/silver/comtrade/dimensions/*.parquet` | Scripted by silver/routing builders. |
| Silver routing outputs | `route_applicability`, `dim_trade_routes`, and routing support dimensions | route/pair-specific | mostly none | `data/silver/comtrade/dimensions/bridge_country_route_applicability.parquet`, `data/silver/comtrade/dim_trade_routes.parquet` | Scripted by the routing package. |
| GCS landing | published bronze, silver, routing, metadata, and audit artifacts | asset-specific | GCS prefixes by family | `gs://<bucket>/<prefix>/bronze/comtrade/...`, `gs://<bucket>/<prefix>/silver/comtrade/...`, `gs://<bucket>/<prefix>/metadata/comtrade/...` | Checksum-aware publish with month filters. |
| BigQuery raw landing | `raw.comtrade_fact`, fixed dimensions/routes, load audit/state tables | table-specific | fact partitioned by `ref_date`, clustered by `reporter_iso3`, `cmdCode`, `flowCode`; route support clustered where useful | `raw.*` | Loaded by `warehouse/load_comtrade_to_bigquery.py`; fact load replaces touched partitions by default. |
| dbt staging | `stg_comtrade_trade_base`, `stg_comtrade_fact`, `stg_dim_country`, `stg_dim_commodity`, `stg_dim_time`, `stg_dim_trade_flow`, `stg_dim_chokepoint`, `stg_dim_country_ports`, `stg_route_applicability`, `stg_dim_trade_route_geography` | model-specific | dbt-managed | analytics staging schema | Standardizes codes, time keys, geography, and route evidence fields. |
| dbt dimensions and facts | conformed dimensions plus trade, route, hub, and provenance facts | model-specific | dbt-managed | analytics marts schema | Canonical analytical surfaces. |
| dbt marts | trade summary, trade exposure, hub dependency, macro features, structural vulnerability, and semantic/dashboard marts | mart-specific | dbt-managed | analytics marts schema | Main analytical outputs consumed by BI. |

## BigQuery Raw Tables

The current Comtrade raw load path manages:

- `raw.comtrade_fact`
- `raw.dim_country`
- `raw.dim_time`
- `raw.dim_commodity`
- `raw.dim_trade_flow`
- `raw.dim_chokepoint`
- `raw.dim_country_ports`
- `raw.route_applicability`
- `raw.dim_trade_routes`
- `raw.comtrade_load_audit`
- `raw.comtrade_load_state`

## Canonical Business Grain

The canonical analytical trade grain is:

- `reporter_iso3`
- `partner_iso3`
- `cmd_code`
- `period`
- `year_month`
- `trade_flow`

This grain is represented by the stable `canonical_grain_key` in dbt.

## Operational Silver Storage Contract

The scripted silver builder separates row identity from rewrite granularity.

Base-row dedupe grain:

- `period`
- `reporter_iso3`
- `partner_iso3`
- `flowCode`
- `cmdCode`
- `customsCode`
- `motCode`
- `partner2Code`

This preserves operational fields that are required to eliminate false duplicates in the Comtrade source rows.

Physical rewrite and cloud-load slice:

- `period`
- `reporter_iso3`
- `cmdCode`
- `flowCode`

Implications:

- reruns replace only the touched monthly reporter-commodity-flow slice
- distinct `partner_iso3`, `customsCode`, `motCode`, and `partner2Code` rows are preserved inside that slice
- unchanged slices are skipped by fingerprint so they are not rewritten locally or reloaded upstream

## Current Required Fields

Raw/silver fact essentials:

- `ref_date`
- `period`
- `year_month`
- `ref_year`
- `reporter_iso3`
- `partner_iso3`
- `flowCode`
- `cmdCode`
- `trade_flow`
- `trade_value_usd`
- `netWgt`
- `grossWgt`
- `qty`
- `motCode`
- `partner2Code`
- source lineage fields such as load batch and source file where available

Cleaned staging and canonical fact essentials:

- `canonical_grain_key`
- `ref_date`
- `period`
- `year_month`
- `ref_year`
- `reporter_iso3`
- `partner_iso3`
- `cmd_code`
- `trade_flow`
- `trade_value_usd`

Common optional-but-important analytical fields:

- `net_weight_kg`
- `gross_weight_kg`
- `qty`
- `mot_code`
- `partner2_code`
- raw lineage columns retained in silver and provenance-compatible raw inputs

## Canonical Trade Fact Logic

`stg_comtrade_trade_base`:

- normalizes `period`, `year_month`, and `ref_year`
- uppercases country ISO3 values
- maps `flowCode` values `M` and `X` to analytical import/export labels
- removes structurally unusable rows
- creates `canonical_grain_key` from reporter, partner, commodity, period, and flow

`fct_reporter_partner_commodity_month`:

- keeps the declared analytical grain
- aggregates `trade_value_usd`, `net_weight_kg`, `gross_weight_kg`, and `qty`
- computes `usd_per_kg`
- counts source records contributing to each canonical row

`fct_reporter_partner_commodity_month_provenance`:

- aggregates raw row lineage to the canonical grain
- preserves batch ids, source files, and bronze extraction timestamps
- exists specifically to keep analytical aggregation auditable

`fct_reporter_partner_commodity_month_lineage_detail`:

- exposes row-level lineage detail from `raw.comtrade_fact` for audit and troubleshooting

## Routing Logic Contract

Routing is intentionally modelled as a pair-level analytical enrichment, not as commodity-ground-truth shipping telemetry.

Inputs:

- `raw.dim_trade_routes`
- `raw.route_applicability`

The route logic in `fct_reporter_partner_commodity_route_month` works as follows:

1. Select one preferred route candidate per `reporter_iso3 + partner_iso3`.
   - `default_shortest` is prioritized when multiple route scenarios exist.
2. Summarize pair-level transport evidence from `stg_route_applicability`.
   - `has_sea`
   - `has_inland_water`
   - `has_unknown`
   - `has_non_marine`
3. Join the canonical trade fact to the preferred route and applicability evidence.
4. Compute:
   - `route_applicability_status`
   - `mot_code_filter_status`
   - `is_maritime_routed`
   - `route_confidence_score`

Current confidence interpretation:

- `HIGH`
  - routed with high or very high route confidence
  - and maritime evidence exists
- `MEDIUM`
  - maritime-routed with sea or inland-water evidence but weaker route confidence
- `LOW`
  - routed with unknown evidence or weaker inferred support
- `VERY_LOW`
  - explicitly non-maritime-only evidence

Justification:

- pair-level route enrichment preserves the canonical trade row count
- `motCode` evidence is used as a guardrail against over-claiming maritime exposure
- one preferred route per pair keeps the main exposure mart interpretable and cheaper to query
- the model is designed for risk and exposure analytics, not vessel-by-vessel route reconstruction

## Hub Allocation Logic Contract

`fct_reporter_partner_commodity_hub_month` expands the canonical fact to `partner2_iso3` variants while preserving additive integrity.

Allocation method:

1. Aggregate route-applicability evidence by `reporter_iso3 + partner_iso3 + partner2_iso3`.
2. Compute `partner2_trade_value_usd` and pair totals.
3. Allocate each canonical trade row by:
   - observed `partner2_trade_value_usd / pair_trade_value_usd` when positive totals exist
   - equal shares across variants when totals exist but values are zero
   - a fallback row with `partner2_iso3 = null` and allocation share `1.0` when no hub variants exist

Justification:

- this preserves total trade value across hub expansion
- it makes hub dependency analytically useful without corrupting canonical totals
- it supports a dedicated hub dependency mart without forcing every dashboard view into hub grain

## Exposure Logic Contract

`mart_trade_exposure`:

- grain: `reporter_iso3 + chokepoint_id + year_month + route_confidence_score`
- numerator: maritime-routed trade value assigned to a chokepoint
- denominator: total reporter monthly trade value
- output: `chokepoint_trade_exposure_ratio`

`mart_reporter_month_chokepoint_exposure`:

- joins reporter-chokepoint trade exposure to:
  - PortWatch stress metrics
  - event counts and severity from the event bridge
- provides the main reporter exposure table used by semantic marts

## Downstream Use

- Page 1 overview uses `mart_dashboard_global_trade_overview`, `mart_trade_month_coverage_status`, and `mart_executive_monthly_system_snapshot`.
- Trade drilldown uses `mart_reporter_partner_commodity_month_enriched`.
- Country exposure maps use `mart_reporter_month_exposure_map`.
- Chokepoint maps use `mart_chokepoint_monthly_hotspot_map`.
- Structural vulnerability uses `mart_reporter_structural_vulnerability`.
- Core analytical and audit use cases continue to use the lower-level facts and marts listed above.

## Current Operational Logging

Implemented logs and manifests exist for:

- extract and checkpoint state: `data/metadata/comtrade/state/extraction_registry.jsonl`, `data/metadata/comtrade/state/comtrade_checkpoint.json`, `logs/comtrade/comtrade_history_*.log`
- metadata: `logs/comtrade/comtrade_metadata_*.log`
- silver: `logs/comtrade/comtrade_silver_manifest.jsonl`
- routing: `logs/comtrade/comtrade_routing_*.log`, `logs/comtrade/comtrade_routing_manifest.jsonl`
- GCS publish: `logs/comtrade/publish_comtrade_to_gcs.log`, `logs/comtrade/publish_comtrade_to_gcs_manifest.jsonl`
- BigQuery load: `logs/comtrade/load_comtrade_to_bigquery.log`, `logs/comtrade/load_comtrade_to_bigquery_manifest.jsonl`, `logs/comtrade/load_comtrade_to_bigquery_batches.jsonl`

## Known Gaps

- This is an analytical routing model, not a ground-truth shipping route registry.
- Route applicability status naming still needs careful interpretation across source values and downstream confidence labels.
- The routing script depends on local geospatial support assets for reproducibility and speed.
- Some local workspaces may still contain only compatibility summary snapshots under `data/silver/comtrade/comtrade_fact/*.parquet`; a current VM/cloud BigQuery fact load requires partitioned `comtrade_fact.parquet` slices under the documented `year/month/reporter_iso3/cmd_code/flow_code` layout.
- Comtrade remains the most quota-sensitive and stateful dataset. Preserve checkpoint, registry, and persistent disk semantics.
