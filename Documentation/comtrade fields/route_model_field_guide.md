# Route model field guide

## Purpose

This document explains the fields produced by the scenario-aware maritime routing setup in `05_comtrade_silver_enrichment_scenario_graph_routing.ipynb`.

It covers:
- what each field means
- how each field is derived
- how to use each field in downstream analytics
- practical query patterns and caveats

---

## 1. Model overview

The route model is **not** a shipment-truth engine. It is a **heuristic maritime exposure model**.

It combines:
- observed trade rows from the Comtrade silver fact table
- `motCode` to decide whether a pair is maritime-eligible
- World Port Index port metadata to infer likely basins and candidate ports
- a basin graph to derive likely chokepoint paths
- optional gateway logic for landlocked countries
- optional transshipment hub hints for long-haul cross-basin flows

The main output is `dim_trade_routes`, keyed by:
- `reporter_iso3`
- `partner_iso3`
- `partner2_iso3`
- `route_scenario`

In practice, this means one route record per reporter-partner-partner2 combination per scenario.

---

## 2. Upstream inputs

The route model is derived from these main inputs.

### 2.1 Silver fact data
Used to determine whether trade between a reporter and partner is maritime-eligible.

Key upstream fields used:
- `analysis_grain`
- `reporter_iso3`
- `partner_iso3`
- `partner2Code` mapped to `partner2_iso3`
- `motCode`
- `trade_value_usd`

### 2.2 `dim_country`
Used for ISO code normalisation and country-level flags.

### 2.3 World Port Index data
Used to infer:
- candidate ports by country
- port basins
- port suitability and score

### 2.4 Basin graph and chokepoint bridges
Used to derive likely chokepoint sequences between inferred basins.

### 2.5 Gateway and transshipment support tables
Used when a country is landlocked or when a long-haul route plausibly passes through a hub.

---

## 3. Supporting output tables

Before covering `dim_trade_routes`, it helps to understand the supporting outputs.

### 3.1 `bridge_country_route_applicability`
This table decides whether a bilateral pair is even eligible for a maritime route.

#### Main fields

##### `reporter_iso3`
The reporting country in the Comtrade fact row.

##### `partner_iso3`
The main partner country in the Comtrade fact row.

##### `partner2_iso3`
A secondary partner or intermediary country, when available.

##### `row_count`
Count of distinct fact-grain rows for this reporter-partner-partner2 combination.

##### `trade_value_usd`
Sum of trade value over all rows in the combination.

##### `has_sea`
Boolean flag. `True` if at least one row has maritime `motCode` evidence.

##### `has_inland_water`
Boolean flag. `True` if inland waterway mode appears.

##### `has_unknown`
Boolean flag. `True` if unknown or aggregate transport modes are present.

##### `has_non_marine`
Boolean flag. `True` if air, road, rail, pipeline, postal, or other non-marine modes are present.

##### `mot_codes_seen`
Pipe-delimited summary of mode codes seen for the bilateral pair.

##### `route_applicability_status`
The route eligibility classification.

Possible values include:
- `MARITIME_ELIGIBLE`
- `INLAND_WATER_ONLY`
- `UNKNOWN_ONLY`
- `NON_MARITIME_ONLY`
- `NO_MODE_EVIDENCE`

#### How it is derived
The notebook groups the silver fact table by:
- `reporter_iso3`
- `partner_iso3`
- `partner2_iso3`

Then it checks which `motCode` values are present.

#### Downstream use
This table is useful for:
- QA on routing scope
- understanding which reporter-partner pairs are routeable by sea
- filtering out non-maritime pairs before route analytics

#### Recommended query pattern
Use this table to check what proportion of your bilateral pairs are truly sea-supported before you analyse chokepoints.

---

### 3.2 `dim_country_ports`
This is the cleaned, ranked list of ports used as candidate ports for each country.

#### Main fields

##### `iso3`
Country ISO3 code linked to the port.

##### `port_name`
Name of the candidate port.

##### `latitude`, `longitude`
Port coordinates.

##### `world_water_body`
Water body description from the World Port Index.

##### `port_basin`
The inferred basin category derived from `world_water_body`.

##### `harbor_size`
Raw WPI harbour size category.

##### `port_score`
A heuristic score based on port size and facilities.

##### `port_rank`
Rank of the port within the country after scoring and basin-aware selection.

##### Facility fields
These are generally binary indicators from WPI, converted into numeric flags:
- `fac_container`
- `fac_solid_bulk`
- `fac_liquid_bulk`
- `fac_oil_terminal`
- `fac_lng_terminal`

#### How it is derived
The notebook:
- loads WPI
- standardises country names to ISO3
- infers a basin from `world_water_body`
- scores ports using harbour size and facility presence
- retains a ranked subset of ports per country
- keeps more ports for multi-basin countries such as Russia, USA, China, France, Spain, Turkey, Canada, and Australia

#### Downstream use
This dimension is mainly for:
- route-building logic
- QA and explainability
- future commodity-aware port selection

It is not usually a front-line dashboard table, but it is very useful for debugging.

---

### 3.3 `dim_port_basin`
This is the basin-level reference for ports.

#### Main fields
Typical fields include:
- `port_name`
- `iso3`
- `port_basin`
- `world_water_body`

#### How it is derived
It is derived from the cleaned port table by keeping the basin inference used in routing.

#### Downstream use
Used for:
- QA of basin assignment
- auditing why a route was classified as Suez-, Gibraltar-, or Hormuz-exposed

---

### 3.4 `dim_chokepoint`
Reference table of chokepoints and coordinates.

#### Main fields
- `chokepoint_name`
- `longitude`
- `latitude`
- optional metadata such as type or region

#### Downstream use
Used to:
- build forced path geometry
- visualise chokepoints on maps
- join route records to a geographic reference

---

### 3.5 `bridge_basin_graph_edges`
This table defines the routing graph between basins.

#### Main fields
- `origin_basin`
- `destination_basin`
- `chokepoint_name`
- `scenario_cost`

#### Meaning
Each row is a directed edge in the basin graph.
The edge may represent:
- open-sea movement
- or a movement that crosses a named chokepoint

#### How it is derived
This is a modelled bridge table, not directly observed from trade.
It encodes routing assumptions for the selected scenario.

#### Downstream use
Used to compute shortest basin paths and their chokepoint sequences.

---

### 3.6 `bridge_port_basin_chokepoints`
This is the audit-friendly version of basin paths used in the route output.

#### Main fields
- `origin_basin`
- `destination_basin`
- `leg_order`
- `chokepoint_name`

#### How it is derived
For each basin pair observed in the generated routes, the notebook computes the shortest basin path and expands its chokepoints into ordered legs.

#### Downstream use
Useful for:
- auditing routing assumptions
- documenting how basin pairs map to chokepoints
- checking whether nearby regional routes are being over-routed

---

### 3.7 `dim_transshipment_hub`
Reference dimension of candidate hub ports.

#### Main fields
- `hub_port`
- `hub_iso3`
- `hub_basin`
- `hub_rank`

#### Downstream use
Used to optionally tag a route with a likely hub when cross-basin long-haul conditions are met.

---

### 3.8 `bridge_basin_default_hubs`
Bridge table suggesting default hubs for origin-basin to destination-basin movements.

#### Main fields
- `origin_basin`
- `destination_basin`
- `hub_basin`
- `hub_rank`

#### Downstream use
Used by the route model to assign a plausible transshipment hub.

---

## 4. Main output: `dim_trade_routes`

This is the primary analytical route dimension.

It contains one inferred route per:
- reporter
- partner
- partner2
- route scenario

Below is the field-level guide.

---

## 5. `dim_trade_routes` field dictionary

### 5.1 Key and identity fields

#### `reporter_iso3`
**Meaning**: ISO3 code of the reporting country.

**Derived from**: silver fact table.

**Downstream use**: one of the main slicing dimensions in trade and chokepoint analytics.

---

#### `partner_iso3`
**Meaning**: ISO3 code of the primary trade partner.

**Derived from**: silver fact table.

**Downstream use**: partner-level route exposure, bilateral trade dependency, route concentration.

---

#### `partner2_iso3`
**Meaning**: optional secondary or intermediary partner ISO3.

**Derived from**: `partner2Code` mapped through country-code lookup.

**Downstream use**:
- advanced analysis of intermediary roles
- future transit or consignment analysis
- currently often null

---

#### `route_scenario`
**Meaning**: routing scenario used when building the route.

Typical values:
- `default_shortest`
- `suez_disrupted`
- `panama_disrupted`
- `cape_preferred`
- `risk_avoidance`

**Derived from**: scenario setting at notebook runtime.

**Downstream use**:
- compare baseline vs rerouted exposure
- support disruption what-if analysis

**Query note**: Always filter or group by `route_scenario`. Otherwise you may mix multiple route assumptions together.

---

### 5.2 Port and gateway fields

#### `reporter_port`
**Meaning**: chosen origin-side port used for routing.

**Derived from**:
- direct country ports if available
- otherwise from a gateway country if sea trade exists but the country is landlocked

**Downstream use**:
- QA
- mapping and route explainability
- future port-level analysis

---

#### `partner_port`
**Meaning**: chosen destination-side port used for routing.

**Derived from**: same logic as `reporter_port` but for the partner country.

**Downstream use**: same as above.

---

#### `reporter_gateway_iso3`
**Meaning**: gateway country used when the reporter has no direct usable domestic port but sea routing is inferred.

**Derived from**: gateway mapping table for landlocked countries.

**Typical value**: null for coastal countries, non-null for landlocked sea-coded trade.

**Downstream use**:
- analyse dependence on third-country maritime access
- separate direct maritime exposure from gateway maritime exposure

---

#### `partner_gateway_iso3`
**Meaning**: gateway country used on the partner side when needed.

**Derived from**: same logic as `reporter_gateway_iso3`.

**Downstream use**: same as above.

---

### 5.3 Basin fields

#### `reporter_basin`
**Meaning**: inferred maritime basin of the chosen reporter-side port.

Examples:
- `BLACK_SEA`
- `MEDITERRANEAN`
- `ATLANTIC`
- `NORTH_ATLANTIC_EUROPE`
- `GULF`
- `INDIAN_OCEAN`
- `WESTERN_PACIFIC`
- `UNKNOWN_COASTAL`

**Derived from**: `infer_port_basin(world_water_body)` applied to the selected port.

**Downstream use**:
- major geography grouping
- route QA
- basin-to-basin analytics
- debugging suspicious chokepoint assignments

---

#### `partner_basin`
**Meaning**: inferred basin of the chosen partner-side port.

**Derived from**: same method as `reporter_basin`.

**Downstream use**: same as above.

---

### 5.4 Distance fields

#### `distance_km`
**Meaning**: great-circle distance between the chosen reporter port and partner port.

**Derived from**: haversine-style geographic distance function.

**Downstream use**:
- QA and plausibility checking
- neighbour-route sanity checks
- route scoring and route confidence logic

**Important**: this is not shipping distance. It is straight-line geographic distance.

---

#### `sea_distance_direct_km`
**Meaning**: direct sea-route distance between the two selected ports, without forcing the path through the derived chokepoint chain.

**Derived from**: `searoute` direct sea path.

**Downstream use**:
- baseline maritime distance
- compare with forced chokepoint path
- identify detour effects

---

#### `sea_distance_forced_km`
**Meaning**: route distance when the path is forced through the chosen chokepoint sequence.

**Derived from**: sea route stitched across chokepoint coordinates and endpoints.

**Downstream use**:
- estimate rerouting burden
- estimate chokepoint-related detour effect
- scenario comparison

**Caveat**: may be null if no chokepoint path is used or if route forcing fails.

---

#### `sea_distance_km`
**Meaning**: the final selected sea distance used in the route record.

**Derived from**:
- `sea_distance_forced_km` if a chokepoint sequence exists and is successfully computed
- otherwise `sea_distance_direct_km`

**Downstream use**:
- headline maritime distance measure in downstream marts
- distance-weighted exposure metrics

---

### 5.5 Chokepoint and route classification fields

#### `main_chokepoint`
**Meaning**: the first named chokepoint in the inferred chokepoint sequence.

**Derived from**: first element of the shortest basin-path chokepoint list.

**Examples**:
- `Turkish Straits`
- `Suez Canal`
- `Hormuz Strait`
- `Panama Canal`
- null for direct or same-basin/open-sea cases

**Downstream use**:
- the most practical field for dashboard grouping
- primary route-risk label
- easy way to compare pre/during/post disruption windows

**Important**: this is the first chokepoint encountered, not necessarily the only one.

---

#### `route_group`
**Meaning**: simplified exposure bucket derived from the chokepoint sequence or open-sea basin logic.

Typical values include:
- `SUEZ_EXPOSED`
- `HORMUZ_EXPOSED`
- `PANAMA_EXPOSED`
- `MALACCA_EXPOSED`
- `GIBRALTAR_EXPOSED`
- `BLACK_SEA_EXIT_EXPOSED`
- `CAPE_REROUTED`
- `EUROPEAN_MARITIME`
- `BLACK_SEA_REGIONAL`
- `DIRECT_OR_OPEN_SEA`
- `UNROUTED`

**Derived from**:
- chokepoint sequence when present
- otherwise a fallback open-sea grouping based on basin pair

**Downstream use**:
- the main categorical dimension for charts
- preferred grouping for route exposure analytics

**Recommendation**: use `route_group` for broad dashboards and `main_chokepoint` for more precise drill-down.

---

#### `route_mode`
**Meaning**: whether the route is direct/open-sea or forced through named chokepoints.

Typical values:
- `direct`
- `forced_chokepoint`
- `unrouted`

**Derived from**:
- whether a chokepoint sequence exists and could be applied

**Downstream use**:
- understand how much of the model is corridor-driven vs same-basin/open-sea
- QA and methodology reporting

---

#### `route_basis`
**Meaning**: pipe-delimited explanation of the logic used to build the route.

Typical components include:
- `SEA_OBSERVED_MOTCODE`
- scenario name such as `default_shortest`
- `DIRECT_PORT`
- `SEA_GATEWAY_INFERRED`
- `OPTIONAL_HUB_INFERRED`

**Derived from**: concatenation of the main logical steps taken in route generation.

**Downstream use**:
- auditability
- filtering high-confidence direct-port routes from more inferred routes
- methodology documentation

**Recommendation**: for production-like gold marts, split this string into flags if you need it often.

---

#### `route_confidence`
**Meaning**: qualitative confidence score for the inferred route.

Typical values:
- `medium`
- `low`
- `very_low`

**Derived from** heuristics such as:
- whether both sides used direct domestic ports
- whether a gateway was required
- whether either basin is `UNKNOWN_COASTAL`
- whether a nearby pair was assigned an implausibly distant chokepoint chain

**Downstream use**:
- filter dashboards to safer rows only
- create high-confidence vs low-confidence comparisons
- support transparent methodology notes

**Recommendation**: default your first dashboard to `route_confidence in ('medium', 'low')` and keep `very_low` for QA.

---

### 5.6 Route applicability and mode evidence

#### `route_applicability_status`
**Meaning**: route eligibility class taken from the country-route applicability bridge.

Possible values:
- `MARITIME_ELIGIBLE`
- `INLAND_WATER_ONLY`
- `UNKNOWN_ONLY`
- `NON_MARITIME_ONLY`
- `NO_MODE_EVIDENCE`

**Derived from**: grouped `motCode` evidence.

**Downstream use**:
- separate routeable sea trade from non-sea or unknown transport trade
- denominator control when calculating maritime exposure shares

**Recommendation**: only use `MARITIME_ELIGIBLE` in maritime chokepoint exposure marts unless you are explicitly studying uncertainty.

---

#### `mot_codes_seen`
**Meaning**: summary string of the transport mode codes observed for the bilateral pair.

**Derived from**: grouped `motCode` values from the fact table.

**Downstream use**:
- QA
- checking whether routes are backed by explicit sea evidence
- useful when debugging odd rows

---

### 5.7 Hub fields

#### `used_transshipment_hub`
**Meaning**: whether the model attached an optional likely transshipment hub to the route.

**Derived from**:
- basin pair hub mapping
- route distance threshold
- cross-basin condition

**Downstream use**:
- network-effect analysis
- future hub-dependency studies
- optional advanced storytelling

**Recommendation**: keep this secondary in MVP dashboards.

---

#### `hub_port`
**Meaning**: hub port name when a transshipment hub was assigned.

**Derived from**: transshipment hub dimension.

**Downstream use**:
- inspect likely transshipment concentration
- future hub-centric visualisations

---

#### `hub_iso3`
**Meaning**: ISO3 of the assigned hub country.

**Derived from**: transshipment hub dimension.

**Downstream use**: hub-country concentration analysis.

---

#### `hub_basin`
**Meaning**: basin of the assigned transshipment hub.

**Derived from**: transshipment hub dimension.

**Downstream use**:
- identify where the network concentrates cargo flows
- compare direct vs hub-mediated basin patterns

---

### 5.8 Geometry field

#### `route_path_coords`
**Meaning**: route geometry coordinates for mapping.

**Derived from**:
- direct sea route geometry when there are no chokepoints
- stitched geometry through chokepoint coordinates when a forced path exists

**Downstream use**:
- route map visualisations
- geospatial debugging

**Recommendation**: do not use this field in routine warehouse marts unless you need a map layer. Keep it out of most BI queries because it is large and verbose.

---

## 6. How the main route fields are derived, step by step

The route generation process is broadly:

1. Read silver fact rows.
2. Group by reporter-partner-partner2.
3. Inspect `motCode` values.
4. Keep only rows that are `MARITIME_ELIGIBLE`.
5. For each country, get candidate ports.
6. If a country has no domestic ports but is landlocked and sea-coded, try gateway inference.
7. For each candidate port pair:
   - compute great-circle distance
   - infer reporter and partner basins
   - find shortest chokepoint path across the basin graph
   - apply neighbour penalties for implausible nearby long-chokepoint chains
   - score candidate pairs
8. Select the best port pair.
9. Optionally assign a hub.
10. Compute direct sea distance.
11. Compute forced chokepoint distance if applicable.
12. Write final route fields and confidence labels.

---

## 7. How to use the model in downstream analytics

### 7.1 Recommended grain for gold marts
Join `dim_trade_routes` back to your trade fact on:
- `reporter_iso3`
- `partner_iso3`
- `partner2_iso3`

and, if relevant,
- `route_scenario`

Then aggregate by your business grain, for example:
- month
- reporter
- commodity
- flow
- route group

### 7.2 Best fields for first-pass analytics
For clean MVP analysis, focus on:
- `route_group`
- `main_chokepoint`
- `route_confidence`
- `route_applicability_status`
- `sea_distance_km`
- `route_scenario`

### 7.3 Example analytical uses

#### Reporter chokepoint dependency
Question: what share of a reporter’s imports are exposed to Suez, Hormuz, Panama, or open sea?

Group by:
- `reporter_iso3`
- `route_group`

Measure:
- `sum(trade_value_usd)`

#### Commodity vulnerability
Question: which commodities are most exposed to Suez or Hormuz?

Group by:
- `cmdCode` or commodity dimension
- `route_group`

Measure:
- `sum(trade_value_usd)`

#### Distance-weighted dependence
Question: how much of trade depends on long-haul maritime routes?

Use:
- `sea_distance_km`
- weighted averages or total exposure-km

#### Scenario comparison
Question: how does route exposure change if Suez is disrupted?

Compare:
- `route_scenario = 'default_shortest'`
- `route_scenario = 'suez_disrupted'`

#### Confidence-aware reporting
Question: which findings remain stable if we exclude low-confidence routes?

Filter:
- `route_confidence = 'medium'`
- or compare `medium` vs `low` vs `very_low`

---

## 8. Recommended query patterns

### 8.1 Safe default filter
For most maritime analysis:
- `route_applicability_status = 'MARITIME_ELIGIBLE'`
- `route_scenario = 'default_shortest'`

### 8.2 Conservative quality filter
For headline dashboard numbers:
- exclude `route_confidence = 'very_low'`

### 8.3 Preserve route scenario in grouped outputs
Never aggregate different route scenarios together unless that is intentional.

### 8.4 Use `route_group` for charts, `main_chokepoint` for drill-down
`route_group` is cleaner and less sparse.
`main_chokepoint` is better for detail pages and QA.

### 8.5 Keep geometry out of normal marts
Do not include `route_path_coords` in wide analytical tables unless you explicitly need map rendering.

---

## 9. Known limitations and interpretation rules

This model should be described honestly.

### What it is good for
- likely chokepoint exposure
- route-risk scenario analysis
- bilateral maritime dependency comparisons
- portfolio storytelling and dashboarding

### What it is not
- AIS vessel tracking
- shipment-truth path reconstruction
- true transshipment schedule modelling
- guaranteed carrier routing choice

### Interpretation rules
- `route_group` means likely exposure, not proof of actual vessel track
- `main_chokepoint` is the first major chokepoint in the inferred path, not the complete story
- gateway and hub fields are inferential and should be used carefully
- very-low-confidence rows should be treated as uncertain

---

## 10. Suggested downstream marts

### `mart_reporter_chokepoint_dependency`
Grain:
- month
- reporter
- route group

Measures:
- trade value
- trade weight
- share of total reporter trade

### `mart_commodity_route_vulnerability`
Grain:
- month
- reporter
- commodity
- route group

Measures:
- trade value
- route share
- average sea distance

### `mart_route_scenario_comparison`
Grain:
- month
- reporter
- commodity
- route scenario
- route group

Measures:
- trade value
- delta vs baseline scenario

### `mart_gateway_dependence`
Grain:
- month
- reporter
- gateway country

Measures:
- trade value routed via inferred gateway

### `mart_hub_dependence`
Grain:
- month
- hub country or hub basin

Measures:
- trade value with `used_transshipment_hub = true`

---

## 11. Example SQL patterns

### 11.1 Basic chokepoint exposure by reporter
```sql
select
    f.reporter_iso3,
    r.route_group,
    sum(f.trade_value_usd) as trade_value_usd
from fact_comtrade f
join dim_trade_routes r
  on f.reporter_iso3 = r.reporter_iso3
 and f.partner_iso3 = r.partner_iso3
 and coalesce(f.partner2_iso3, 'NULL') = coalesce(r.partner2_iso3, 'NULL')
where r.route_scenario = 'default_shortest'
  and r.route_applicability_status = 'MARITIME_ELIGIBLE'
  and r.route_confidence <> 'very_low'
group by 1, 2;
```

### 11.2 Commodity exposure to Suez
```sql
select
    f.cmdCode,
    sum(f.trade_value_usd) as trade_value_usd
from fact_comtrade f
join dim_trade_routes r
  on f.reporter_iso3 = r.reporter_iso3
 and f.partner_iso3 = r.partner_iso3
 and coalesce(f.partner2_iso3, 'NULL') = coalesce(r.partner2_iso3, 'NULL')
where r.route_group = 'SUEZ_EXPOSED'
  and r.route_scenario = 'default_shortest'
group by 1;
```

### 11.3 Scenario comparison
```sql
select
    r.route_scenario,
    r.route_group,
    sum(f.trade_value_usd) as trade_value_usd
from fact_comtrade f
join dim_trade_routes r
  on f.reporter_iso3 = r.reporter_iso3
 and f.partner_iso3 = r.partner_iso3
 and coalesce(f.partner2_iso3, 'NULL') = coalesce(r.partner2_iso3, 'NULL')
group by 1, 2;
```

---

## 12. Final recommendation

For your first dashboard and first gold marts, keep the model usage disciplined.

Use these fields first:
- `route_group`
- `main_chokepoint`
- `route_confidence`
- `route_applicability_status`
- `sea_distance_km`
- `route_scenario`

Treat these as secondary or QA-oriented:
- `reporter_port`
- `partner_port`
- gateway fields
- hub fields
- `route_path_coords`
- `mot_codes_seen`

That gives you a route model that is sophisticated enough to impress, but still structured enough to query and explain.
