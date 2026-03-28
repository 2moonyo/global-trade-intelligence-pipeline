from __future__ import annotations

import pandas as pd


def scope_caption(filters: dict[str, object]) -> str:
    flow = filters.get("trade_flow", "All trade")
    start_period = filters.get("start_period") or "earliest available month"
    end_period = filters.get("end_period") or "latest available month"
    return f"Scope: {flow} from {start_period} to {end_period}."


def overview_subtitle() -> str:
    return "Trade scale remains concentrated, and chokepoint-linked risk is uneven across countries."


def dependence_subtitle() -> str:
    return "Scale becomes dependence when a small number of bilateral corridors and commodities dominate the selected trade."


def chokepoint_subtitle() -> str:
    return "Operational chokepoint stress and trade exposure do not fall evenly across countries or commodities."


def events_subtitle() -> str:
    return "Event windows help show whether shifts in commodity trade and maritime traffic moved together during disruption."


def energy_subtitle() -> str:
    return "Structural energy dependence adds context to trade exposure by showing which countries are intrinsically more fragile."


def overview_interpretation(top_reporters_df: pd.DataFrame) -> str:
    if top_reporters_df.empty:
        return "The selected scope does not return any trade rows."
    leader = top_reporters_df.iloc[0]
    return f"{leader['reporter_country_name']} leads the selected scope by trade value."


def dependence_interpretation(corridors_df: pd.DataFrame) -> str:
    if corridors_df.empty:
        return "No bilateral corridors matched the current filters."
    leader = corridors_df.iloc[0]
    return f"{leader['reporter_country_name']} and {leader['partner_country_name']} form the largest corridor in the current scope."


def chokepoint_interpretation(exposure_df: pd.DataFrame) -> str:
    if exposure_df.empty:
        return "No reporter-level chokepoint exposure points matched the current filters."
    leader = exposure_df.iloc[0]
    return f"{leader['reporter_country_name']} shows the highest latest mapped exposure in the filtered window."


def event_interpretation(event_name: str, affected_chokepoints: int | None) -> str:
    if affected_chokepoints is not None and not pd.isna(affected_chokepoints) and int(affected_chokepoints) > 0:
        return f"{event_name} connects directly to {int(affected_chokepoints)} mapped chokepoint(s) in the event bridge."
    return f"{event_name} has event metadata, but its chokepoint linkage is limited in the bridge tables."


def energy_interpretation(ranking_df: pd.DataFrame, indicator_label: str) -> str:
    if ranking_df.empty:
        return f"No reporters have non-null values for {indicator_label.lower()} in the current scope."
    leader = ranking_df.iloc[0]
    return f"{leader['reporter_country_name']} ranks highest on {indicator_label.lower()} in the latest usable year."
