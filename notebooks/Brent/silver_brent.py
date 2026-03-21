from pathlib import Path
import pandas as pd


BENCHMARK_MAP = {
    "DCOILBRENTEU": {
        "benchmark_code": "BRENT_EU",
        "benchmark_name": "Brent Crude Europe",
        "region": "europe",
    },
    "DCOILWTICO": {
        "benchmark_code": "WTI_US",
        "benchmark_name": "WTI Crude US",
        "region": "us",
    },
}


def build_brent_silver(
    input_csv: str,
    output_daily_parquet: str,
    output_monthly_parquet: str,
) -> None:
    df = pd.read_csv(input_csv)

    df["trade_date"] = pd.to_datetime(df["date"], errors="coerce")
    df["price_usd_per_bbl"] = pd.to_numeric(df["price_usd"], errors="coerce")
    df["load_ts"] = pd.to_datetime(df["load_ts"], format="%Y%m%dT%H%M%SZ", errors="coerce")

    df = df.rename(columns={"series_id": "source_series_id"})
    df = df[df["trade_date"].notna() & df["price_usd_per_bbl"].notna()].copy()

    mapped = df["source_series_id"].map(BENCHMARK_MAP)
    df["benchmark_code"] = mapped.map(lambda x: x["benchmark_code"] if isinstance(x, dict) else "UNKNOWN")
    df["benchmark_name"] = mapped.map(lambda x: x["benchmark_name"] if isinstance(x, dict) else "Unknown benchmark")
    df["region"] = mapped.map(lambda x: x["region"] if isinstance(x, dict) else None)

    df["year"] = df["trade_date"].dt.year
    df["month"] = df["trade_date"].dt.month
    df["day"] = df["trade_date"].dt.day
    df["year_month"] = df["trade_date"].dt.to_period("M").astype(str)

    daily_cols = [
        "trade_date",
        "year",
        "month",
        "day",
        "year_month",
        "benchmark_code",
        "benchmark_name",
        "region",
        "source_series_id",
        "price_usd_per_bbl",
        "load_ts",
    ]

    daily_df = (
        df[daily_cols]
        .sort_values(["benchmark_code", "trade_date"])
        .drop_duplicates(subset=["benchmark_code", "trade_date"], keep="last")
        .reset_index(drop=True)
    )

    monthly_base = daily_df.sort_values(["benchmark_code", "trade_date"]).copy()

    monthly_df = (
        monthly_base.groupby(["benchmark_code", "benchmark_name", "region", "source_series_id", "year_month"], as_index=False)
        .agg(
            avg_price_usd_per_bbl=("price_usd_per_bbl", "mean"),
            min_price_usd_per_bbl=("price_usd_per_bbl", "min"),
            max_price_usd_per_bbl=("price_usd_per_bbl", "max"),
            trading_day_count=("trade_date", "count"),
            month_start_date=("trade_date", "min"),
        )
    )

    first_prices = (
        monthly_base.groupby(["benchmark_code", "year_month"], as_index=False)
        .first()[["benchmark_code", "year_month", "price_usd_per_bbl"]]
        .rename(columns={"price_usd_per_bbl": "month_start_price_usd_per_bbl"})
    )

    last_prices = (
        monthly_base.groupby(["benchmark_code", "year_month"], as_index=False)
        .last()[["benchmark_code", "year_month", "price_usd_per_bbl"]]
        .rename(columns={"price_usd_per_bbl": "month_end_price_usd_per_bbl"})
    )

    monthly_df = monthly_df.merge(first_prices, on=["benchmark_code", "year_month"], how="left")
    monthly_df = monthly_df.merge(last_prices, on=["benchmark_code", "year_month"], how="left")

    monthly_df["year"] = monthly_df["month_start_date"].dt.year
    monthly_df["month"] = monthly_df["month_start_date"].dt.month
    monthly_df = monthly_df.sort_values(["benchmark_code", "year_month"]).reset_index(drop=True)

    monthly_df["mom_abs_change_usd"] = (
        monthly_df.groupby("benchmark_code")["avg_price_usd_per_bbl"].diff()
    )

    monthly_df["mom_pct_change"] = (
        monthly_df.groupby("benchmark_code")["avg_price_usd_per_bbl"].pct_change()
    )

    Path(output_daily_parquet).parent.mkdir(parents=True, exist_ok=True)
    Path(output_monthly_parquet).parent.mkdir(parents=True, exist_ok=True)

    daily_df.to_parquet(output_daily_parquet, index=False)
    monthly_df.to_parquet(output_monthly_parquet, index=False)

if __name__ == "__main__":
    input_csv = "data/bronze/brent/Batch/brent_crude_20260311T205757Z.csv"
    output_daily_parquet = "data/silver/brent/brent_daily.parquet"
    output_monthly_parquet = "data/silver/brent/brent_monthly.parquet"

    build_brent_silver(
        input_csv=input_csv,
        output_daily_parquet=output_daily_parquet,
        output_monthly_parquet=output_monthly_parquet,
    )