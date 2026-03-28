from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from app.lib.config import EXPORT_MAX_ROWS


def render_csv_download(df: pd.DataFrame, label: str, filename_prefix: str) -> None:
    if df.empty:
        st.caption("No rows available to export for the current selection.")
        return

    export_df = df.head(EXPORT_MAX_ROWS).copy()
    if len(df) > EXPORT_MAX_ROWS:
        st.caption(f"Export truncated to the first {EXPORT_MAX_ROWS:,} rows.")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"
    st.download_button(
        label=label,
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )
