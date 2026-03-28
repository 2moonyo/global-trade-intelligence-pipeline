from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.ui import apply_base_style, configure_page, render_app_shell


configure_page()
apply_base_style()
render_app_shell()

base_dir = Path(__file__).resolve().parent
navigation = st.navigation(
    [
        st.Page(str(base_dir / "pages" / "1_Executive_Overview.py"), title="Executive Overview", default=True),
        st.Page(str(base_dir / "pages" / "2_Trade_Dependence.py"), title="Trade Dependence"),
        st.Page(str(base_dir / "pages" / "3_Chokepoint_Stress_Exposure.py"), title="Chokepoint Stress & Exposure"),
        st.Page(str(base_dir / "pages" / "4_Events_Commodity_Impact.py"), title="Events & Commodity Impact"),
        st.Page(str(base_dir / "pages" / "5_Energy_Vulnerability_Context.py"), title="Energy Vulnerability Context"),
    ]
)
navigation.run()
