from __future__ import annotations

import sys
from pathlib import Path
import streamlit as st

# Make imports stable regardless of where Streamlit is launched
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.data_access import load_campaign_kpis  # noqa: E402


st.set_page_config(
    page_title="CRM Campaign Incrementality",
    layout="wide",
)

st.title("CRM Campaign Effectiveness & Incrementality")
st.caption("Exposed vs. Holdout | KPI-first | Decision-first | Portfolio-grade")

df = load_campaign_kpis()
if df.empty:
    st.stop()

# Global filters (used implicitly by pages via Streamlit session state)
st.sidebar.header("Global Filters")
campaigns = ["All"] + df["campaign_id"].dropna().unique().tolist()
sel = st.sidebar.selectbox("Campaign", campaigns, index=0)
st.session_state["selected_campaign"] = None if sel == "All" else sel

channels = ["All"] + sorted(df["channel"].dropna().unique().tolist())
ch = st.sidebar.selectbox("Channel", channels, index=0)
st.session_state["selected_channel"] = None if ch == "All" else ch

st.sidebar.markdown("---")
st.sidebar.write("Pipeline quick start:")
st.sidebar.code("python scripts/run_all.py\nstreamlit run app/app.py", language="bash")

st.markdown(
    """
This app reads **pre-computed KPI marts** and shows only decision-relevant views:
- **Executive Overview:** which campaigns to scale/stop
- **Deep Dive:** why a campaign is incremental or not
- **Segment Analysis:** where uplift is concentrated
- **Customer Drilldown:** validate distribution & outliers
- **Definitions:** formulas, assumptions, limitations
"""
)

st.info("Use the left sidebar to set global filters, then navigate using the Streamlit pages menu.")
