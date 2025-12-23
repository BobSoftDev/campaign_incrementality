from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import streamlit as st


def _project_root_from_this_file(this_file: Path) -> Path:
    # app/data_access.py -> project root is parent of "app"
    return this_file.resolve().parents[1]


@dataclass(frozen=True)
class DataPaths:
    project_root: Path
    marts_dir: Path
    processed_dir: Path
    raw_dir: Path

    @staticmethod
    def default() -> "DataPaths":
        root = _project_root_from_this_file(Path(__file__))
        return DataPaths(
            project_root=root,
            marts_dir=root / "data" / "marts",
            processed_dir=root / "data" / "processed",
            raw_dir=root / "data" / "raw",
        )


def _missing_hint() -> str:
    return (
        "Required data not found. Run the pipeline from the project root:\n"
        "1) python scripts/run_all.py\n"
        "2) streamlit run app/app.py\n"
    )


@st.cache_data(show_spinner=False)
def load_campaign_kpis() -> pd.DataFrame:
    p = DataPaths.default().marts_dir / "mart_kpis_campaign.csv"
    if not p.exists():
        st.error(_missing_hint())
        return pd.DataFrame()
    df = pd.read_csv(p)
    if "start_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_segment_kpis() -> pd.DataFrame:
    p = DataPaths.default().marts_dir / "mart_kpis_segment.csv"
    if not p.exists():
        st.error(_missing_hint())
        return pd.DataFrame()
    return pd.read_csv(p)


@st.cache_data(show_spinner=False)
def load_outcomes_light() -> pd.DataFrame:
    p = DataPaths.default().marts_dir / "mart_campaign_outcomes_light.csv"
    if not p.exists():
        st.error(_missing_hint())
        return pd.DataFrame()
    return pd.read_csv(p)
