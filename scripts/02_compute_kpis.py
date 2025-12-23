from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import yaml


def _project_root_from_this_file(this_file: Path) -> Path:
    return this_file.resolve().parents[1]


def _load_settings(project_root: Path) -> dict:
    cfg_path = project_root / "config" / "settings.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config file: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass(frozen=True)
class Paths:
    project_root: Path
    processed_dir: Path
    marts_dir: Path
    raw_dir: Path

    @staticmethod
    def from_config(project_root: Path, cfg: dict) -> "Paths":
        out = cfg.get("output", {})
        processed_dir = project_root / out.get("processed_dir", "data/processed")
        marts_dir = project_root / out.get("marts_dir", "data/marts")
        raw_dir = project_root / out.get("raw_dir", "data/raw")
        return Paths(project_root, processed_dir, marts_dir, raw_dir)

    def ensure(self) -> None:
        self.marts_dir.mkdir(parents=True, exist_ok=True)


def _read_required_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required dataset: {path}")
    return pd.read_csv(path)


def _kpi_block(df: pd.DataFrame) -> pd.Series:
    n = len(df)
    conv = int(df["converted_flag"].sum()) if n > 0 else 0
    rev = float(df["revenue_in_window"].sum()) if n > 0 else 0.0

    cr = (conv / n) if n > 0 else 0.0
    rpc = (rev / n) if n > 0 else 0.0

    return pd.Series({
        "n_customers": int(n),
        "converters": int(conv),
        "revenue": float(rev),
        "CR": float(cr),
        "RPC": float(rpc),
    })


def main() -> None:
    project_root = _project_root_from_this_file(Path(__file__))
    cfg = _load_settings(project_root)
    paths = Paths.from_config(project_root, cfg)
    paths.ensure()

    outcomes = _read_required_csv(paths.processed_dir / "mart_campaign_outcomes.csv")
    campaigns = _read_required_csv(paths.raw_dir / "dim_campaigns.csv")

    min_group = int(cfg["governance"]["min_group_size"])

    def compute_campaign_kpis(g: pd.DataFrame) -> pd.Series:
        exposed = g[g["exposed_flag"] == 1]
        holdout = g[g["holdout_flag"] == 1]

        e = _kpi_block(exposed).add_prefix("exposed_")
        h = _kpi_block(holdout).add_prefix("holdout_")

        cr_uplift = float(e["exposed_CR"] - h["holdout_CR"])
        rpc_uplift = float(e["exposed_RPC"] - h["holdout_RPC"])
        incr_rev = float(rpc_uplift * e["exposed_n_customers"])

        insufficient = (e["exposed_n_customers"] < min_group) or (h["holdout_n_customers"] < min_group)
        leakage_rate = 0.0  # placeholder (synthetic mode)

        return pd.concat([e, h, pd.Series({
            "CR_uplift": cr_uplift,
            "RPC_uplift": rpc_uplift,
            "incremental_revenue": incr_rev,
            "insufficient_sample_flag": int(insufficient),
            "leakage_rate": float(leakage_rate),
        })])

    camp_kpis = outcomes.groupby("campaign_id", as_index=False).apply(compute_campaign_kpis)
    camp_kpis = camp_kpis.reset_index(drop=True)

    campaigns["start_date"] = pd.to_datetime(campaigns["start_date"], errors="coerce")
    camp_kpis = camp_kpis.merge(
        campaigns[["campaign_id", "campaign_name", "start_date", "channel", "target_segment", "attribution_window_days"]],
        on="campaign_id",
        how="left"
    )

    def compute_segment_kpis(g: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for seg, s in g.groupby("segment_name"):
            exposed = s[s["exposed_flag"] == 1]
            holdout = s[s["holdout_flag"] == 1]

            e = _kpi_block(exposed).add_prefix("exposed_")
            h = _kpi_block(holdout).add_prefix("holdout_")

            cr_uplift = float(e["exposed_CR"] - h["holdout_CR"])
            rpc_uplift = float(e["exposed_RPC"] - h["holdout_RPC"])
            incr_rev = float(rpc_uplift * e["exposed_n_customers"])

            insufficient = (e["exposed_n_customers"] < min_group) or (h["holdout_n_customers"] < min_group)

            rows.append({
                "campaign_id": g["campaign_id"].iloc[0],
                "segment_name": seg,
                **e.to_dict(),
                **h.to_dict(),
                "CR_uplift": cr_uplift,
                "RPC_uplift": rpc_uplift,
                "incremental_revenue": incr_rev,
                "insufficient_sample_flag": int(insufficient),
            })
        return pd.DataFrame(rows)

    seg_kpis = outcomes.groupby("campaign_id", as_index=False).apply(compute_segment_kpis)
    seg_kpis = seg_kpis.reset_index(drop=True)

    camp_path = paths.marts_dir / "mart_kpis_campaign.csv"
    seg_path = paths.marts_dir / "mart_kpis_segment.csv"
    outcomes_path = paths.marts_dir / "mart_campaign_outcomes_light.csv"

    camp_kpis.to_csv(camp_path, index=False)
    seg_kpis.to_csv(seg_path, index=False)

    keep = [
        "campaign_id", "customer_id",
        "exposed_flag", "holdout_flag",
        "converted_flag", "revenue_in_window",
        "segment_name", "lifecycle", "loyalty_tier", "region",
        "baseline_buy_prob_daily"
    ]
    outcomes[keep].to_csv(outcomes_path, index=False)

    print("✅ KPI marts written:")
    print(f"- {camp_path}")
    print(f"- {seg_path}")
    print(f"- {outcomes_path}")


if __name__ == "__main__":
    main()
