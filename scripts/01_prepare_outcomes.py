from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import numpy as np
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
    raw_dir: Path
    processed_dir: Path

    @staticmethod
    def from_config(project_root: Path, cfg: dict) -> "Paths":
        out = cfg.get("output", {})
        raw_dir = project_root / out.get("raw_dir", "data/raw")
        processed_dir = project_root / out.get("processed_dir", "data/processed")
        return Paths(project_root, raw_dir, processed_dir)

    def ensure(self) -> None:
        self.processed_dir.mkdir(parents=True, exist_ok=True)


def _read_required_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required dataset: {path}")
    return pd.read_csv(path)


def main() -> None:
    project_root = _project_root_from_this_file(Path(__file__))
    cfg = _load_settings(project_root)
    paths = Paths.from_config(project_root, cfg)
    paths.ensure()

    customers = _read_required_csv(paths.raw_dir / "dim_customers.csv")
    campaigns = _read_required_csv(paths.raw_dir / "dim_campaigns.csv")
    elig = _read_required_csv(paths.raw_dir / "fact_eligibility.csv")
    exp = _read_required_csv(paths.raw_dir / "fact_exposure.csv")
    tx = _read_required_csv(paths.raw_dir / "fact_transactions.csv")

    campaigns["start_date"] = pd.to_datetime(campaigns["start_date"])
    campaigns["attribution_window_days"] = campaigns["attribution_window_days"].astype(int)

    exp["delivered_ts"] = pd.to_datetime(exp["delivered_ts"], errors="coerce")
    tx["txn_ts"] = pd.to_datetime(tx["txn_ts"])

    # Join campaign settings onto exposure
    exp = exp.merge(
        campaigns[["campaign_id", "start_date", "attribution_window_days"]],
        on="campaign_id",
        how="left"
    )

    # Anchor:
    # - Exposed: delivered_ts
    # - Holdout: campaign start_date at 09:00
    exp["anchor_ts"] = np.where(
        exp["delivered_flag"].astype(int) == 1,
        exp["delivered_ts"].astype("datetime64[ns]"),
        (exp["start_date"] + pd.Timedelta(hours=9)).astype("datetime64[ns]"),
    )
    exp["anchor_ts"] = pd.to_datetime(exp["anchor_ts"], errors="coerce")

    # Denominator: eligible customers only
    elig_ok = elig[elig["eligible_flag"].astype(int) == 1][["campaign_id", "customer_id"]].drop_duplicates()
    base = elig_ok.merge(exp, on=["campaign_id", "customer_id"], how="left")

    # For any eligible customer missing in exposure file, treat as not-delivered control
    base["delivered_flag"] = base["delivered_flag"].fillna(0).astype(int)
    base["control_flag"] = base["control_flag"].fillna(1).astype(int)
    base["bounce_flag"] = base["bounce_flag"].fillna(0).astype(int)

    base["anchor_ts"] = pd.to_datetime(
        base["anchor_ts"].fillna(base["start_date"] + pd.Timedelta(hours=9)),
        errors="coerce"
    )
    base["window_days"] = base["attribution_window_days"].fillna(
        int(cfg["campaign_design"]["default_attribution_window_days"])
    ).astype(int)

    base["window_start"] = base["anchor_ts"]
    base["window_end"] = base["anchor_ts"] + pd.to_timedelta(base["window_days"], unit="D")

    # Transactions filter
    min_start = base["window_start"].min()
    max_end = base["window_end"].max()
    tx_f = tx[(tx["txn_ts"] >= min_start) & (tx["txn_ts"] < max_end)].copy()

    merged = base[["campaign_id", "customer_id", "window_start", "window_end"]].merge(
        tx_f[["customer_id", "txn_ts", "gross_revenue"]],
        on="customer_id",
        how="left"
    )

    in_win = (merged["txn_ts"] >= merged["window_start"]) & (merged["txn_ts"] < merged["window_end"])
    merged = merged[in_win]

    agg = merged.groupby(["campaign_id", "customer_id"], as_index=False).agg(
        revenue_in_window=("gross_revenue", "sum"),
        txn_count_in_window=("gross_revenue", "size"),
    )

    out = base.merge(agg, on=["campaign_id", "customer_id"], how="left")
    out["revenue_in_window"] = out["revenue_in_window"].fillna(0.0)
    out["txn_count_in_window"] = out["txn_count_in_window"].fillna(0).astype(int)
    out["converted_flag"] = (out["txn_count_in_window"] > 0).astype(int)

    out["exposed_flag"] = (out["delivered_flag"] == 1).astype(int)
    out["holdout_flag"] = ((out["control_flag"] == 1) | (out["delivered_flag"] == 0)).astype(int)

    out = out.merge(
        customers[["customer_id", "loyalty_tier", "lifecycle", "region", "baseline_buy_prob_daily"]],
        on="customer_id",
        how="left"
    )
    out["segment_name"] = out["lifecycle"].astype(str) + " | " + out["loyalty_tier"].astype(str)

    keep = [
        "campaign_id", "customer_id",
        "exposed_flag", "holdout_flag", "delivered_flag", "control_flag", "bounce_flag",
        "anchor_ts", "window_start", "window_end", "window_days",
        "converted_flag", "revenue_in_window", "txn_count_in_window",
        "lifecycle", "loyalty_tier", "region", "baseline_buy_prob_daily", "segment_name"
    ]
    out = out[keep].copy()

    out_path = paths.processed_dir / "mart_campaign_outcomes.csv"
    out.to_csv(out_path, index=False)

    print("✅ Prepared outcomes mart:")
    print(f"- {out_path}")


if __name__ == "__main__":
    main()
