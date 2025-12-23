from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import yaml


def _project_root_from_this_file(this_file: Path) -> Path:
    # scripts/00_generate_data.py -> project root is parent of "scripts"
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
    marts_dir: Path

    @staticmethod
    def from_config(project_root: Path, cfg: dict) -> "Paths":
        out = cfg.get("output", {})
        raw_dir = project_root / out.get("raw_dir", "data/raw")
        processed_dir = project_root / out.get("processed_dir", "data/processed")
        marts_dir = project_root / out.get("marts_dir", "data/marts")
        return Paths(project_root, raw_dir, processed_dir, marts_dir)

    def ensure(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.marts_dir.mkdir(parents=True, exist_ok=True)


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def _make_customers(rng: np.random.Generator, cfg: dict) -> pd.DataFrame:
    n = int(cfg["simulation"]["n_customers"])
    end = pd.Timestamp(cfg["simulation"]["end_date"])

    customer_id = np.arange(1, n + 1)

    tenure_days = rng.integers(30, 900, size=n)
    signup_date = (end - pd.to_timedelta(tenure_days, unit="D")).astype("datetime64[ns]")

    loyalty_tier = rng.choice(["Bronze", "Silver", "Gold", "Platinum"], size=n, p=[0.46, 0.32, 0.18, 0.04])
    region = rng.choice(["North", "South", "East", "West"], size=n, p=[0.27, 0.23, 0.26, 0.24])
    channel_pref = rng.choice(["Email", "SMS", "Push"], size=n, p=[0.62, 0.23, 0.15])

    consent_email = (rng.random(n) < 0.86).astype(int)
    consent_sms = (rng.random(n) < 0.58).astype(int)

    tier_multiplier = pd.Series(loyalty_tier).map({"Bronze": 0.70, "Silver": 0.95, "Gold": 1.25, "Platinum": 1.45}).to_numpy()
    region_multiplier = pd.Series(region).map({"North": 1.05, "South": 0.95, "East": 1.00, "West": 1.02}).to_numpy()
    tenure_multiplier = np.clip(0.85 + (tenure_days / 1000.0), 0.85, 1.35)

    base_rate = 0.012 * tier_multiplier * region_multiplier * tenure_multiplier
    base_rate = np.clip(base_rate + rng.normal(0, 0.002, size=n), 0.002, 0.06)

    recency_score = rng.beta(2.2, 3.5, size=n)
    lifecycle = np.where(recency_score > 0.72, "Active",
                 np.where(recency_score > 0.42, "Warm", "Lapsed"))
    lifecycle = np.where(tenure_days < 90, "New", lifecycle)

    return pd.DataFrame({
        "customer_id": customer_id,
        "signup_date": pd.to_datetime(signup_date),
        "tenure_days": tenure_days,
        "loyalty_tier": loyalty_tier,
        "region": region,
        "channel_pref": channel_pref,
        "consent_email": consent_email,
        "consent_sms": consent_sms,
        "baseline_buy_prob_daily": base_rate,
        "lifecycle": lifecycle,
    })


def _make_campaigns(rng: np.random.Generator, cfg: dict) -> pd.DataFrame:
    n = int(cfg["simulation"]["n_campaigns"])
    start = pd.Timestamp(cfg["simulation"]["start_date"])
    end = pd.Timestamp(cfg["simulation"]["end_date"])

    campaign_starts = pd.to_datetime(
        rng.choice(
            pd.date_range(start + pd.Timedelta(days=5), end - pd.Timedelta(days=25), freq="D"),
            size=n,
            replace=False
        )
    ).sort_values()

    channels = rng.choice(["Email", "SMS", "Push"], size=n, p=[0.6, 0.25, 0.15])
    target_segment = rng.choice(["New", "Active", "Warm", "Lapsed"], size=n, p=[0.18, 0.38, 0.24, 0.20])

    true_rpc_uplift = rng.normal(loc=0.18, scale=0.22, size=n)
    if n >= 6:
        true_rpc_uplift[0] = 0.45
        true_rpc_uplift[1] = 0.25
        true_rpc_uplift[2] = 0.05
        true_rpc_uplift[3] = 0.00
        true_rpc_uplift[4] = -0.12
        true_rpc_uplift[5] = 0.15

    window_days = int(cfg["campaign_design"]["default_attribution_window_days"])
    holdout_pct = float(cfg["campaign_design"]["holdout_pct"])

    return pd.DataFrame({
        "campaign_id": [f"C{str(i+1).zfill(3)}" for i in range(n)],
        "campaign_name": [f"Campaign {i+1}" for i in range(n)],
        "start_date": pd.to_datetime(campaign_starts),
        "end_date": pd.to_datetime(campaign_starts) + pd.Timedelta(days=2),
        "channel": channels,
        "target_segment": target_segment,
        "attribution_window_days": window_days,
        "holdout_pct": holdout_pct,
        "true_rpc_uplift": true_rpc_uplift,  # synthetic-only
    })


def _eligibility_logic(customers: pd.DataFrame, campaigns: pd.DataFrame, rng: np.random.Generator, cfg: dict) -> pd.DataFrame:
    overlap_rate = float(cfg["campaign_design"]["overlap_rate"])
    rows = []

    prop = customers["baseline_buy_prob_daily"].to_numpy()
    prop_norm = (prop - prop.min()) / (prop.max() - prop.min() + 1e-9)

    for _, camp in campaigns.iterrows():
        seg = camp["target_segment"]
        channel = camp["channel"]

        seg_match = (customers["lifecycle"] == seg).to_numpy().astype(float)
        seg_match = np.clip(seg_match + rng.normal(0.10, 0.10, size=len(customers)), 0, 1)

        if channel == "Email":
            consent = customers["consent_email"].to_numpy().astype(float)
        elif channel == "SMS":
            consent = customers["consent_sms"].to_numpy().astype(float)
        else:
            consent = (rng.random(len(customers)) < 0.92).astype(float)

        p = 0.05 + 0.55 * seg_match * consent + 0.30 * prop_norm
        p = np.clip(p, 0.0, 0.85)

        eligible_flag = (rng.random(len(customers)) < p).astype(int)

        if overlap_rate > 0:
            extra = (rng.random(len(customers)) < overlap_rate * 0.05).astype(int)
            eligible_flag = np.maximum(eligible_flag, extra)

        snap_date = pd.Timestamp(camp["start_date"]) - pd.Timedelta(days=1)

        rows.append(pd.DataFrame({
            "campaign_id": camp["campaign_id"],
            "customer_id": customers["customer_id"].to_numpy(),
            "eligible_flag": eligible_flag,
            "eligibility_reason": np.where(eligible_flag == 1, "rules_pass", "rules_fail"),
            "snapshot_date": snap_date,
        }))

    return pd.concat(rows, ignore_index=True)


def _make_exposure(elig: pd.DataFrame, campaigns: pd.DataFrame, rng: np.random.Generator, cfg: dict) -> pd.DataFrame:
    holdout_pct = float(cfg["campaign_design"]["holdout_pct"])
    bounce_rate = float(cfg["campaign_design"]["bounce_rate"])

    exposures = []
    for _, camp in campaigns.iterrows():
        cid = camp["campaign_id"]
        start_ts = pd.Timestamp(camp["start_date"]) + pd.Timedelta(hours=9)

        e = elig[(elig["campaign_id"] == cid) & (elig["eligible_flag"] == 1)][["campaign_id", "customer_id"]].copy()
        if e.empty:
            continue

        n = len(e)
        is_holdout = (rng.random(n) < holdout_pct).astype(int)
        e["control_flag"] = is_holdout

        bounced = (rng.random(n) < bounce_rate).astype(int)
        e["bounce_flag"] = bounced
        e["delivered_flag"] = np.where((e["control_flag"] == 0) & (e["bounce_flag"] == 0), 1, 0)

        jitter_min = rng.integers(0, 120, size=n)
        delivered_ts = start_ts + pd.to_timedelta(jitter_min, unit="m")
        e["delivered_ts"] = pd.to_datetime(
            np.where(e["delivered_flag"] == 1, delivered_ts.astype("datetime64[ns]"), np.datetime64("NaT"))
        )

        e["send_id"] = [f"{cid}_S{str(i+1).zfill(7)}" for i in range(n)]
        exposures.append(e[["campaign_id", "customer_id", "send_id", "delivered_flag", "delivered_ts", "bounce_flag", "control_flag"]])

    return pd.concat(exposures, ignore_index=True)


def _simulate_transactions(
    rng: np.random.Generator,
    cfg: dict,
    customers: pd.DataFrame,
    campaigns: pd.DataFrame,
    exposure: pd.DataFrame,
) -> pd.DataFrame:
    start = pd.Timestamp(cfg["simulation"]["start_date"])
    end = pd.Timestamp(cfg["simulation"]["end_date"])
    days = pd.date_range(start, end, freq="D")
    n_cust = len(customers)

    p_daily = customers["baseline_buy_prob_daily"].to_numpy()

    tier_scale = customers["loyalty_tier"].map({"Bronze": 1.0, "Silver": 1.15, "Gold": 1.35, "Platinum": 1.55}).to_numpy()
    weekday_boost = np.array([1.00, 0.98, 0.99, 1.02, 1.08, 1.15, 1.05])

    txn_rows = []
    txn_id_counter = 1

    delivered = exposure[exposure["delivered_flag"] == 1][["campaign_id", "customer_id", "delivered_ts"]].copy()
    delivered["delivered_date"] = pd.to_datetime(delivered["delivered_ts"]).dt.floor("D")

    camp_uplift_prob: Dict[str, float] = {}
    for _, camp in campaigns.iterrows():
        rpc_uplift = float(camp["true_rpc_uplift"])
        camp_uplift_prob[camp["campaign_id"]] = float(np.clip(rpc_uplift / 50.0, -0.01, 0.02))

    # campaign_id -> {customer_id -> delivered_date}
    delivered_map: Dict[str, Dict[int, pd.Timestamp]] = {}
    for cid, g in delivered.groupby("campaign_id"):
        dd = g.sort_values("delivered_date").drop_duplicates("customer_id")
        delivered_map[cid] = dict(zip(dd["customer_id"].astype(int), dd["delivered_date"]))

    # Iterate day by day (efficient enough for ~25k*90 with vectorization)
    customer_ids = customers["customer_id"].to_numpy().astype(int)

    for d in days:
        wd = int(d.weekday())
        p = p_daily * weekday_boost[wd]

        # Apply incremental probability for customers currently inside a campaign window (delivered only)
        p_adj = p.copy()

        for _, camp in campaigns.iterrows():
            cid = camp["campaign_id"]
            window = int(camp["attribution_window_days"])
            uplift = camp_uplift_prob.get(cid, 0.0)

            if uplift == 0.0:
                continue

            cmap = delivered_map.get(cid)
            if not cmap:
                continue

            delivered_dates = (
                pd.to_datetime(pd.Series(customer_ids).map(cmap), errors="coerce")
                .to_numpy(dtype="datetime64[ns]")
            )

            in_window = (
                (delivered_dates != np.datetime64("NaT"))
                & (d.to_datetime64() >= delivered_dates)
                & (d.to_datetime64() < (delivered_dates + np.timedelta64(window, "D")))
            )

            p_adj = p_adj + uplift * in_window.astype(float)

        p_adj = np.clip(p_adj, 0.0, 0.35)

        buy = (rng.random(n_cust) < p_adj)
        if not buy.any():
            continue

        buyers = customer_ids[buy]
        n_txn = np.where(rng.random(len(buyers)) < 0.12, 2, 1)

        buyer_tier_scale = tier_scale[buy]
        base = rng.lognormal(mean=2.85, sigma=0.55, size=n_txn.sum())
        revenue = base * buyer_tier_scale.repeat(n_txn)

        items = np.clip((revenue / 8.5 + rng.normal(0, 1.0, size=len(revenue))).round().astype(int), 1, 40)

        store_id = rng.integers(1, 120, size=len(revenue))
        channel = rng.choice(["Store", "Online"], size=len(revenue), p=[0.78, 0.22])

        buyer_rep = np.repeat(buyers, n_txn)

        for i in range(len(revenue)):
            txn_rows.append({
                "txn_id": f"T{txn_id_counter:010d}",
                "customer_id": int(buyer_rep[i]),
                "txn_ts": pd.Timestamp(d) + pd.Timedelta(minutes=int(rng.integers(8 * 60, 21 * 60))),
                "store_id": int(store_id[i]),
                "channel": str(channel[i]),
                "gross_revenue": float(round(revenue[i], 2)),
                "items_count": int(items[i]),
            })
            txn_id_counter += 1

    return pd.DataFrame(txn_rows)


def main() -> None:
    project_root = _project_root_from_this_file(Path(__file__))
    cfg = _load_settings(project_root)
    seed = int(cfg["project"]["random_seed"])
    rng = _rng(seed)

    paths = Paths.from_config(project_root, cfg)
    paths.ensure()

    customers = _make_customers(rng, cfg)
    campaigns = _make_campaigns(rng, cfg)
    eligibility = _eligibility_logic(customers, campaigns, rng, cfg)
    exposure = _make_exposure(eligibility, campaigns, rng, cfg)
    transactions = _simulate_transactions(rng, cfg, customers, campaigns, exposure)

    customers.to_csv(paths.raw_dir / "dim_customers.csv", index=False)
    campaigns.to_csv(paths.raw_dir / "dim_campaigns.csv", index=False)
    eligibility.to_csv(paths.raw_dir / "fact_eligibility.csv", index=False)
    exposure.to_csv(paths.raw_dir / "fact_exposure.csv", index=False)
    transactions.to_csv(paths.raw_dir / "fact_transactions.csv", index=False)

    print("✅ Generated raw data:")
    print(f"- {paths.raw_dir / 'dim_customers.csv'}")
    print(f"- {paths.raw_dir / 'dim_campaigns.csv'}")
    print(f"- {paths.raw_dir / 'fact_eligibility.csv'}")
    print(f"- {paths.raw_dir / 'fact_exposure.csv'}")
    print(f"- {paths.raw_dir / 'fact_transactions.csv'}")


if __name__ == "__main__":
    main()
