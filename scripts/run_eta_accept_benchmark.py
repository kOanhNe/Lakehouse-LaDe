"""Validate accept-time ETA features on the shared Protocol B query set."""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_eta_experiment import FEATURES
from src.eta.dataset import _haversine, build_eta_samples, read_lade_csv
from src.eta.model import fit_lightgbm
from src.eta.splits import time_boundaries

TARGET = "target_remaining_minutes"
AOI_FEATURES = ["aoi_past_avg_min", "aoi_past_count"]
VARIANTS = {
    "protocol_b": FEATURES,
    "protocol_b_aoi": FEATURES + AOI_FEATURES,
    "protocol_b_aoi_gps": FEATURES + AOI_FEATURES + ["accept_to_stop_km"],
}


def accept_time_features(orders: pd.DataFrame) -> pd.DataFrame:
    """Build extra features indexed by order ID using timestamps at acceptance."""
    eligible = orders.loc[orders.delivery_minutes.gt(0) & orders.accept_ts.notna() & orders.delivery_ts.notna()]
    features = pd.DataFrame({"aoi_past_count": 0, "aoi_past_avg_min": np.nan}, index=orders.index)
    for _, group in eligible.groupby("aoi_id", sort=False):
        deliveries = group.sort_values("delivery_ts", kind="stable")
        counts = np.searchsorted(deliveries.delivery_ts.to_numpy(), group.accept_ts.to_numpy(), side="right")
        totals = np.r_[0., deliveries.delivery_minutes.to_numpy(dtype=float).cumsum()]
        averages = np.divide(totals[counts], counts, out=np.full(len(counts), np.nan), where=counts > 0)
        features.loc[group.index, "aoi_past_count"] = counts
        features.loc[group.index, "aoi_past_avg_min"] = averages

    valid_gps = (
        orders.accept_gps_ts.le(orders.accept_ts)
        & orders.accept_gps_lat.between(-90, 90)
        & orders.accept_gps_lng.between(-180, 180)
        & ~(orders.accept_gps_lat.eq(0) & orders.accept_gps_lng.eq(0))
        & orders.lat.between(-90, 90)
        & orders.lng.between(-180, 180)
        & ~(orders.lat.eq(0) & orders.lng.eq(0))
    )
    features["accept_to_stop_km"] = _haversine(
        orders.accept_gps_lat, orders.accept_gps_lng, orders.lat, orders.lng,
    ).where(valid_gps)
    return features


def validation_rows(samples: pd.DataFrame, orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use only labels observed before the validation period when fitting."""
    boundaries = time_boundaries(samples, "query_ts")
    delivered = samples.order_id.map(orders.delivery_ts)
    train_mask = samples.split.eq("train") & delivered.lt(boundaries.train_end)
    return samples.loc[train_mask], samples.loc[samples.split.eq("validation")]


def run(source: Path, output: Path, seed: int = 42) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    config = yaml.safe_load((ROOT / "config" / "prereg.yaml").read_text(encoding="utf-8"))["data"]
    with source.open("rb") as csv_file:
        source_hash = hashlib.file_digest(csv_file, "sha256").hexdigest()
    if source_hash != config["sha256"]:
        raise ValueError(f"Dataset SHA-256 mismatch for {source}. Expected {config['sha256']}, got {source_hash}")

    events = read_lade_csv(source)
    orders = events.set_index("order_id", verify_integrity=True)
    samples = build_eta_samples(events, "B", config["max_queries_per_courier_day"], config["query_seed"])
    samples = samples.join(accept_time_features(orders), on="order_id", validate="one_to_one")
    train, validation = validation_rows(samples, orders)
    heldout_rows = int(samples.split.eq("test").sum())
    results = []
    for variant, features in VARIANTS.items():
        train_rows = train.loc[:, [*features, TARGET]].copy()
        validation_inputs = validation.loc[:, [*features, TARGET]].copy()
        medians = train_rows[features].median()
        train_rows[features] = train_rows[features].fillna(medians)
        validation_inputs[features] = validation_inputs[features].fillna(medians)
        model = fit_lightgbm(train_rows, validation_inputs, features, seed=seed)
        predicted = np.maximum(model.predict(validation_inputs[features]), 0)
        error = np.abs(validation_inputs[TARGET].to_numpy() - predicted)
        results.append({
            "variant": variant,
            "seed": seed,
            "train_rows": len(train),
            "validation_rows": len(validation),
            "heldout_rows": heldout_rows,
            "acc20_pct": 100 * np.mean(error <= 20),
            "acc30_pct": 100 * np.mean(error <= 30),
            "mae_min": error.mean(),
            "p90_abs_error_min": np.quantile(error, 0.9),
        })

    report = pd.DataFrame(results)
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output, index=False, float_format="%.8f")
    print(report.round(2).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "reports" / "eta_accept_benchmark_validation.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    run(args.source, args.output, args.seed)


if __name__ == "__main__":
    main()
