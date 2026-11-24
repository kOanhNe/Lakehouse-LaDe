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
from src.eta.dataset import build_eta_samples, read_lade_csv
from src.eta.model import fit_lightgbm
from src.eta.splits import time_boundaries

TARGET = "target_remaining_minutes"
AOI_FEATURES = ["aoi_past_avg_min", "aoi_past_count"]
VARIANTS = {
    "protocol_b": FEATURES,
    "protocol_b_aoi": FEATURES + AOI_FEATURES,
    "protocol_b_aoi_gps": FEATURES + AOI_FEATURES + ["accept_to_stop_km"],
}


def validation_rows(samples: pd.DataFrame, orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use only labels observed before the validation period when fitting."""
    boundaries = time_boundaries(samples, "query_ts")
    delivered = samples.order_id.map(orders.delivery_ts)
    train_mask = samples.split.eq("train") & delivered.lt(boundaries.train_end)
    return samples.loc[train_mask], samples.loc[samples.split.eq("validation")]


def fill_from_train_medians(train: pd.DataFrame, validation: pd.DataFrame,
                            features: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = [*features, TARGET]
    train_rows = train.loc[:, columns].copy()
    validation_rows = validation.loc[:, columns].copy()
    medians = train_rows[features].median()
    train_rows[features] = train_rows[features].fillna(medians)
    validation_rows[features] = validation_rows[features].fillna(medians)
    return train_rows, validation_rows


def validation_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    error = np.abs(actual - np.maximum(predicted, 0))
    return {
        "acc20_pct": 100 * np.mean(error <= 20),
        "acc30_pct": 100 * np.mean(error <= 30),
        "mae_min": error.mean(),
        "p90_abs_error_min": np.quantile(error, 0.9),
    }


def run(source: Path, output: Path, seeds: list[int] | None = None) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    prereg = yaml.safe_load((ROOT / "config" / "prereg.yaml").read_text(encoding="utf-8"))
    config = prereg["data"]
    if seeds is None:
        seeds = prereg["model"]["seeds"]
    with source.open("rb") as csv_file:
        source_hash = hashlib.file_digest(csv_file, "sha256").hexdigest()
    if source_hash != config["sha256"]:
        raise ValueError(f"Dataset SHA-256 mismatch for {source}. Expected {config['sha256']}, got {source_hash}")

    events = read_lade_csv(source)
    orders = events.set_index("order_id", verify_integrity=True)
    samples = build_eta_samples(
        events, "B", config["max_queries_per_courier_day"], config["query_seed"], include_aoi_gps=True,
    )
    train, validation = validation_rows(samples, orders)
    sample_counts = {
        "train_rows": len(train),
        "validation_rows": len(validation),
        "heldout_rows": int(samples.split.eq("test").sum()),
    }
    actual = validation[TARGET].to_numpy()
    median = np.full(len(validation), train[TARGET].median())
    results = [{
        "variant": "median_train", "seed": None, **sample_counts, **validation_metrics(actual, median),
    }]
    for variant, features in VARIANTS.items():
        train_rows, validation_inputs = fill_from_train_medians(train, validation, features)
        for seed in seeds:
            model = fit_lightgbm(train_rows, validation_inputs, features, seed=seed)
            predicted = model.predict(validation_inputs[features])
            results.append({
                "variant": variant, "seed": seed, **sample_counts, **validation_metrics(actual, predicted),
            })

    report = pd.DataFrame(results)
    report["seed"] = report["seed"].astype("Int64")
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output, index=False, float_format="%.8f")
    print(report.round(2).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "reports" / "eta_accept_benchmark_validation.csv")
    parser.add_argument("--seeds", "--seed", type=int, nargs="+", help="Model seeds from prereg.yaml by default")
    args = parser.parse_args()
    run(args.source, args.output, args.seeds)


if __name__ == "__main__":
    main()
