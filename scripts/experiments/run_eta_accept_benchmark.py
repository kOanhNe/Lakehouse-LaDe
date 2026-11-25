"""Compare accept-time ETA on the shared Protocol B query set."""
from __future__ import annotations

import argparse
import hashlib
from math import ceil
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from lakehouse import PROJECT_ROOT as ROOT

from scripts.experiments.run_eta_experiment import FEATURES
from eta.dataset import build_eta_samples, read_lade_csv
from eta.model import fit_lightgbm
from eta.splits import time_boundaries

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


def load_benchmark_samples(source: Path) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    prereg = yaml.safe_load((ROOT / "config" / "prereg.yaml").read_text(encoding="utf-8"))
    config = prereg["data"]
    with source.open("rb") as csv_file:
        source_hash = hashlib.file_digest(csv_file, "sha256").hexdigest()
    if source_hash != config["sha256"]:
        raise ValueError(f"Dataset SHA-256 mismatch for {source}. Expected {config['sha256']}, got {source_hash}")

    events = read_lade_csv(source)
    orders = events.set_index("order_id", verify_integrity=True)
    samples = build_eta_samples(
        events, "B", config["max_queries_per_courier_day"], config["query_seed"], include_aoi_gps=True,
    )
    return prereg, orders, samples


def write_report(rows: list[dict], output: Path) -> None:
    report = pd.DataFrame(rows)
    report["seed"] = report["seed"].astype("Int64")
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output, index=False, float_format="%.8f")
    print(report.round(2).to_string(index=False))


def run(source: Path, output: Path, seeds: list[int] | None = None) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    prereg, orders, samples = load_benchmark_samples(source)
    if seeds is None:
        seeds = prereg["model"]["seeds"]
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

    write_report(results, output)


def region_hour_median(train: pd.DataFrame, scored: pd.DataFrame) -> np.ndarray:
    """Median by region and four-hour bucket, with a pooled train fallback."""
    group = ["candidate_region_id", "hour_bucket"]
    train_groups = train.assign(hour_bucket=train.accept_hour.floordiv(4))
    stats = train_groups.groupby(group)[TARGET].agg(["median", "size"])
    medians = stats.loc[stats["size"].ge(30), "median"].rename("region_hour_median")
    scored_groups = scored.assign(hour_bucket=scored.accept_hour.floordiv(4))
    return scored_groups.join(medians, on=group)["region_hour_median"].fillna(train[TARGET].median()).to_numpy()


def holdout_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    residual = actual - predicted
    metrics = validation_metrics(actual, predicted)
    metrics["acc60_pct"] = 100 * np.mean(np.abs(residual) <= 60)
    metrics["underestimate_gt60_pct"] = 100 * np.mean(residual > 60)
    return metrics


def candidate_metrics(actual: np.ndarray, predicted: np.ndarray,
                      validation_residual: np.ndarray) -> dict[str, float]:
    lower_offset, upper_offset = np.quantile(validation_residual, [0.10, 0.90])
    lower = np.maximum(0, predicted + lower_offset)
    upper = np.maximum(0, predicted + upper_offset)

    slow = actual > 180
    flagged = np.argsort(-predicted, kind="stable")[:ceil(0.10 * len(actual))]
    return {
        "interval_coverage_pct": 100 * np.mean((actual >= lower) & (actual <= upper)),
        "interval_median_width_min": np.median(upper - lower),
        "over_180_prevalence_pct": 100 * np.mean(slow),
        "flagged_rows": len(flagged),
        "flagged_over_180_pct": 100 * np.mean(slow[flagged]),
        "over_180_recall_pct": 100 * np.sum(slow[flagged]) / max(1, np.sum(slow)),
    }


def run_holdout(source: Path, output: Path) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    prereg, orders, samples = load_benchmark_samples(source)
    train, validation = validation_rows(samples, orders)
    test_start = time_boundaries(samples, "query_ts").validation_end
    validation = validation.loc[validation.order_id.map(orders.delivery_ts).lt(test_start)]
    test = samples.loc[samples.split.eq("test")].sort_values(["query_ts", "order_id"])
    sample_counts = {
        "train_rows": len(train), "validation_rows": len(validation), "test_rows": len(test),
    }
    print(f"Fit/validation/test rows {sample_counts}, holdout from {test_start.date()}", flush=True)
    actual = test[TARGET].to_numpy()
    results = []
    for variant, predicted in (
        ("median_train", np.full(len(test), train[TARGET].median())),
        ("median_region_hour", region_hour_median(train, test)),
    ):
        results.append({"variant": variant, "seed": None, **sample_counts, **holdout_metrics(actual, predicted)})

    for variant in ("protocol_b", "protocol_b_aoi_gps"):
        features = VARIANTS[variant]
        train_inputs, validation_inputs = fill_from_train_medians(train, validation, features)
        test_inputs = test[features].fillna(train[features].median())
        for seed in prereg["model"]["seeds"]:
            model = fit_lightgbm(train_inputs, validation_inputs, features, seed=seed)
            predicted = np.maximum(model.predict(test_inputs), 0)
            row = {"variant": variant, "seed": seed, **sample_counts, **holdout_metrics(actual, predicted)}
            if variant == "protocol_b_aoi_gps":
                validation_prediction = np.maximum(model.predict(validation_inputs[features]), 0)
                residual = validation_inputs[TARGET].to_numpy() - validation_prediction
                row.update(candidate_metrics(actual, predicted, residual))
            results.append(row)
            print(f"Scored {variant}, seed {seed}", flush=True)

    write_report(results, output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--seeds", "--seed", type=int, nargs="+", help="Model seeds from prereg.yaml by default")
    parser.add_argument("--holdout", action="store_true", help="Evaluate the frozen validation-selected candidate")
    args = parser.parse_args()
    if args.holdout:
        if args.seeds is not None:
            parser.error("--holdout uses the frozen seeds from prereg.yaml")
        output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_holdout.csv"
        run_holdout(args.source, output)
    else:
        output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_validation.csv"
        run(args.source, output, args.seeds)


if __name__ == "__main__":
    main()
