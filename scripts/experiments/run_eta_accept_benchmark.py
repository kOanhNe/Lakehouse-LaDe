"""Compare accept-time ETA on the shared Protocol B query set."""
from __future__ import annotations

import argparse
import hashlib
from datetime import timedelta
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from lakehouse import PROJECT_ROOT as ROOT

from eta.dataset import (
    OBSERVED_ACCEPT_VARIANTS, QUEUE_HISTORY_VARIANT, REGISTERED_ACCEPT_VARIANTS as VARIANTS, TARGET,
    attach_observed_context, build_protocol_b_samples, labels_available_before,
    load_accept_prereg, read_lade_csv, rolling_origin_rows, validation_rows,
)
from eta.metrics import accept_eta_metrics, paired_cluster_bootstrap
from eta.model import fit_accept_eta, spark_accept_eta_predictions
from eta.splits import time_boundaries

REFERENCE_VARIANT = "protocol_b_aoi_gps"


def validation_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    error = np.abs(actual - np.maximum(predicted, 0))
    return {
        "acc20_pct": 100 * np.mean(error <= 20),
        "acc30_pct": 100 * np.mean(error <= 30),
        "mae_min": error.mean(),
        "p90_abs_error_min": np.quantile(error, 0.9),
    }


def load_benchmark_samples(source: Path) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    prereg = load_accept_prereg()
    config = prereg["data"]
    with source.open("rb") as csv_file:
        source_hash = hashlib.file_digest(csv_file, "sha256").hexdigest()
    if source_hash != config["sha256"]:
        raise ValueError(f"Dataset SHA-256 mismatch for {source}. Expected {config['sha256']}, got {source_hash}")

    events = read_lade_csv(source)
    orders = events.set_index("order_id", verify_integrity=True)
    samples = build_protocol_b_samples(events, config)
    return prereg, orders, samples


def write_report(rows: list[dict], output: Path) -> None:
    report = pd.DataFrame(rows)
    report["seed"] = report["seed"].astype("Int64")
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output, index=False, float_format="%.8f")
    summary = report.loc[report.scope.eq("pooled")] if "scope" in report else report
    print(summary.dropna(axis="columns", how="all").round(2).to_string(index=False))


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
        for seed in seeds:
            model = fit_accept_eta(train, validation, features, seed=seed)
            predicted = model.predict(validation)
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
    validation = labels_available_before(validation, orders, test_start)
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
        for seed in prereg["model"]["seeds"]:
            model = fit_accept_eta(train, validation, features, seed=seed)
            predicted = model.predict(test)
            row = {"variant": variant, "seed": seed, **sample_counts, **holdout_metrics(actual, predicted)}
            if variant == "protocol_b_aoi_gps":
                residual = validation[TARGET].to_numpy() - model.predict(validation)
                row.update(candidate_metrics(actual, predicted, residual))
            results.append(row)
            print(f"Scored {variant}, seed {seed}", flush=True)

    write_report(results, output)


def rolling_metrics(actual, predicted) -> dict:
    metrics = accept_eta_metrics(actual, predicted)
    metrics.pop("rmse")
    return metrics


def run_rolling_origin(source: Path, output: Path, seeds: list[int] | None = None, *, spark=None) -> None:
    """Score only development query dates, on the same orders for every variant."""
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    prereg, orders, registered = load_benchmark_samples(source)
    seeds = seeds or prereg["model"]["seeds"]
    observed = attach_observed_context(registered, orders.reset_index())
    spark_variants = ("spark_lr", "spark_gbt_absolute", "spark_gbt_calibrated")
    if spark is None:
        reference = REFERENCE_VARIANT
        variants = {reference: VARIANTS[reference], **OBSERVED_ACCEPT_VARIANTS}
    else:
        reference = QUEUE_HISTORY_VARIANT
        feature_set = OBSERVED_ACCEPT_VARIANTS[reference]
        variants = dict.fromkeys((reference, *spark_variants), feature_set)
    predictions = {
        ("median_region_hour", None): [],
        **{(variant, seed): [] for variant in variants for seed in seeds},
    }
    score_windows = []
    results = []
    for fold, (train, calibration, scored) in enumerate(rolling_origin_rows(registered, orders), 1):
        actual = scored[TARGET].to_numpy()
        start = scored.query_ts.min().date()
        end = scored.query_ts.max().date() + timedelta(days=1)
        scope = {
            "scope": "window", "group": str(fold), "score_start": start,
            "score_end_exclusive": end, "train_rows": len(train), "calibration_rows": len(calibration),
        }
        score_windows.append(observed.loc[scored.index])
        print(f"Window {fold} [{start}, {end}) | fit={len(train)} "
              f"calibration={len(calibration)} score={len(scored)}", flush=True)
        for (variant, seed), previous in predictions.items():
            diagnostics = {}
            if seed is None:
                predicted = region_hour_median(train, scored)
            elif variant in spark_variants:
                predicted = spark_accept_eta_predictions(
                    spark, observed.loc[train.index], observed.loc[calibration.index],
                    observed.loc[scored.index], variants[variant], variant, seed,
                    diagnostics=diagnostics,
                )
            else:
                frame = registered if variant == REFERENCE_VARIANT else observed
                model = fit_accept_eta(frame.loc[train.index], frame.loc[calibration.index], variants[variant], seed)
                predicted = model.predict(frame.loc[scored.index])
            previous.append(predicted)
            metrics = rolling_metrics(actual, predicted)
            results.append({"variant": variant, "seed": seed, **scope, **metrics, **diagnostics})
            tree_count = f" trees={diagnostics['gbt_trees']}" if diagnostics else ""
            print(f"  {variant} seed={seed} MAE={metrics['mae']:.2f}{tree_count}", flush=True)

    scored = pd.concat(score_windows, verify_integrity=True)
    actual = scored[TARGET].to_numpy()
    cluster = scored.courier_id.astype(str) + "|" + scored.courier_day
    slices = {
        "batch_size": pd.cut(scored.batch_size, [0, 1, 3, np.inf], labels=["1", "2-3", "4+"]),
        "accept_hour_4h": scored.accept_hour.floordiv(4).mul(4).astype(int),
        "region": scored.candidate_region_id,
        "duration": np.where(actual <= 1440, "<=1440", ">1440"),
    }
    slice_indices = {
        scope: scored.groupby(labels, observed=True, sort=True).indices for scope, labels in slices.items()
    }
    for (variant, seed), windows in predictions.items():
        predicted = np.concatenate(windows)
        row = {"variant": variant, "seed": seed, "scope": "pooled", "group": "all",
               **rolling_metrics(actual, predicted)}
        if seed is not None and variant != reference and cluster.nunique() >= 2:
            paired = pd.DataFrame({
                TARGET: actual, "candidate": predicted,
                "reference": np.concatenate(predictions[(reference, seed)]),
                "courier_day": cluster.to_numpy(),
            })
            cfg = prereg["bootstrap"]
            bootstrap = paired_cluster_bootstrap(
                paired, TARGET, "candidate", "reference", "courier_day", cfg["n_resamples"], cfg["seed"],
            )
            bootstrap["bootstrap_seed"] = bootstrap.pop("seed")
            row.update(bootstrap)
            row["reference_variant"] = reference
        results.append(row)
        for scope, groups in slice_indices.items():
            for label, positions in groups.items():
                results.append({
                    "variant": variant, "seed": seed, "scope": scope, "group": str(label),
                    **rolling_metrics(actual[positions], predicted[positions]),
                })

    write_report(results, output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--seeds", "--seed", type=int, nargs="+", help="Model seeds from prereg.yaml by default")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--holdout", action="store_true", help="Evaluate the frozen validation-selected candidate")
    mode.add_argument("--rolling-origin", action="store_true", help="Compare features on development dates only")
    parser.add_argument("--spark", action="store_true", help="Compare Spark LR/GBT with LightGBM on rolling-origin rows")
    args = parser.parse_args()
    if args.spark and not args.rolling_origin:
        parser.error("--spark uses the --rolling-origin evaluation")
    if args.holdout:
        if args.seeds is not None:
            parser.error("--holdout uses the frozen seeds from prereg.yaml")
        output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_holdout.csv"
        run_holdout(args.source, output)
    elif args.rolling_origin:
        if args.spark:
            from pyspark.sql import SparkSession

            spark = (SparkSession.builder.master("local[2]").appName("ETA rolling-origin")
                     .config("spark.sql.shuffle.partitions", "8")
                     .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            output = args.output or ROOT / "reports" / "eta" / "eta_accept_spark_calibrated_rolling.csv"
            with TemporaryDirectory(prefix="eta-spark-checkpoints-") as checkpoint_dir:
                try:
                    spark.sparkContext.setCheckpointDir(Path(checkpoint_dir).as_uri())
                    run_rolling_origin(args.source, output, args.seeds or [42], spark=spark)
                finally:
                    spark.stop()
        else:
            output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_rolling.csv"
            run_rolling_origin(args.source, output, args.seeds)
    else:
        output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_validation.csv"
        run(args.source, output, args.seeds)


if __name__ == "__main__":
    main()
